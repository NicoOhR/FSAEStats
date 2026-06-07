use crate::pipeline::{
    FilterOp, GroupAgg, GroupOp, JoinOp, NormMethod, NormOp, NormWithin, PipelineOp, RankOp,
    RankWithin, SelectOp, SortOp, Source, WeaknessOp, YearDeltaOp,
};
use enum_dispatch::enum_dispatch;
use polars::prelude::*;

pub(crate) struct ApplyState<'a> {
    pub lf: LazyFrame,
    pub src: &'a Source,
    pub errs: Vec<PolarsError>,
}

#[enum_dispatch]
pub(crate) trait Apply {
    fn apply<'a>(&self, st: ApplyState<'a>) -> ApplyState<'a>;
}

impl Apply for FilterOp {
    fn apply<'a>(&self, mut st: ApplyState<'a>) -> ApplyState<'a> {
        let mut lf = st.lf;
        if let Some(teams) = &self.teams {
            let s = Series::new("".into(), teams.as_slice());
            lf = lf.filter(col("Team").is_in(lit(s), false));
        }
        if let Some(years) = &self.years {
            let s = Series::new(
                "".into(),
                years.iter().map(|&y| y as i64).collect::<Vec<i64>>(),
            );
            lf = lf.filter(col("year").is_in(lit(s), false));
        }
        if let Some(events) = &self.events {
            let s = Series::new("".into(), events.as_slice());
            lf = lf.filter(col("competition").is_in(lit(s), false));
        }
        st.lf = lf;
        st
    }
}

impl Apply for SelectOp {
    fn apply<'a>(&self, mut st: ApplyState<'a>) -> ApplyState<'a> {
        let exprs: Vec<Expr> = self.columns.iter().map(|c| col(c.as_str())).collect();
        st.lf = st.lf.select(exprs);
        st
    }
}

impl Apply for SortOp {
    fn apply<'a>(&self, mut st: ApplyState<'a>) -> ApplyState<'a> {
        let descending = self.descending.unwrap_or(false);
        let lf = st.lf;
        st.lf = lf.sort_by_exprs(
            &[col(&self.by).cast(DataType::Float64)],
            SortMultipleOptions::default()
                .with_order_descending(descending)
                .with_nulls_last(true),
        );
        st
    }
}

impl Apply for GroupOp {
    fn apply<'a>(&self, mut st: ApplyState<'a>) -> ApplyState<'a> {
        let by_exprs: Vec<Expr> = self.by.iter().map(|c| col(c.as_str())).collect();

        let agg_exprs: Vec<Expr> = if matches!(self.agg, GroupAgg::Count) {
            vec![len().alias("count")]
        } else {
            let by_set: std::collections::HashSet<&str> =
                self.by.iter().map(String::as_str).collect();
            let schema = st.lf.collect_schema().expect("schema collection failed");
            schema
                .iter_names()
                .filter(|name| !by_set.contains(name.as_str()))
                .map(|name| {
                    let c = col(name.as_str()).cast(DataType::Float64);
                    match self.agg {
                        GroupAgg::Sum => c.sum(),
                        GroupAgg::Mean => c.mean(),
                        GroupAgg::Max => c.max(),
                        GroupAgg::Min => c.min(),
                        GroupAgg::Count => unreachable!(),
                    }
                })
                .collect()
        };

        let lf = st.lf;
        st.lf = lf.group_by(by_exprs).agg(agg_exprs);
        st
    }
}

impl Apply for NormOp {
    fn apply<'a>(&self, mut st: ApplyState<'a>) -> ApplyState<'a> {
        let partition = match self.within {
            NormWithin::Year => col("year"),
            NormWithin::Competition => col("competition"),
        };
        let cast = col(&self.column).cast(DataType::Float64);
        let derived_expr = match self.method {
            NormMethod::Zscore => {
                let mean = cast.clone().mean().over([partition.clone()]);
                let std = cast.clone().std(1).over([partition.clone()]);
                ((cast - mean) / std).alias(format!("{}_zscore", self.column))
            }
            NormMethod::PctGap => {
                let max = cast.clone().max().over([partition.clone()]);
                ((max.clone() - cast) / max).alias(format!("{}_pct_gap", self.column))
            }
        };
        let lf = st.lf;
        st.lf = lf.with_column(derived_expr);
        st
    }
}

impl Apply for RankOp {
    fn apply<'a>(&self, mut st: ApplyState<'a>) -> ApplyState<'a> {
        let partition = match self.within {
            RankWithin::Year => col("year"),
            RankWithin::Competition => col("competition"),
            RankWithin::Event => col("competition"),
        };
        let rank_expr = col(&self.column)
            .cast(DataType::Float64)
            .rank(
                RankOptions {
                    method: RankMethod::Dense,
                    descending: false,
                },
                None,
            )
            .over([partition])
            .alias(format!("{}_rank", self.column));
        let lf = st.lf;
        st.lf = lf.with_column(rank_expr);
        st
    }
}

impl Apply for YearDeltaOp {
    fn apply<'a>(&self, mut st: ApplyState<'a>) -> ApplyState<'a> {
        let cast = col(&self.column).cast(DataType::Float64);
        let delta = (cast.clone() - cast.shift(lit(1i64))).alias(format!("{}_delta", self.column));
        let team = Series::new("".into(), &[self.team.as_str()]);
        let lf = st.lf;
        st.lf = lf
            .filter(col("Team").is_in(lit(team), false))
            .sort(["year"], SortMultipleOptions::default())
            .with_column(delta);
        st
    }
}

impl Apply for JoinOp {
    fn apply<'a>(&self, mut st: ApplyState<'a>) -> ApplyState<'a> {
        // Build the right-hand frame using the same year/competition filters as the
        // source, but scanning the join view's parquet instead.
        let right_src = Source {
            view: self.view.clone(),
            years: st.src.years.clone(),
            competitions: st.src.competitions.clone(),
        };
        let right_lf = match right_src.create_frame() {
            Ok(lf) => lf,
            Err(e) => {
                st.errs.push(e);
                return st;
            }
        };
        let on = col(&self.on);
        let lf = st.lf;
        st.lf = lf.join(right_lf, [on.clone()], [on], JoinArgs::new(JoinType::Left));
        st
    }
}

impl Apply for WeaknessOp {
    fn apply<'a>(&self, _st: ApplyState<'a>) -> ApplyState<'a> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::*;

    fn overall_2024() -> Source {
        Source {
            view: View::OverallStandings,
            years: vec![2024],
            competitions: vec![Comps::Michigan],
        }
    }

    fn str_vals(df: &DataFrame, col_name: &str) -> Vec<String> {
        df.column(col_name)
            .unwrap()
            .str()
            .unwrap()
            .into_iter()
            .flatten()
            .map(str::to_string)
            .collect()
    }

    fn state(src: &Source) -> ApplyState {
        ApplyState {
            lf: src.create_frame().unwrap(),
            src,
            errs: vec![],
        }
    }

    #[test]
    fn filter_by_team_returns_single_row() {
        let src = overall_2024();
        let op = FilterOp {
            teams: Some(vec!["The Ohio State University".into()]),
            years: None,
            events: None,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 1);
        assert_eq!(
            out.column("Team").unwrap().str().unwrap().get(0),
            Some("The Ohio State University"),
        );
    }

    #[test]
    fn filter_by_year_reduces_to_single_year() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let op = FilterOp {
            teams: None,
            years: Some(vec![2025u16]),
            events: None,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert!(out
            .column("year")
            .unwrap()
            .i64()
            .unwrap()
            .into_iter()
            .flatten()
            .all(|y| y == 2025));
    }

    #[test]
    fn filter_by_event_excludes_nonexistent_competition() {
        let src = overall_2024();
        let op = FilterOp {
            teams: None,
            years: None,
            events: Some(vec!["michigan_ev".into()]),
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 0);
    }

    #[test]
    fn filter_combines_team_and_year() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let op = FilterOp {
            teams: Some(vec!["The Ohio State University".into()]),
            years: Some(vec![2024u16]),
            events: None,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 1);
        assert_eq!(
            out.column("year").unwrap().i64().unwrap().get(0),
            Some(2024)
        );
    }

    #[test]
    fn select_drops_unspecified_columns() {
        let src = overall_2024();
        let op = SelectOp {
            columns: vec!["Team".into(), "TotalScore".into()],
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert_eq!(out.width(), 2);
        assert_eq!(out.height(), 119);
        assert!(out.column("Team").is_ok());
        assert!(out.column("TotalScore").is_ok());
        assert!(out.column("AccelerationScore").is_err());
    }

    #[test]
    fn sort_descending_puts_ohio_state_first() {
        let src = overall_2024();
        let op = SortOp {
            by: "TotalScore".into(),
            descending: Some(true),
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        let first = out.column("Team").unwrap().str().unwrap().get(0).unwrap();
        assert_eq!(first, "The Ohio State University");
    }

    #[test]
    fn sort_ascending_puts_ohio_state_last_among_finishers() {
        let src = overall_2024();
        let op = SortOp {
            by: "TotalScore".into(),
            descending: Some(false),
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 119);
        let first = out
            .column("Team")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap_or("");
        assert_ne!(first, "The Ohio State University");
    }

    #[test]
    fn group_by_year_count_gives_one_row_per_year() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2023, 2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let op = GroupOp {
            by: vec!["year".into()],
            agg: GroupAgg::Count,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 3);
        assert!(out.column("year").is_ok());
        assert_eq!(out.width(), 2);
    }

    #[test]
    fn group_by_year_sum_gives_one_aggregated_row_per_year() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2023, 2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let op = GroupOp {
            by: vec!["year".into()],
            agg: GroupAgg::Sum,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 3);
    }

    #[test]
    fn normalize_zscore_within_year_sums_to_zero() {
        let src = overall_2024();
        let op = NormOp {
            column: "TotalScore".into(),
            method: NormMethod::Zscore,
            within: NormWithin::Year,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert!(out.column("TotalScore_zscore").is_ok());
        let sum: f64 = out
            .column("TotalScore_zscore")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .flatten()
            .sum();
        assert!(sum.abs() < 1e-6);
    }

    #[test]
    fn normalize_pct_gap_winner_has_zero_gap() {
        let src = overall_2024();
        let op = NormOp {
            column: "TotalScore".into(),
            method: NormMethod::PctGap,
            within: NormWithin::Year,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert!(out.column("TotalScore_pct_gap").is_ok());
        let gaps: Vec<f64> = out
            .column("TotalScore_pct_gap")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        let min = gaps.iter().cloned().fold(f64::INFINITY, f64::min);
        assert!(min.abs() < 1e-9);
        assert!(gaps.iter().all(|&g| g >= 0.0));
    }

    #[test]
    fn rank_within_year_produces_dense_ranks_starting_at_one() {
        let src = overall_2024();
        let op = RankOp {
            column: "TotalScore".into(),
            within: RankWithin::Year,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert!(out.column("TotalScore_rank").is_ok());
        let mut ranks: Vec<u32> = out
            .column("TotalScore_rank")
            .unwrap()
            .u32()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        ranks.sort_unstable();
        assert_eq!(ranks[0], 1);
        for (i, &r) in ranks.iter().enumerate() {
            assert!(r <= (i as u32 + 1), "gap in ranks at position {i}: got {r}");
        }
    }

    #[test]
    fn rank_partitions_independently_by_year() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let op = RankOp {
            column: "TotalScore".into(),
            within: RankWithin::Year,
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        let ranks: Vec<u32> = out
            .column("TotalScore_rank")
            .unwrap()
            .u32()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(*ranks.iter().min().unwrap(), 1);
        let max_rank = *ranks.iter().max().unwrap();
        assert!(max_rank < ranks.len() as u32);
    }

    #[test]
    fn year_delta_for_purdue_matches_known_values() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2023, 2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let op = YearDeltaOp {
            column: "TotalScore".into(),
            team: "Purdue Univ - W Lafayette".into(),
        };
        let st = op.apply(state(&src));
        let out = st
            .lf
            .sort(["year"], SortMultipleOptions::default())
            .collect()
            .unwrap();
        assert_eq!(out.height(), 3);
        assert!(out.column("TotalScore_delta").is_ok());
        let deltas: Vec<Option<f64>> = out
            .column("TotalScore_delta")
            .unwrap()
            .f64()
            .unwrap()
            .into_iter()
            .collect();
        assert!(deltas[0].is_none());
        assert!((deltas[1].unwrap() - (-55.6)).abs() < 0.05); // 684.1 − 739.7
        assert!((deltas[2].unwrap() - 69.5).abs() < 0.05); // 753.6 − 684.1
    }

    #[test]
    fn year_delta_excludes_other_teams() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let op = YearDeltaOp {
            column: "TotalScore".into(),
            team: "The Ohio State University".into(),
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert!(str_vals(&out, "Team")
            .iter()
            .all(|t| t == "The Ohio State University"));
    }

    #[test]
    fn join_team_profile_appends_country_column() {
        let src = overall_2024();
        let op = JoinOp {
            view: View::TeamProfile,
            on: "Team".into(),
        };
        let st = op.apply(state(&src));
        let out = st.lf.collect().unwrap();
        assert!(out.column("TotalScore").is_ok());
        assert!(out.column("Country").is_ok());
    }

    #[test]
    #[ignore = "WeaknessOp design not finalized"]
    fn weakness_produces_event_weakness_scores() {
        todo!()
    }
}
