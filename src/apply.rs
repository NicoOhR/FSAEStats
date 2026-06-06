use crate::pipeline::{
    FilterOp, GroupOp, JoinOp, NormOp, PipelineOp, RankOp, SelectOp, SortOp, Source, WeaknessOp,
    YearDeltaOp,
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
    fn apply(&self, st: &mut ApplyState);
}

impl Apply for FilterOp {
    fn apply(&self, st: &mut ApplyState) {
        todo!()
    }
}

impl Apply for SelectOp {
    fn apply(&self, st: &mut ApplyState) {
        todo!()
    }
}

impl Apply for GroupOp {
    fn apply(&self, st: &mut ApplyState) {
        todo!()
    }
}

impl Apply for JoinOp {
    fn apply(&self, st: &mut ApplyState) {
        todo!()
    }
}

impl Apply for SortOp {
    fn apply(&self, st: &mut ApplyState) {
        todo!()
    }
}

impl Apply for NormOp {
    fn apply(&self, st: &mut ApplyState) {
        todo!()
    }
}

impl Apply for RankOp {
    fn apply(&self, st: &mut ApplyState) {
        todo!()
    }
}

impl Apply for YearDeltaOp {
    fn apply(&self, st: &mut ApplyState) {
        todo!()
    }
}

impl Apply for WeaknessOp {
    fn apply(&self, st: &mut ApplyState) {
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
    //Filter
    #[test]
    fn filter_by_team_returns_single_row() {
        let src = overall_2024();
        let lf = src.create_frame().unwrap();
        let op = FilterOp {
            teams: Some(vec!["The Ohio State University".into()]),
            years: None,
            events: None,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 1);
        assert_eq!(
            out.column("Team").unwrap().str().unwrap().get(0),
            Some("The Ohio State University"),
        );
    }

    #[test]
    fn filter_by_year_reduces_to_single_year() {
        // Source spans 2024 + 2025; filter op narrows to 2025 only.
        let src = Source {
            view: View::OverallStandings,
            years: vec![2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let lf = src.create_frame().unwrap();
        let op = FilterOp {
            teams: None,
            years: Some(vec![2025u16]),
            events: None,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
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
        // `events` filters the "competition" column.
        // No michigan_ev data exists, so the result should be empty.
        let src = overall_2024();
        let lf = src.create_frame().unwrap();
        let op = FilterOp {
            teams: None,
            years: None,
            events: Some(vec!["michigan_ev".into()]),
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
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
        let lf = src.create_frame().unwrap();
        let op = FilterOp {
            teams: Some(vec!["The Ohio State University".into()]),
            years: Some(vec![2024u16]),
            events: None,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 1);
        assert_eq!(
            out.column("year").unwrap().i64().unwrap().get(0),
            Some(2024),
        );
    }
    //Select
    #[test]
    fn select_drops_unspecified_columns() {
        let src = overall_2024();
        let lf = src.create_frame().unwrap();
        let op = SelectOp {
            columns: vec!["Team".into(), "TotalScore".into()],
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert_eq!(out.width(), 2);
        assert_eq!(out.height(), 119); // 2024 has 119 teams
        assert!(out.column("Team").is_ok());
        assert!(out.column("TotalScore").is_ok());
        assert!(out.column("AccelerationScore").is_err());
    }

    #[test]
    fn sort_descending_puts_ohio_state_first() {
        let src = overall_2024();
        let lf = src.create_frame().unwrap();
        let op = SortOp {
            by: "TotalScore".into(),
            descending: Some(true),
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        let first = out.column("Team").unwrap().str().unwrap().get(0).unwrap();
        assert_eq!(first, "The Ohio State University"); // 907.6 — highest in 2024
    }

    #[test]
    fn sort_ascending_puts_ohio_state_last_among_finishers() {
        let src = overall_2024();
        let lf = src.create_frame().unwrap();
        let op = SortOp {
            by: "TotalScore".into(),
            descending: Some(false),
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 119);
        // Ohio State (907.6) must not be the first numeric-scored team when ascending
        let first = out
            .column("Team")
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap_or("");
        assert_ne!(first, "The Ohio State University");
    }

    //Group
    #[test]
    fn group_by_year_count_gives_one_row_per_year() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2023, 2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let lf = src.create_frame().unwrap();
        let op = GroupOp {
            by: vec!["year".into()],
            agg: GroupAgg::Count,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 3); // one row per distinct year
        assert!(out.column("year").is_ok());
        assert_eq!(out.width(), 2); // year + count
    }

    #[test]
    fn group_by_year_sum_gives_one_aggregated_row_per_year() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2023, 2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let lf = src.create_frame().unwrap();
        let op = GroupOp {
            by: vec!["year".into()],
            agg: GroupAgg::Sum,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert_eq!(out.height(), 3);
    }

    //Norm
    #[test]
    fn normalize_zscore_within_year_sums_to_zero() {
        let src = overall_2024();
        let lf = src.create_frame().unwrap();
        let op = NormOp {
            column: "TotalScore".into(),
            method: NormMethod::Zscore,
            within: NormWithin::Year,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert!(out.column("TotalScore_zscore").is_ok());
        // Non-null z-scores in a single partition always sum to 0
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
        // pct_gap = (max - value) / max; Ohio State (907.6) is the max, gap ≈ 0.
        let src = overall_2024();
        let lf = src.create_frame().unwrap();
        let op = NormOp {
            column: "TotalScore".into(),
            method: NormMethod::PctGap,
            within: NormWithin::Year,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
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

    //Rank
    #[test]
    fn rank_within_year_produces_dense_ranks_starting_at_one() {
        let src = overall_2024(); // 119 teams, TotalScore cast → Float64
        let lf = src.create_frame().unwrap();
        let op = RankOp {
            column: "TotalScore".into(),
            within: RankWithin::Year,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
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
        assert_eq!(ranks[0], 1); // dense rank starts at 1
                                 // no gaps in a dense rank sequence
        for (i, &r) in ranks.iter().enumerate() {
            assert!(r <= (i as u32 + 1), "gap in ranks at position {i}: got {r}");
        }
    }

    #[test]
    fn rank_partitions_independently_by_year() {
        // Two years loaded; each year should have its own 1-based ranking.
        let src = Source {
            view: View::OverallStandings,
            years: vec![2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let lf = src.create_frame().unwrap();
        let op = RankOp {
            column: "TotalScore".into(),
            within: RankWithin::Year,
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        let ranks: Vec<u32> = out
            .column("TotalScore_rank")
            .unwrap()
            .u32()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        // Each year independently starts at 1 → minimum rank across all rows is 1
        assert_eq!(*ranks.iter().min().unwrap(), 1);
        // Maximum rank equals the size of the larger group, not the combined total
        let max_rank = *ranks.iter().max().unwrap();
        assert!(max_rank < ranks.len() as u32); // not a global rank over all rows
    }

    // YearDeltaOp

    #[test]
    fn year_delta_for_purdue_matches_known_values() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2023, 2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let lf = src.create_frame().unwrap();
        let op = YearDeltaOp {
            column: "TotalScore".into(),
            team: "Purdue Univ - W Lafayette".into(),
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
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
        assert!(deltas[0].is_none()); // 2023: first year in the window
        assert!((deltas[1].unwrap() - (-55.6)).abs() < 0.05); // 2024: 684.1 − 739.7
        assert!((deltas[2].unwrap() - 69.5).abs() < 0.05); // 2025: 753.6 − 684.1
    }

    #[test]
    fn year_delta_excludes_other_teams() {
        let src = Source {
            view: View::OverallStandings,
            years: vec![2024, 2025],
            competitions: vec![Comps::Michigan],
        };
        let lf = src.create_frame().unwrap();
        let op = YearDeltaOp {
            column: "TotalScore".into(),
            team: "The Ohio State University".into(),
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert!(str_vals(&out, "Team")
            .iter()
            .all(|t| t == "The Ohio State University"));
    }

    // JoinOp

    #[test]
    fn join_team_profile_appends_country_column() {
        let src = overall_2024();
        let lf = src.create_frame().unwrap();
        let op = JoinOp {
            view: View::TeamProfile,
            on: "Team".into(),
        };
        let mut st = ApplyState {
            lf,
            src: &src,
            errs: vec![],
        };
        op.apply(&mut st);
        let out = st.lf.collect().unwrap();
        assert!(out.column("TotalScore").is_ok()); // from left frame
        assert!(out.column("Country").is_ok()); // from TeamProfile
    }

    // WeaknessOp — design TBD

    #[test]
    #[ignore = "WeaknessOp design not finalized"]
    fn weakness_produces_event_weakness_scores() {
        todo!()
    }
}
