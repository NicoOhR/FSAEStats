use crate::validate::Validate;
use enum_dispatch::enum_dispatch;
use polars::prelude::*;
use serde::Deserialize;
use strum_macros::{Display, EnumString};

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PipelineOp {
    // Transform Ops
    Filter(FilterOp),
    Select(SelectOp),
    Group(GroupOp),
    Join(JoinOp),
    // Analytic Ops
    Sort(SortOp),
    Normalize(NormOp),
    Ranked(RankOp),
    YearDelta(YearDeltaOp),
    // Inference Ops
    Weakness(WeaknessOp),
}
#[derive(Deserialize)]
pub struct FilterOp {
    pub teams: Option<Vec<String>>,
    pub years: Option<Vec<u16>>,
    pub events: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct SelectOp {
    pub columns: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupAgg {
    Sum,
    Mean,
    Max,
    Min,
    Count,
}

#[derive(Deserialize)]
pub struct GroupOp {
    pub by: Vec<String>,
    pub agg: GroupAgg,
}

#[derive(Deserialize)]
pub struct JoinOp {
    pub view: View,
    pub on: String,
}

#[derive(Deserialize)]
pub struct SortOp {
    pub by: String,
    pub descending: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormMethod {
    Zscore,
    PctGap,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormWithin {
    Year,
    Competition,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RankWithin {
    Year,
    Competition,
    Event,
}

#[derive(Deserialize)]
pub struct NormOp {
    pub column: String,
    pub method: NormMethod,
    pub within: NormWithin,
}

#[derive(Deserialize)]
pub struct RankOp {
    pub column: String,
    pub within: RankWithin,
}

#[derive(Deserialize)]
pub struct YearDeltaOp {
    pub column: String,
    pub team: String,
}

#[derive(Deserialize)]
pub struct WeaknessOp {
    pub team: String,
}

#[derive(Display, Deserialize, EnumString, Debug)]
#[strum(serialize_all = "snake_case")]
pub enum Comps {
    MichiganIc,
    MichiganEv,
}

#[derive(Deserialize)]
pub struct Pipeline(pub Vec<PipelineOp>);

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum View {
    CompetitionResults,
    OverallStandings,
    DynamicEvents,
    StaticEvents,
    EnduranceLaps,
    TeamProfile,
    FieldSummary,
    // Raw event views
    Accel,
    SkidPad,
    Autocross,
    Endurance,
    Efficiency,
    Design,
    Presentation,
    Cost,
}

#[derive(Deserialize)]
pub struct Source {
    pub view: View,
    pub years: Vec<i32>,
    pub competitions: Vec<Comps>,
}

impl Source {
    pub fn create_frame(&self) -> PolarsResult<LazyFrame> {
        let years = Series::new("".into(), self.years.as_slice());
        let comps = Series::new(
            "".into(),
            self.competitions
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<String>>(),
        );

        let scan = |path: &str| -> PolarsResult<LazyFrame> {
            Ok(
                LazyFrame::scan_parquet(path.into(), ScanArgsParquet::default())?
                    .filter(col("year").is_in(lit(years.clone()), false))
                    .filter(col("competition").is_in(lit(comps.clone()), false)),
            )
        };

        let lf = match self.view {
            View::CompetitionResults | View::OverallStandings | View::FieldSummary => {
                scan("data/parquet/**/overall.parquet")?
            }
            View::DynamicEvents => scan("data/parquet/**/overall.parquet")?.select([
                col("Place"),
                col("CarNum"),
                col("Team"),
                col("AccelerationScore"),
                col("SkidPadScore"),
                col("AutocrossScore"),
                col("EnduranceScore"),
                col("EfficiencyScore"),
            ]),
            View::StaticEvents => scan("data/parquet/**/overall.parquet")?.select([
                col("Place"),
                col("CarNum"),
                col("Team"),
                col("CostScore"),
                col("PresentationScore"),
                col("DesignScore"),
            ]),
            View::EnduranceLaps => scan("data/parquet/**/enduranceLap.parquet")?,
            View::TeamProfile => scan("data/parquet/**/team_information.parquet")?,
            View::Accel => scan("data/parquet/**/accel.parquet")?,
            View::SkidPad => scan("data/parquet/**/skid.parquet")?,
            View::Autocross => scan("data/parquet/**/autocross.parquet")?,
            View::Endurance => scan("data/parquet/**/endurance.parquet")?,
            View::Efficiency => scan("data/parquet/**/efficiency.parquet")?,
            View::Design => scan("data/parquet/**/design.parquet")?,
            View::Presentation => scan("data/parquet/**/presentation.parquet")?,
            View::Cost => scan("data/parquet/**/cost.parquet")?,
        };

        Ok(lf)
    }
}
