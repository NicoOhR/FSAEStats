use crate::validate::Validate;
use enum_dispatch::enum_dispatch;

use serde::Deserialize;

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

#[derive(Deserialize)]
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
}

#[derive(Deserialize)]
pub struct Source {
    pub view: View,
    pub years: Vec<u16>,
    pub competitions: Vec<Comps>,
}
