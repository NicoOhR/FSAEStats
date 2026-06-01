use crate::validate::Validate;
use enum_dispatch::enum_dispatch;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[enum_dispatch(Validate)]
pub enum PipeLineOp {
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
pub struct GroupOp {
    pub by: Vec<String>,
    pub agg: String,
}

#[derive(Deserialize)]
pub struct JoinOp {
    pub view: String,
    pub on: String,
}

#[derive(Deserialize)]
pub struct SortOp {
    pub by: String,
    pub descending: Option<bool>,
}

#[derive(Deserialize)]
pub struct NormOp {
    pub column: String,
    pub method: String,
    pub within: String,
}

#[derive(Deserialize)]
pub struct RankOp {
    pub column: String,
    pub within: String,
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
pub struct Pipeline(pub Vec<PipeLineOp>);

#[derive(Deserialize)]
pub struct Source {
    pub view: String,
    pub years: Vec<u16>,
    pub competitions: Vec<Comps>,
}
