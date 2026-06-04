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
