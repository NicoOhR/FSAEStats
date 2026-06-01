use crate::pipeline::{
    FilterOp, GroupOp, JoinOp, NormOp, PipeLineOp, RankOp, SelectOp, SortOp, Source, WeaknessOp,
    YearDeltaOp,
};
use enum_dispatch::enum_dispatch;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ValidationError {}

#[enum_dispatch]
pub trait Validate {
    fn validate(&self, src: &Source) -> Vec<ValidationError>;
}

impl Validate for FilterOp {
    fn validate(&self, src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}

impl Validate for SelectOp {
    fn validate(&self, src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}

impl Validate for GroupOp {
    fn validate(&self, src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}

impl Validate for JoinOp {
    fn validate(&self, src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}

impl Validate for SortOp {
    fn validate(&self, _src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}

impl Validate for NormOp {
    fn validate(&self, src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}

impl Validate for RankOp {
    fn validate(&self, src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}

impl Validate for YearDeltaOp {
    fn validate(&self, src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}

impl Validate for WeaknessOp {
    fn validate(&self, src: &Source) -> Vec<ValidationError> {
        todo!()
    }
}
