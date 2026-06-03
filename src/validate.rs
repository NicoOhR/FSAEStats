use crate::pipeline::{
    FilterOp, GroupOp, JoinOp, NormOp, PipelineOp, RankOp, SelectOp, SortOp, Source, WeaknessOp,
    YearDeltaOp,
};
use enum_dispatch::enum_dispatch;
use thiserror::Error;

pub static YEARS_RANGE: (u16, u16) = (2024, 2025);

#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("At least one field must be provided")]
    AllEmptyArgs,
    #[error("Years requested are out of range")]
    YearsOutOfRange,
    #[error("Required argument must be provided for this operation")]
    RequiredArgIsEmpty,
}

fn validate_years(years: &[u16]) -> bool {
    years
        .iter()
        .any(|&x| x >= YEARS_RANGE.0 && x <= YEARS_RANGE.1)
}

#[enum_dispatch]
pub trait Validate {
    fn validate(&self) -> Vec<ValidationError> {
        vec![]
    }
}

impl Validate for FilterOp {
    fn validate(&self) -> Vec<ValidationError> {
        let mut errs: Vec<ValidationError> = vec![];
        if self.teams.is_none() && self.years.is_none() && self.events.is_none() {
            errs.push(ValidationError::AllEmptyArgs);
        }
        if let Some(years) = &self.years {
            if !validate_years(years) {
                errs.push(ValidationError::YearsOutOfRange);
            }
        }
        errs
    }
}

impl Validate for SelectOp {
    fn validate(&self) -> Vec<ValidationError> {
        let mut errs: Vec<ValidationError> = vec![];
        if self.columns.is_empty() {
            errs.push(ValidationError::RequiredArgIsEmpty);
        }
        errs
    }
}

impl Validate for GroupOp {
    fn validate(&self) -> Vec<ValidationError> {
        let mut errs: Vec<ValidationError> = vec![];
        if self.by.is_empty() {
            errs.push(ValidationError::RequiredArgIsEmpty);
        }
        errs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{FilterOp, GroupAgg, GroupOp, SelectOp};

    #[test]
    fn filter_all_none_is_invalid() {
        let op = FilterOp {
            teams: None,
            years: None,
            events: None,
        };
        let errs = op.validate();
        assert!(errs
            .iter()
            .any(|e| matches!(e, ValidationError::AllEmptyArgs)));
    }

    #[test]
    fn filter_one_field_is_valid() {
        let op = FilterOp {
            teams: Some(vec!["MIT".into()]),
            years: None,
            events: None,
        };
        assert!(op.validate().is_empty());
    }

    #[test]
    fn filter_years_in_range_is_valid() {
        let op = FilterOp {
            teams: None,
            years: Some(vec![2024]),
            events: None,
        };
        assert!(op.validate().is_empty());
    }

    #[test]
    fn filter_years_out_of_range_is_invalid() {
        let op = FilterOp {
            teams: None,
            years: Some(vec![2010]),
            events: None,
        };
        let errs = op.validate();
        assert!(errs
            .iter()
            .any(|e| matches!(e, ValidationError::YearsOutOfRange)));
    }

    #[test]
    fn select_empty_columns_is_invalid() {
        let op = SelectOp { columns: vec![] };
        let errs = op.validate();
        assert!(errs
            .iter()
            .any(|e| matches!(e, ValidationError::RequiredArgIsEmpty)));
    }

    #[test]
    fn select_with_columns_is_valid() {
        let op = SelectOp {
            columns: vec!["Team".into()],
        };
        assert!(op.validate().is_empty());
    }

    #[test]
    fn group_empty_by_is_invalid() {
        let op = GroupOp {
            by: vec![],
            agg: GroupAgg::Sum,
        };
        let errs = op.validate();
        assert!(errs
            .iter()
            .any(|e| matches!(e, ValidationError::RequiredArgIsEmpty)));
    }

    #[test]
    fn group_with_by_is_valid() {
        let op = GroupOp {
            by: vec!["year".into()],
            agg: GroupAgg::Mean,
        };
        assert!(op.validate().is_empty());
    }
}
