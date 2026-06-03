use crate::pipeline::{
    FilterOp, GroupOp, JoinOp, NormMethod, NormOp, PipelineOp, RankOp, SelectOp, SortOp,
    WeaknessOp, YearDeltaOp,
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
    #[error("Unknown column for this view: {0}")]
    UnknownColumn(String),
}

fn validate_years(years: &[u16]) -> bool {
    years
        .iter()
        .any(|&x| x >= YEARS_RANGE.0 && x <= YEARS_RANGE.1)
}

fn check_columns<'a>(
    referenced: impl IntoIterator<Item = &'a str>,
    available: &[String],
) -> Vec<ValidationError> {
    referenced
        .into_iter()
        .filter(|c| !available.iter().any(|a| a.as_str() == *c))
        .map(|c| ValidationError::UnknownColumn(c.to_string()))
        .collect()
}

#[enum_dispatch]
pub trait Validate {
    /// Validates an op against `available`, the evolved schema up to this point
    /// in the pipeline (see [`View::columns`](crate::pipeline::View::columns)
    /// for the initial set). Ops that produce new columns push their output
    /// name into `available` so downstream ops can reference it.
    fn validate(&self, _available: &mut Vec<String>) -> Vec<ValidationError> {
        vec![]
    }
}

impl Validate for FilterOp {
    fn validate(&self, _available: &mut Vec<String>) -> Vec<ValidationError> {
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
    fn validate(&self, available: &mut Vec<String>) -> Vec<ValidationError> {
        let mut errs: Vec<ValidationError> = vec![];
        if self.columns.is_empty() {
            errs.push(ValidationError::RequiredArgIsEmpty);
        }
        errs.extend(check_columns(
            self.columns.iter().map(String::as_str),
            available,
        ));
        errs
    }
}

impl Validate for GroupOp {
    fn validate(&self, available: &mut Vec<String>) -> Vec<ValidationError> {
        let mut errs: Vec<ValidationError> = vec![];
        if self.by.is_empty() {
            errs.push(ValidationError::RequiredArgIsEmpty);
        }
        errs.extend(check_columns(self.by.iter().map(String::as_str), available));
        errs
    }
}

impl Validate for SortOp {
    fn validate(&self, available: &mut Vec<String>) -> Vec<ValidationError> {
        check_columns([self.by.as_str()], available)
    }
}

impl Validate for NormOp {
    fn validate(&self, available: &mut Vec<String>) -> Vec<ValidationError> {
        let errs = check_columns([self.column.as_str()], available);
        let output = match self.method {
            NormMethod::Zscore => format!("{}_zscore", self.column),
            NormMethod::PctGap => format!("{}_pct_gap", self.column),
        };
        available.push(output);
        errs
    }
}

impl Validate for RankOp {
    fn validate(&self, available: &mut Vec<String>) -> Vec<ValidationError> {
        let errs = check_columns([self.column.as_str()], available);
        available.push(format!("{}_rank", self.column));
        errs
    }
}

impl Validate for YearDeltaOp {
    fn validate(&self, available: &mut Vec<String>) -> Vec<ValidationError> {
        let errs = check_columns([self.column.as_str()], available);
        available.push(format!("{}_delta", self.column));
        errs
    }
}

// Ops that reference no columns and have no structural constraints: rely on the
// trait defaults but must still impl `Validate` for `enum_dispatch`.
impl Validate for JoinOp {}
impl Validate for WeaknessOp {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{FilterOp, GroupAgg, GroupOp, SelectOp};

    fn avail(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn filter_all_none_is_invalid() {
        let op = FilterOp {
            teams: None,
            years: None,
            events: None,
        };
        let errs = op.validate(&mut avail(&[]));
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
        assert!(op.validate(&mut avail(&[])).is_empty());
    }

    #[test]
    fn filter_years_in_range_is_valid() {
        let op = FilterOp {
            teams: None,
            years: Some(vec![2024]),
            events: None,
        };
        assert!(op.validate(&mut avail(&[])).is_empty());
    }

    #[test]
    fn filter_years_out_of_range_is_invalid() {
        let op = FilterOp {
            teams: None,
            years: Some(vec![2010]),
            events: None,
        };
        let errs = op.validate(&mut avail(&[]));
        assert!(errs
            .iter()
            .any(|e| matches!(e, ValidationError::YearsOutOfRange)));
    }

    #[test]
    fn select_empty_columns_is_invalid() {
        let op = SelectOp { columns: vec![] };
        let errs = op.validate(&mut avail(&[]));
        assert!(errs
            .iter()
            .any(|e| matches!(e, ValidationError::RequiredArgIsEmpty)));
    }

    #[test]
    fn select_with_columns_is_valid() {
        let op = SelectOp {
            columns: vec!["Team".into()],
        };
        assert!(op.validate(&mut avail(&["Team"])).is_empty());
    }

    #[test]
    fn group_empty_by_is_invalid() {
        let op = GroupOp {
            by: vec![],
            agg: GroupAgg::Sum,
        };
        let errs = op.validate(&mut avail(&[]));
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
        assert!(op.validate(&mut avail(&["year"])).is_empty());
    }
}
