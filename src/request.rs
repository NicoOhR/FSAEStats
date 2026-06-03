use crate::{
    pipeline::{Pipeline, Source},
    validate::{Validate, ValidationError},
};
use serde::Deserialize;

//requests takes a JSON request, deseralizes into the PipeLineRequest
//and validates
//
//pipeline.rs does not need to check for validity at this point

#[derive(Deserialize)]
pub struct PipelineRequest {
    src: Source,
    ops: Pipeline,
}

impl PipelineRequest {
    pub fn validate(&self) -> Vec<ValidationError> {
        let mut available: Vec<String> = self.src.view.columns()
            .iter()
            .map(|s| s.to_string())
            .collect();
        self.ops
            .0
            .iter()
            .flat_map(|op| op.validate(&mut available))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(view: &str) -> String {
        format!(r#"{{"view": "{view}", "years": [2024], "competitions": ["MichiganIc"]}}"#)
    }

    fn request(view: &str, ops: &str) -> String {
        format!(r#"{{"src": {}, "ops": {ops}}}"#, src(view))
    }

    #[test]
    fn valid_request_deserializes() {
        let json = request("competition_results", "[]");
        assert!(serde_json::from_str::<PipelineRequest>(&json).is_ok());
    }

    #[test]
    fn invalid_view_is_rejected() {
        let json = request("not_a_view", "[]");
        assert!(serde_json::from_str::<PipelineRequest>(&json).is_err());
    }

    #[test]
    fn invalid_op_tag_is_rejected() {
        let json = request("overall_standings", r#"[{"op": "explode"}]"#);
        assert!(serde_json::from_str::<PipelineRequest>(&json).is_err());
    }

    #[test]
    fn select_missing_columns_field_is_rejected() {
        let json = request("overall_standings", r#"[{"op": "select"}]"#);
        assert!(serde_json::from_str::<PipelineRequest>(&json).is_err());
    }

    #[test]
    fn valid_filter_op_deserializes() {
        let json = request("dynamic_events", r#"[{"op": "filter", "teams": ["MIT"]}]"#);
        assert!(serde_json::from_str::<PipelineRequest>(&json).is_ok());
    }

    #[test]
    fn valid_sort_op_deserializes() {
        let json = request(
            "overall_standings",
            r#"[{"op": "sort", "by": "TotalScore", "descending": true}]"#,
        );
        assert!(serde_json::from_str::<PipelineRequest>(&json).is_ok());
    }

    #[test]
    fn invalid_norm_method_is_rejected() {
        let json = request(
            "competition_results",
            r#"[{"op": "normalize", "column": "TotalScore", "method": "bad", "within": "year"}]"#,
        );
        assert!(serde_json::from_str::<PipelineRequest>(&json).is_err());
    }

    fn validate(view: &str, ops: &str) -> Vec<ValidationError> {
        let json = request(view, ops);
        serde_json::from_str::<PipelineRequest>(&json)
            .expect("request should deserialize")
            .validate()
    }

    fn has_unknown_column(errs: &[ValidationError], name: &str) -> bool {
        errs.iter()
            .any(|e| matches!(e, ValidationError::UnknownColumn(c) if c == name))
    }

    #[test]
    fn select_known_column_is_valid() {
        let errs = validate("overall_standings", r#"[{"op": "select", "columns": ["TotalScore"]}]"#);
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
    }

    #[test]
    fn select_unknown_column_is_rejected() {
        let errs = validate("overall_standings", r#"[{"op": "select", "columns": ["Nope"]}]"#);
        assert!(has_unknown_column(&errs, "Nope"));
    }

    #[test]
    fn column_validation_is_view_aware() {
        // TotalScore exists in the overall views but not in the projected dynamic_events view.
        let errs = validate("dynamic_events", r#"[{"op": "select", "columns": ["TotalScore"]}]"#);
        assert!(has_unknown_column(&errs, "TotalScore"));
    }

    #[test]
    fn sort_unknown_column_is_rejected() {
        let errs = validate("overall_standings", r#"[{"op": "sort", "by": "Bogus"}]"#);
        assert!(has_unknown_column(&errs, "Bogus"));
    }

    #[test]
    fn partition_columns_are_selectable() {
        let errs = validate("overall_standings", r#"[{"op": "select", "columns": ["year", "competition"]}]"#);
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
    }

    #[test]
    fn normalized_column_is_selectable_downstream() {
        let errs = validate(
            "overall_standings",
            r#"[
                {"op": "normalize", "column": "TotalScore", "method": "zscore", "within": "year"},
                {"op": "select", "columns": ["TotalScore_zscore"]}
            ]"#,
        );
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
    }

    #[test]
    fn ranked_column_is_selectable_downstream() {
        let errs = validate(
            "overall_standings",
            r#"[
                {"op": "ranked", "column": "TotalScore", "within": "year"},
                {"op": "select", "columns": ["TotalScore_rank"]}
            ]"#,
        );
        assert!(errs.is_empty(), "unexpected errors: {errs:?}");
    }

    #[test]
    fn derived_column_not_available_before_op() {
        let errs = validate(
            "overall_standings",
            r#"[
                {"op": "select", "columns": ["TotalScore_zscore"]},
                {"op": "normalize", "column": "TotalScore", "method": "zscore", "within": "year"}
            ]"#,
        );
        assert!(has_unknown_column(&errs, "TotalScore_zscore"));
    }
}
