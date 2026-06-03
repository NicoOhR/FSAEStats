use crate::{
    pipeline::{Pipeline, PipelineOp, Source},
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
    pub fn validate(self) -> Vec<ValidationError> {
        self.ops
            .0
            .iter()
            .flat_map(|o| match o {
                PipelineOp::Filter(op) => op.validate(),
                PipelineOp::Select(op) => op.validate(),
                PipelineOp::Group(op) => op.validate(),
                _ => vec![],
            })
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
}
