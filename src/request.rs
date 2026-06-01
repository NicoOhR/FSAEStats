use crate::{
    pipeline::{Pipeline, Source},
    validate::ValidationError,
};
use serde::Deserialize;

//requests takes a JSON request, deseralizes into the PipeLineRequest
//and validates
//
//pipeline.rs does not need to check for validity at this point

#[derive(Deserialize)]
struct PipelineRequest {
    src: Source,
    ops: Pipeline,
}

impl PipelineRequest {
    pub fn validate(self) -> Vec<ValidationError> {
        todo!()
    }
}
