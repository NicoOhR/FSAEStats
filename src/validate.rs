use crate::pipeline::Source;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ValidationError {}

pub trait Validate {
    fn validate(&self, src: &Source) -> Vec<ValidationError>;
}
