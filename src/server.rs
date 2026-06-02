use crate::request::*;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Method, Request, Response, StatusCode},
};
use serde_json::from_slice;

pub async fn user_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/pipeline") => {
            let body = req.collect().await?.to_bytes();
            let request = from_slice::<PipelineRequest>(&body)?;
            let op_errs = request.validate();
            if !op_errs.is_empty() {
                let body = serde_json::json!(
                {"errors": op_errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()});
                let resp = Response::new(full(body.to_string()));
                return Ok(resp);
            }
            todo!()
        }
        _ => {
            let mut not_found = Response::new(empty());
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}
fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}
fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
