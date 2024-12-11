use crate::{request_handler, request_parser};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Method, Request, Response, StatusCode},
};
use request_handler::*;

pub async fn user_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    let pool = request_handler::create_pool().await.unwrap();

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(full("GET the /team/year/event"))),
        (&Method::GET, "/request") => {
            let mut base_request = request_parser::parse_request(req).await?;
            let request_struct = request_parser::EventRequest::from_hash(&mut base_request)?;
            let sqlite_row = request_handler(request_struct.clone(), pool).await?;

            println!("{}", serde_json::to_string_pretty(&sqlite_row).unwrap());

            Ok(Response::new(full(
                serde_json::to_string(&sqlite_row).unwrap(),
            )))
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
