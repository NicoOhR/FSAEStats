use crate::request_parser;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Request, Response, Method, StatusCode}
};


pub async fn user_request(
    req: Request<hyper::body::Incoming>
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error>{
    match (req.method(), req.uri().path()) {
        (&Method::GET , "/") => Ok(Response::new(full(
            "GET the /team/year/event",
        ))),
        (&Method::GET, "/request") => {
            let base_request = request_parser::parse_request(req).await;
            Ok(Response::new(full(
                base_request.unwrap().to_string(),
            )))
        },
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

