use duckdb::{Connection, Result as DkResult};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Method, Request, Response, StatusCode},
};
use serde::Serialize;

use crate::requests::*;

pub async fn user_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    let conn = Connection::open("")?;
    conn.execute("INSTALL sqlite_scanner; LOAD sqlite_scanner;", [])?;
    conn.execute("ATTACH 'app.db' (TYPE sqlite);", [])?;

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(full("GET the /team/year/event"))),
        (&Method::GET, "/event") => {
            let mut request = parse_request(req).await?;
            let response = UserRequest::from_hash(&mut request)?.handle(conn).await?;

            println!("request {response}");

            Ok(Response::new(full(response.to_string())))
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
