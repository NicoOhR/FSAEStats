use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Method, Request, Response, StatusCode},
};
use serde::Serialize;
use sqlx::{sqlite::SqlitePool, FromRow};

use crate::requests::*;

pub async fn create_pool() -> Result<SqlitePool, sqlx::Error> {
    let database_url = "sqlite://race.db";
    SqlitePool::connect(database_url).await
}

pub async fn user_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    let pool = create_pool().await.unwrap();

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(full("GET the /team/year/event"))),
        (&Method::GET, "/event") => {
            let mut request = parse_request(req).await?;
            let response = EventRequest::from_hash(&mut request)?.handle(pool).await?;

            println!(
                "request {}",
                serde_json::to_string_pretty(&response).unwrap()
            );

            Ok(Response::new(full(
                serde_json::to_string(&response).unwrap(),
            )))
        }
        (&Method::GET, "/graph") => {
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
