use crate::requests::*;
use base64::{engine::general_purpose, Engine as _};
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Method, Request, Response, StatusCode},
};
use serde::Serialize;
use serde_json::{Map, Number, Value};
use sqlx::{
    query,
    sqlite::{SqliteRow, SqliteValueRef},
    Column, FromRow, Row, SqlitePool, TypeInfo, ValueRef,
};

async fn create_pool() -> Result<SqlitePool, sqlx::Error> {
    let database_url = "sqlite://./data/race.db";
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
            let response = UserRequest::from_hash(&mut request)?.handle(pool).await?;

            for row in &response {
                dump_row(&row);
            }

            let json_row: Result<Vec<_>, _> = response.iter().map(row_to_json).collect();
            let bytes = serde_json::to_vec(&json_row?)?;
            Ok(Response::new(full(bytes)))
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

pub fn row_to_json(row: &SqliteRow) -> sqlx::Result<Value> {
    let mut obj = Map::with_capacity(row.len()); // row.len() == #cols :contentReference[oaicite:2]{index=2}
    for (i, col) in row.columns().iter().enumerate() {
        let v: Value = row.try_get(i)?;
        obj.insert(col.name().into(), v);
    }
    Ok(Value::Object(obj))
}

pub fn dump_row(row: &SqliteRow) -> sqlx::Result<()> {
    for (i, col) in row.columns().iter().enumerate() {
        let s: String = match col.type_info().name().to_uppercase().as_str() {
            "INTEGER" | "INT" | "INT8" | "BIGINT" => row.try_get::<i64, _>(i)?.to_string(),
            "REAL" | "FLOAT" | "DOUBLE" => row.try_get::<f64, _>(i)?.to_string(),
            "TEXT" | "CHAR" | "CLOB" | "VARCHAR" => row.try_get::<String, _>(i)?,
            "BLOB" => base64::encode(row.try_get::<Vec<u8>, _>(i)?),
            _ => "null".into(),
        };
        println!("{} = {}", col.name(), s);
    }
    Ok(())
}
