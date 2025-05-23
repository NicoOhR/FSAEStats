use crate::requests::*;
use duckdb::arrow::datatypes::Schema;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::pretty::*;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Method, Request, Response, StatusCode},
};
use hyper_util::client::legacy::connect::Connection;
use serde_json::to_string_pretty;
use serde_json::{Map, Number, Value};
use sqlx::{sqlite::SqliteRow, Column, Row, SqlitePool, TypeInfo, ValueRef};
use std::{error::Error, io::Cursor};
async fn create_pool() -> Result<duckdb::Connection, duckdb::Error> {
    let conn = duckdb::Connection::open("./data/race.duckdb")?;
    Ok(conn)
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
            let strings = pretty_format_batches(&response[..])?.to_string();
            Ok(Response::new(full(strings)))
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

//should impl for SqliteRow
pub fn dump_row(row: &SqliteRow) -> sqlx::Result<()> {
    //debugging command to print the members of a SqliteRow
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
pub fn row_to_json(row: &SqliteRow) -> Result<Value, Box<dyn Error + Send + Sync>> {
    //trying to map SqliteRow to a JsonValue
    let mut map = Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let v = match col.type_info().name().to_uppercase().as_str() {
            "INTEGER" | "INT" | "INT8" | "BIGINT" => {
                println!("{col:?}, Integer");
                let val: i64 = row.try_get(i)?;
                Value::Number(Number::from(val))
            }
            "REAL" | "FLOAT" | "DOUBLE" => {
                println!("{col:?} float");
                let val: f64 = row.try_get(i)?;
                match Number::from_f64(val) {
                    Some(num) => Value::Number(num),
                    None => Value::Null,
                }
            }
            "TEXT" | "CHAR" | "CLOB" | "VARCHAR" => {
                println!("{col:?} text");
                let val: String = row.try_get(i)?;
                Value::String(val)
            }
            "BLOB" => {
                println!("blob");
                let bytes: Vec<u8> = row.try_get(i)?;
                Value::String(base64::encode(bytes))
            }
            _ => Value::Null,
        };

        map.insert(col.name().to_string(), v);
    }
    Ok(Value::Object(map))
}
