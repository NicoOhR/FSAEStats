use hyper::Error as HyperError;
use hyper::Request;
use polars::prelude::*;
use serde::de::Error;
use serde::Serialize;
use serde_json::{Map, Value};
use sqlx;
use sqlx::SqlitePool;
use sqlx::{Column, QueryBuilder, Row, Sqlite};
use std::collections::HashMap;
use std::hash::Hash;
use struct_iterable::Iterable;
use strum_macros::Display;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Request must contain query")]
    EmptyParse,
    #[error("Request does not contian the matching keys")]
    IncorrectParse,
    #[error("Event could not be found")]
    EventNotFound,
    #[error("Required Field is missing: {0:?}")]
    Missing(&'static str),
    #[error("Hyper error: {0:?}")]
    Hyper(#[from] HyperError),
}

#[derive(Debug, Clone)]
pub struct UserRequest {
    pub team: String,
    pub year: String,
    pub event: String,
}

trait FromString {
    fn from_string(string: String) -> Result<Box<Self>, ParseError>;
}

pub trait RequestTrait {
    fn from_hash(args_map: &mut HashMap<String, String>) -> Result<Box<Self>, ParseError>;

    fn to_string(self) -> String;

    async fn handle(
        self,
        pool: SqlitePool,
    ) -> sqlx::Result<Vec<sqlx::sqlite::SqliteRow>, Box<dyn std::error::Error + Send + Sync>>;
}

impl UserRequest {
    fn new(team: String, year: String, event: String) -> Self {
        Self { team, year, event }
    }
}

impl RequestTrait for UserRequest {
    fn from_hash(args: &mut HashMap<String, String>) -> Result<Box<Self>, ParseError> {
        let team = args.remove("team").ok_or(ParseError::Missing("team"))?;
        let year = args.remove("year").ok_or(ParseError::Missing("year"))?;
        let event = args.remove("event").ok_or(ParseError::Missing("event"))?;

        Ok(Box::new(Self { team, year, event }))
    }
    fn to_string(self) -> String {
        let req_as_string: String = format!(
            "team : {}, year : {}, event : {}",
            self.team, self.year, self.event
        );
        req_as_string
    }
    async fn handle(
        self,
        pool: SqlitePool,
    ) -> sqlx::Result<Vec<sqlx::sqlite::SqliteRow>, Box<dyn std::error::Error + Send + Sync>> {
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(format!(
            "SELECT * FROM {} WHERE Team = ",
            quote_ident(&self.event)
        ));
        qb.push_bind(&self.team); // still placeholder‑safe
        let rows = qb.build().fetch_all(&pool).await?;
        Ok(rows)
    }
}

pub async fn parse_request(
    req: Request<hyper::body::Incoming>,
) -> Result<HashMap<String, String>, ParseError> {
    let query = match req.uri().query() {
        Some(value) => value,
        None => return Err(ParseError::EmptyParse),
    };
    let mut request_hash_map: HashMap<String, String> = HashMap::new();
    for param in query.split("&") {
        let mut _iter = param.split("=");
        request_hash_map.insert(
            _iter.next().unwrap().to_string(),
            _iter.next().unwrap().to_string().replace("%20", " "),
        );
    }

    Ok(request_hash_map)
}

fn quote_ident(raw: &str) -> String {
    let escaped = raw.replace('"', "\"\"");
    format!("\"{escaped}\"")
}
