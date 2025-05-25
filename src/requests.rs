use duckdb::Result;
use duckdb::*;
use hyper::Error as HyperError;
use hyper::Request;
use std::collections::HashMap;
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

    async fn handle(self, conn: duckdb::Connection) -> Result<Vec<arrow::array::RecordBatch>>;
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
    async fn handle(self, conn: duckdb::Connection) -> Result<Vec<arrow::array::RecordBatch>> {
        let query: String = format!("SELECT * FROM {} WHERE Team = '{}'", self.event, self.team);
        let mut stmt = conn.prepare(&query)?;
        let rbs = stmt.query_arrow([])?.collect();
        Ok(rbs)
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
