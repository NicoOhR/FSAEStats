use crate::db_structs;
use hyper::Error as HyperError;
use hyper::Request;
use serde::de::Error;
use serde::Serialize;
use serde_json::Value;
use sqlx::{sqlite::SqlitePool, FromRow};
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
    #[error("Hyper error: {0:?}")]
    Hyper(#[from] HyperError),
}

#[derive(Debug, Display, Clone)]
pub enum Event {
    Autocross,
    Accel,
    Skidpad,
    Endurance,
}

#[derive(Debug, Serialize, Clone)]
pub enum Response {
    Autocross(db_structs::AutocrossResults),
    Accel(db_structs::AccelResults),
    Endurance(db_structs::EnduranceResults),
    Skidpad(db_structs::SkidResults),
}

#[derive(Debug, Clone)]
pub struct EventRequest {
    pub team: String,
    pub year: String,
    pub event: Event,
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
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync>>;
}

impl FromString for Event {
    fn from_string(string: String) -> Result<Box<Self>, ParseError> {
        match string.to_lowercase().as_str() {
            "autocross" => Ok(Box::new(Event::Autocross)),
            "accel" | "acceleration" => Ok(Box::new(Event::Accel)),
            "skid" | "skidpad" => Ok(Box::new(Event::Skidpad)),
            "endurance" => Ok(Box::new(Event::Endurance)),
            _ => Err(ParseError::EventNotFound),
        }
    }
}

impl EventRequest {
    fn new(team: String, year: String, event: Event) -> Self {
        Self { team, year, event }
    }
}

impl RequestTrait for EventRequest {
    async fn handle(
        self,
        pool: SqlitePool,
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
        let query = match self.event {
            Event::Autocross => {
                let query = "SELECT * FROM autocross_results WHERE Team = ?";
                let row = sqlx::query_as::<_, db_structs::AutocrossResults>(query)
                    .bind(&self.team)
                    .fetch_one(&pool)
                    .await?;
                Response::Autocross(row)
            }
            Event::Accel => {
                let query = "SELECT * FROM accel_results WHERE Team = ?";
                let row = sqlx::query_as::<_, db_structs::AccelResults>(query)
                    .bind(&self.team)
                    .fetch_one(&pool)
                    .await?;
                Response::Accel(row)
            }
            Event::Endurance => {
                let query = "SELECT * FROM endurance_results WHERE Team = ?";
                let row = sqlx::query_as::<_, db_structs::EnduranceResults>(query)
                    .bind(&self.team)
                    .fetch_one(&pool)
                    .await?;
                Response::Endurance(row)
            }
            Event::Skidpad => {
                let query = "SELECT * FROM skidpad_results WHERE Team = ?";
                let row = sqlx::query_as::<_, db_structs::SkidResults>(query)
                    .bind(&self.team)
                    .fetch_one(&pool)
                    .await?;
                Response::Skidpad(row)
            }
        };

        Ok(query)
    }

    fn from_hash(args_map: &mut HashMap<String, String>) -> Result<Box<Self>, ParseError> {
        let team = match args_map.remove("team") {
            Some(value) => value,
            None => return Err(ParseError::IncorrectParse),
        };
        let year = match args_map.remove("year") {
            Some(value) => value,
            None => return Err(ParseError::IncorrectParse),
        };
        let event = match args_map.remove("event") {
            Some(value) => *Event::from_string(value)?,
            None => return Err(ParseError::IncorrectParse),
        };
        Ok(Box::new(Self { team, year, event }))
    }

    fn to_string(self) -> String {
        let req_as_string: String = format!(
            "team : {}, year : {}, event : {}",
            self.team, self.year, self.event
        );
        req_as_string
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
