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
    #[error("Graph could not be found")]
    GraphNotFound,
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

#[derive(Debug, Display, Clone)]
pub enum Graph {
    RunsLine,
}

#[derive(Debug, Serialize, Clone)]
pub enum Response {
    Autocross(db_structs::AutocrossResults),
    Accel(db_structs::AccelResults),
    Endurance(db_structs::EnduranceResults),
    Skidpad(db_structs::SkidResults),
    Runs(HashMap<String, f64>),
}

#[derive(Debug, Clone)]
pub struct EventRequest {
    pub team: String,
    pub year: String,
    pub event: Event,
}

#[derive(Debug, Clone)]
pub struct GraphRequest {
    pub team: String,
    pub year: String,
    pub event: Event,
    pub graph: Graph,
}

trait FromString {
    fn from_string(string: String) -> Result<Box<Self>, ParseError>;
}

pub trait RequestTrait {
    fn new(team: String, year: String, event: Event, graph: Graph) -> Self;

    fn from_hash(args_map: &mut HashMap<String, String>) -> Result<Box<Self>, ParseError>;

    fn to_string(self) -> String;

    async fn handle(
        self,
        pool: SqlitePool,
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync>>;
}

impl FromString for Graph {
    fn from_string(string: String) -> Result<Box<Self>, ParseError> {
        match string.to_lowercase().as_str() {
            //"scatter" => Ok(Box::new(Graph::Scatter)),
            "runs" => Ok(Box::new(Graph::RunsLine)),
            //"distribution" => Ok(Box::new(Graph::Distribution)),
            _ => Err(ParseError::GraphNotFound),
        }
    }
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

impl RequestTrait for GraphRequest {
    fn new(team: String, year: String, event: Event, graph: Graph) -> Self {
        Self {
            team,
            year,
            event,
            graph,
        }
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
            None => return Err(ParseError::EventNotFound),
        };
        let graph = match args_map.remove("graph") {
            Some(value) => *Graph::from_string(value)?,
            None => return Err(ParseError::GraphNotFound),
        };
        Ok(Box::new(Self {
            team,
            year,
            event,
            graph,
        }))
    }
    fn to_string(self) -> String {
        let req_as_string: String = format!(
            "team : {}, year : {}, event : {}, graph : {}",
            self.team, self.year, self.event, self.graph
        );
        req_as_string
    }
    async fn handle(
        self,
        pool: SqlitePool,
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
        let event_query = EventRequest {
            team: self.team,
            year: self.year,
            event: self.event,
        };

        let query = match self.graph {
            Graph::RunsLine => {
                let response = event_query.handle(pool).await?;
                let serialized = serde_json::to_value(response.clone())
                    .expect("Could not turn to JSON")
                    .to_string();
                println!("{}", serialized);
                match response {
                    Response::Autocross(data) => Ok(Response::Runs(get_times(data))),
                    Response::Accel(data) => Ok(Response::Runs(get_times(data))),
                    Response::Endurance(data) => Ok(Response::Runs(get_times(data))),
                    Response::Skidpad(data) => Ok(Response::Runs(get_times(data))),
                    _ => Err(Box::new(ParseError::GraphNotFound)),
                }
            }
            _ => Err(Box::new(ParseError::GraphNotFound)),
        };
        Ok(query?)
    }
}

fn get_times<T: Iterable>(data: T) -> HashMap<String, f64> {
    let mut times: HashMap<String, f64> = HashMap::new();
    for (field, value) in data.iter() {
        if field.contains("time") {
            let time_values = value
                .downcast_ref::<Option<f64>>()
                .copied()
                .unwrap()
                .unwrap_or(0.0);
            times.insert(field.to_string(), time_values);
            println!("{:?}", value);
        }
    }
    times
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

    fn new(team: String, year: String, event: Event, graph: Graph) -> Self {
        Self { team, year, event }
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
