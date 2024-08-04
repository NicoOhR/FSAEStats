use hyper::Error as HyperError;
use hyper::Request;
use std::collections::HashMap;
use strum_macros::Display;
use thiserror::Error;

//realistically, the combined error type should exist in server.rs
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Request must contain query")]
    EmptyParse,
    #[error("Request does not contian the matching keys")]
    IncorrectParse,
    #[error("Event copuld not be found")]
    EventNotFound,
    #[error("Hyper error: {0:?}")]
    Hyper(#[from] HyperError),
}

#[derive(Debug, Display)]
pub enum Event {
    Autocross,
    Accel,
    Skidpad,
    Endurance,
}

#[derive(Debug)]
pub struct EventRequest {
    pub team: String,
    pub year: String,
    pub event: Event, //this should be an event sum type
}

impl Event {
    pub fn from_string(string: String) -> Result<Self, ParseError> {
        match string.to_lowercase().as_str() {
            "autocross" => Ok(Event::Autocross),
            "accel" | "acceleration" => Ok(Event::Accel),
            "skid" | "skidpad" => Ok(Event::Skidpad),
            "endurance" => Ok(Event::Endurance),
            _ => {
                return Err(ParseError::EventNotFound);
            }
        }
    }
}

impl EventRequest {
    fn new(team: String, year: String, event: Event) -> Self {
        Self { team, year, event }
    }

    // need to make macro for this :(
    pub fn from_hash(args_map: &mut HashMap<String, String>) -> Result<Self, ParseError> {
        let team = match args_map.remove("team") {
            Some(value) => value,
            None => return Err(ParseError::IncorrectParse),
        };
        let year = match args_map.remove("year") {
            Some(value) => value,
            None => return Err(ParseError::IncorrectParse),
        };
        let event = match args_map.remove("event") {
            Some(value) => Event::from_string(value)?,
            None => return Err(ParseError::IncorrectParse),
        };
        Ok(Self { team, year, event })
    }

    pub fn to_string(self) -> String {
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
    //team=TeamName&year=CompYear&event=WhatEvent
    let query = match req.uri().query() {
        Some(value) => value,
        None => return Err(ParseError::EmptyParse),
    };
    let mut request_hash_map: HashMap<String, String> = HashMap::new();
    for param in query.split("&") {
        let mut _iter = param.split("=");
        request_hash_map.insert(
            _iter.next().unwrap().to_string(),
            _iter.next().unwrap().to_string(),
        );
    }

    Ok(request_hash_map)
}
