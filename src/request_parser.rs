use hyper::Request;
use http::uri;
use hyper::body::Incoming;
use std::io::Result;


//
#[derive(Debug)]
struct BasicRequest {
    team: String,
    year : String,
    event : String //this should be an event sum type
}

impl BasicRequest {
    fn new(team: String, year: String, event: String ) -> Self {
        Self { team, year, event }
    }
}

//return type should be made into a primitive
pub async fn parse_request(req : Request<hyper::body::Incoming>) -> Result<BasicRequest>{
    //the query looks like team=TeamName&year=CompYear&event=WhatEvent
    let query = req.uri().query().unwrap();
    let mut query_iterator = query.split("&"); 
    let team = String::from(query_iterator.next().unwrap());  
    let year= String::from(query_iterator.next().unwrap());  
    let event = String::from(query_iterator.next().unwrap());  
    Ok(BasicRequest::new(team, year, event))
}
