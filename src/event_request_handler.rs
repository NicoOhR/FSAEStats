use crate::request_parser::{self, EventRequest, ParseError};

struct AutocrossRun{
    raw_time: f32,
    cones: Option<u8>,
    off_course: Option<u8>,
    adjust_time: f32 
}

struct AccelerationRun{
    raw_time: f32,
    cones: Option<u8>,
    adjusted_time: f32
}
struct SkidpadRun{
    time_r : f32,
    time_l : f32,
    cones : Option<u8>,
    adjusted_time : f32
}

//endurance is one run
struct EnduranceEvent{
    time: f32,
    laps : u8,
    cones: Option<u8>,
    penalty: Option<u8>,
    adjusted_time : Option<u8>,
    time_score: f32,
    lap_score: u8,
    endurance_score: f32
}
struct FinalResult{
    best_time: f32,
    score: f32
}

enum Events{
    Autocross,
    Accel,
    Skidpad,
    Endurance
}

enum EventResponse{
    Autocross(Vec<Option<AutocrossRun>>, Option<FinalResult>),
    Accel(Vec<Option<AccelerationRun>>, Option<FinalResult>),
    Skidpad(Vec<Option<SkidpadRun>>, Option<FinalResult>),
    Endurance(Option<EnduranceEvent>)

}

pub fn request_handler(event_request : EventRequest) -> Result<EventResponse, ParseError>{
   let event = match event_request.event.to_lowercase().as_str() {
        "autocross" => Events::Autocross,
        "accel" | "acceleration" => Events::Accel, 
        "skid" | "skidpad" => Events::Skidpad, 
        "endurance" => Events::Endurance,
        _ => {
            return Err(ParseError::EventNotFound);
        }
    }; // done for the purpose of String -> Events type, should be handled on Parse :/
    
    //from here, make calls to the database, parse, formulate into EventResponse

}
