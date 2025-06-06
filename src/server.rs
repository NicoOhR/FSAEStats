use std::io::Cursor;

use crate::requests::*;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Method, Request, Response, StatusCode},
};

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
            let mut buf = Vec::new();
            let mut writer = arrow::json::LineDelimitedWriter::new(&mut buf);
            response.iter().for_each(|x| writer.write(x).unwrap());
            writer.finish()?;
            let strings = (String::from_utf8(buf)?).to_string();
            Ok(Response::new(full(strings)))
        }
        (&Method::GET, "/event_arrow") => {
            let mut request = parse_request(req).await?;
            let response = UserRequest::from_hash(&mut request)?.handle(pool).await?;
            let mut buf = Cursor::new(Vec::new());
            {
                let mut writer =
                    arrow::ipc::writer::StreamWriter::try_new(&mut buf, &*response[0].schema())?;

                response.iter().for_each(|x| writer.write(x).unwrap());

                writer.finish()?;
            }
            Ok(Response::new(full(buf.into_inner())))
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
