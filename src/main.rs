use std::convert::Infallible;
use std::net::SocketAddr;

use hyper_util::rt::TokioIo;
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Frame;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{body::Body, Method, Request, Response, StatusCode};
use tokio::net::TcpListener;

async fn hello(_: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible >{
    Ok(Response::new(Full::new(Bytes::from("Hello World!"))))
}

async fn echo(
    req: Request<hyper::body::Incoming>
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error>{
    match (req.method(), req.uri().path()) {
        (&Method::GET , "/") => Ok(Response::new(full(
            "Try POSTing Data to /echo",
        ))),
        (&Method::GET, "/echo") => Ok(Response::new(req.into_body().boxed())),
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
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127,0,0,1], 3000));

    let listener = TcpListener::bind(addr).await?;

    loop{
        let (stream, _) = listener.accept().await?;

        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new().serve_connection(io, service_fn(echo)).await{
                eprintln!("Error Serving Connection: {:?}", err);
            }
        });
    }
}
