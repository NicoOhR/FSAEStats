use std::net::SocketAddr;

use hyper::{body::Bytes,
    server::conn::http1,
    service::service_fn,
    {Request, Response, Method, StatusCode}
};

use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};

async fn user_request(
    req: Request<hyper::body::Incoming>
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error>{
    match (req.method(), req.uri().path()) {
        (&Method::GET , "/") => Ok(Response::new(full(
            "Try POSTing Data to /user_request",
        ))),
        (&Method::POST, "/user_request") => {
            Ok(Response::new(req.into_body().boxed()))
        },
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
            if let Err(err) = http1::Builder::new().serve_connection(io, service_fn(user_request)).await{
                eprintln!("Error Serving Connection: {:?}", err);
            }
        });
    }
}
