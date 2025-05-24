use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use sqlx::*;
use std::net::SocketAddr;
use tokio::net::TcpListener;
mod requests;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("Invalid port number");

    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;

        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(server::user_request))
                .await
            {
                eprintln!("Error Serving Connection: {:?}", err);
            }
        });
    }
}
