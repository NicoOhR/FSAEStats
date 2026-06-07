use crate::request::*;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::{
    body::Bytes,
    {Method, Request, Response, StatusCode},
};
use polars::prelude::*;
use serde_json::from_slice;

pub async fn user_request(
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let body = req.collect().await?.to_bytes();
    dispatch(&method, &path, body).await
}

async fn dispatch(
    method: &Method,
    path: &str,
    body: Bytes,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Box<dyn std::error::Error + Send + Sync>> {
    match (method, path) {
        (&Method::POST, "/pipeline") => {
            //creates the pipeline in memory
            let request = match from_slice::<PipelineRequest>(&body) {
                Ok(r) => r,
                Err(e) => return Ok(bad_request(e.to_string())),
            };
            let op_errs = request.validate();
            if !op_errs.is_empty() {
                let body = serde_json::json!(
                    {"errors": op_errs.iter().map(|e| e.to_string()).collect::<Vec<_>>()});
                return Ok(bad_request(body.to_string()));
            }
            // Polars' lazy engine spins up its own async runtime for scans, which
            // panics if driven from inside the tokio runtime already running this
            // handler. Run the blocking resolve+serialize work on a blocking thread.
            let result = tokio::task::spawn_blocking(move || {
                let mut df = request.resolve()?;
                to_ipc_bytes(&mut df)
            })
            .await;
            let bytes = match result {
                Ok(Ok(b)) => b,
                Ok(Err(e)) => return Ok(server_error(e.to_string())),
                Err(e) => return Ok(server_error(e.to_string())),
            };
            let mut resp = Response::new(full(bytes));
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("application/vnd.apache.arrow.file"),
            );
            Ok(resp)
        }
        _ => {
            let mut resp = Response::new(empty());
            *resp.status_mut() = StatusCode::NOT_FOUND;
            Ok(resp)
        }
    }
}

fn bad_request(msg: String) -> Response<BoxBody<Bytes, hyper::Error>> {
    let mut resp = Response::new(full(msg));
    *resp.status_mut() = StatusCode::BAD_REQUEST;
    resp
}

fn server_error(msg: String) -> Response<BoxBody<Bytes, hyper::Error>> {
    let mut resp = Response::new(full(msg));
    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    resp
}

fn to_ipc_bytes(df: &mut DataFrame) -> PolarsResult<Vec<u8>> {
    let mut buf = Vec::new();
    IpcWriter::new(&mut buf).finish(df)?;
    Ok(buf)
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn call(method: &str, path: &str, body: &str) -> Response<BoxBody<Bytes, hyper::Error>> {
        dispatch(&method.parse().unwrap(), path, Bytes::from(body.to_owned()))
            .await
            .unwrap()
    }

    fn src(view: &str) -> String {
        format!(r#"{{"view": "{view}", "years": [2024], "competitions": ["michigan"]}}"#)
    }

    fn pipeline(view: &str, ops: &str) -> String {
        format!(r#"{{"src": {}, "ops": {ops}}}"#, src(view))
    }

    #[tokio::test]
    async fn unknown_route_returns_404() {
        let resp = call("GET", "/unknown", "").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn malformed_json_returns_400() {
        let resp = call("POST", "/pipeline", "not json at all").await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unknown_view_returns_400() {
        let json = pipeline("not_a_view", "[]");
        let resp = call("POST", "/pipeline", &json).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
    // valid_request_returns_200 is deferred until resolve() is implemented in request.rs
}
