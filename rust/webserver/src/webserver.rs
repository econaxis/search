use io::ErrorKind;
use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::ops::{Deref};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use futures::future::BoxFuture;
use futures::lock::Mutex;
use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};
use tracing::{debug, Level, span};


use crate::elapsed_span;
use crate::IndexWorker::{IndexWorker};
use std::fs;


pub struct ApplicationState {
    pub iw: Vec<IndexWorker>,
    pub jobs_counter: AtomicU32,
}

pub struct Error(io::Error);

impl From<&str> for Error {
    fn from(c: &str) -> Self {
        Self(io::Error::new(io::ErrorKind::Other, c))
    }
}

impl From<String> for Error {
    fn from(c: String) -> Self {
        Self(io::Error::new(io::ErrorKind::Other, c))
    }
}

impl From<io::Error> for Error {
    fn from(c: io::Error) -> Self {
        Self(c)
    }
}

impl Into<io::Error> for Error {
    fn into(self) -> io::Error {
        self.0
    }
}

impl From<hyper::Error> for Error {
    fn from(c: hyper::Error) -> Self {
        Self::from(c.to_string())
    }
}

impl Into<Box<dyn std::error::Error + Send + Sync>> for Error {
    fn into(self) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(self.0)
    }
}


unsafe impl<'a> Send for ApplicationState {}

async fn broadcast_query<'a>(indices: &[IndexWorker], query: &[String]) -> Vec<u8> {
    let futures_list: Vec<_> = indices.iter().map(|iw| {
        iw.send_query_async(query)
    }).collect();


    let starttime = elapsed_span::new_span();
    let mut res = futures::future::join_all(futures_list).await;
    res.insert(0, vec![b'[']);
    let mut res = res.into_iter().reduce(|mut x1, mut x2| {
        x1.append(&mut x2);
        x1.extend(",".as_bytes());
        x1
    }).unwrap_or_default();
    res.truncate(res.len() - 1);
    res.push(b']');
    debug!("Fanning out requests + reduction. Duration: {}", starttime.elapsed());
    res
}


async fn handle_request<'a>(data: &ApplicationState, query: &[String]) -> Result<Response<Body>, Error> {
    debug!(?query, "Started processing for ");
    let starttime = elapsed_span::new_span();

    let iw = &data.iw;

    let res = broadcast_query(iw, query).await;

    let str = String::from_utf8(res).unwrap();

    Ok(Response::builder()
        .header("Content-Type", "application/json")
        .header("Server-Timing", starttime.elapsed())
        .body(str.into()).unwrap())
}

fn parse_url_query<'a>(uri: &'a hyper::Uri, query_term: &str) -> Result<Vec<&'a str>, Error> {
    let query = uri.query().ok_or("Can't pull query")?;
    let match_indices = query.match_indices(query_term).next().
        ok_or(format!("{} query not found", query_term))?.0;

    let mut query: Vec<&'a str> = query[match_indices + query_term.len()..].split(|x| x == '+').collect();

    let query = query.drain_filter(|x| !x.is_empty()).collect();
    Ok(query)
}

fn return_index_html() -> Result<Response<Body>, Error> {
    let idx_html = fs::read_to_string("../website/index.html")?;
    Ok(Response::builder().body(idx_html.into()).unwrap())
}

async fn route_request<'a>(req: Request<Body>, data: Arc<ApplicationState>) -> Result<Response<Body>, Error> {
    let uri = req.uri().path();
    if uri.starts_with("/search") {
        let q = parse_url_query(req.uri(), "q=")?;
        let q: Vec<String> = q.iter().map(|x| x.to_string()).collect();
        handle_request(data.deref(), q.as_slice()).await
    } else if uri == "/" || uri == "/index" {
        return_index_html()
    } else {
        Err(Error::from(format!("no matching path found for {}", uri)))
    }
}


pub fn get_server(state: ApplicationState) -> BoxFuture<'static, Result<(), hyper::Error>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    let state = Arc::from(state);

    let make_svc = make_service_fn(move |_conn| {
        // Since this outer closure is called everytime a new TCP connection comes in,
        // we have to clone the state into us.
        let state = state.clone();
        // service_fn converts our function into a `Service`
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                // Since this inner closure is called everytime a request10.1145/1277741.1277774 is made (from the same TCP connection),
                // have to clone the state again.
                route_request(req, state.clone())
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    Box::pin(server)
}