use std::convert::Infallible;
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server, StatusCode, http};
use hyper::service::{make_service_fn, service_fn};

use futures::future::BoxFuture;
use futures::lock::Mutex;

use crate::IndexWorker::{IndexWorker, ResultsList};
use std::sync::{Arc};
use serde::{Deserialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::HashMap;
use tracing::Instrument;


use crate::highlighter::{highlight_files, serialize_response_to_json};
use tracing::{debug, debug_span, span, Level};
use std::io;
use io::ErrorKind;
use std::ops::Deref;

pub struct HighlightRequest {
    query: Vec<String>,
    files: ResultsList,
}

pub struct ApplicationState {
    pub iw: Vec<IndexWorker>,
    pub highlighting_jobs: Arc<Mutex<HashMap<u32, HighlightRequest>>>,
    pub jobs_counter: AtomicU32,
}

unsafe impl Send for ApplicationState {}

#[derive(Deserialize, Debug)]
struct Query {
    q: String,
}

async fn broadcast_query(indices: &[IndexWorker], query: &[String]) -> ResultsList {
    let futures_list: Vec<_> = indices.iter().map(|iw| {
        iw.send_query_async(query)
    }).collect();

    let res = futures::future::join_all(futures_list);
    let res = res.instrument(debug_span!("Fanning out requests")).await;

    let _sp = debug_span!("Reducing requests").entered();
    let mut res = res.into_iter().reduce(|x1, x2| x1.join(x2)).unwrap();
    res.sort();
    res
}


fn clear_highlight_tasks(cur_docid: u32, highlight_queue: &mut HashMap<u32, HighlightRequest>) {
    let _sp = span!(Level::DEBUG, "clearing highlight queue", length = highlight_queue.len()).entered();
    if highlight_queue.len() > 50 {
        let limit = cur_docid.saturating_sub(5);
        highlight_queue.retain(|k, _v| *k > limit);
    }
}

async fn highlight_handler(data: &ApplicationState, highlightid: u32) -> Result<Response<Body>, io::Error> {
    let jobs = data.highlighting_jobs.lock().await;
    let jobrequest = jobs.get(&highlightid).ok_or(io::Error::new(io::ErrorKind::Other, format!("highlight request id {} not found", highlightid)))?;
    let HighlightRequest { query, files } = jobrequest;
    let query = query.clone();
    let files = files.clone();
    std::mem::drop(jobs);

    debug!(flen = files.len(), "Received highlighting request");

    let res = tokio::task::spawn_blocking(move || {
        highlight_files(&files, query.as_slice())
    });

    let res = res.await?;


    Response::builder()
        .header("Content-Type", "application/json")
        .body(Body::from(serialize_response_to_json(&res))).map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))
}


async fn handle_request(data: &ApplicationState, query: &[String]) -> Result<Response<Body>, io::Error> {
    debug!(?query, "Started processing for ");

    let iw = &data.iw;

    let res = broadcast_query(iw, query).await;

    let id = data.jobs_counter.fetch_add(1, Ordering::Relaxed);

    // Since highlighting might be resource intensive, we don't want to block incoming connections.
    let mut highlight_jobs = data.highlighting_jobs.lock().await;

    // Truncate the results list to max 10 elements. We don't need to highlight anymore.
    // let res_trunc = &res[0..std::cmp::min(res.len(), 10)];
    // let res_trunc = res_trunc.to_vec();
    highlight_jobs.insert(id, HighlightRequest { query: query.to_vec(), files: ResultsList::from(res.to_vec()) });

    if highlight_jobs.len() > 100 {
        clear_highlight_tasks(id, &mut highlight_jobs);
    }

    let jsonout = serde_json::json!({
        "id": id,
        "data": res
    });
    Ok(Response::new(Body::from(jsonout.to_string())))
}

fn parse_url_query(uri: &hyper::Uri) -> Result<Vec<String>, io::Error> {
    let query = uri.query().ok_or(io::Error::new(io::ErrorKind::Other, "Can't pull query"))?;
    let match_indices = query.match_indices("q=").next().
        ok_or(io::Error::new(ErrorKind::Other, "?q query not found"))?.0;

    let query: Vec<String> = query[match_indices + 2..].split(|x| x == '+').map(|x| x.to_string()).collect();
    debug!(parsed = ?query);

    Ok(query)
}


async fn route_request(req: Request<Body>, data: Arc<ApplicationState>) -> Result<Response<Body>, io::Error> {
    let uri = req.uri().path();
    if uri.starts_with("/search") {
        let q = parse_url_query(req.uri())?;
        handle_request(data.deref(), &*q).await
    } else {
        Err(io::Error::new(ErrorKind::Other, format!("no matching path found for {}", uri)))
    }
}


pub fn get_server() -> BoxFuture<'static, Result<(), hyper::Error>> {
    debug!("starting server");
    // We'll bind to 127.0.0.1:3000
    let addr = SocketAddr::from(([0, 0, 0, 0], 8000));

    let make_svc = make_service_fn(move |_conn| {
        debug!("Making service func");
        // let state = state.clone();
        // service_fn converts our function into a `Service`
        async move {
            Ok::<_, Infallible>(service_fn(|req: Request<Body>| async move {
                Ok::<_, String>(Response::new(Body::from("fdsaf")))
                // route_request(req, state.clone())
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    return Box::pin(server);
}