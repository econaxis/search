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
use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use serde::Deserialize;
use tracing::{debug, debug_span, Level, span};
use tracing::Instrument;

use crate::elapsed_span;
use crate::highlighter::{highlight_files, serialize_response_to_json};
use crate::IndexWorker::{IndexWorker, ResultsList};

pub struct HighlightRequest {
    query: Vec<String>,
    files: ResultsList,
}

fn make_err(str: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, str)
}

pub struct ApplicationState {
    pub iw: Vec<IndexWorker>,
    pub highlighting_jobs: Arc<Mutex<HashMap<u32, HighlightRequest>>>,
    pub jobs_counter: AtomicU32,
}

unsafe impl Send for ApplicationState {}

async fn broadcast_query(indices: &[IndexWorker], query: &[String]) -> ResultsList {
    let futures_list: Vec<_> = indices.iter().map(|iw| {
        iw.send_query_async(query)
    }).collect();

    let res = futures::future::join_all(futures_list);
    let res = res.instrument(debug_span!("Fanning out requests")).await;

    let _sp = debug_span!("Reducing requests").entered();
    let mut res = res.into_iter().reduce(|x1, x2| x1.join(x2)).unwrap_or_default();
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
    let (query, files) = {
        let jobs = data.highlighting_jobs.lock().await;
        let jobrequest = jobs.get(&highlightid).ok_or(make_err(&*format!("highlight request id {} not found", highlightid)))?;
        let HighlightRequest { query, files } = jobrequest;
        (query.clone(), files.clone())
    };

    debug!(flen = files.len(), "Received highlighting request");

    let starttime = elapsed_span::new_span();
    let res = tokio::task::spawn_blocking(move || {
        highlight_files(&files, query.as_slice())
    }).await?;

    Response::builder()
        .header("Content-Type", "application/json")
        .header("Server-Timing", starttime.elapsed().to_string())
        .body(Body::from(serialize_response_to_json(&res))).map_err(|e| make_err(&*format!("{}", e)))
}


async fn handle_request(data: &ApplicationState, query: &[String]) -> Result<Response<Body>, io::Error> {
    debug!(?query, "Started processing for ");
    let starttime = elapsed_span::new_span();

    let iw = &data.iw;

    let res = broadcast_query(iw, query).await;

    let id = data.jobs_counter.fetch_add(1, Ordering::Relaxed);

    // Since highlighting might be resource intensive, we don't want to block incoming connections.
    let mut highlight_jobs = data.highlighting_jobs.lock().await;

    // Truncate the results list to max 10 elements. We don't need to highlight anymore.
    // let res_trunc = &res[0..std::cmp::min(res.len(), 10)];
    // let res_trunc = res_trunc.to_vec();
    highlight_jobs.insert(id, HighlightRequest { query: query.to_vec(), files: ResultsList::from(res.to_vec()) });

    if highlight_jobs.len() > 50 {
        clear_highlight_tasks(id, &mut highlight_jobs);
    }

    let jsonout = serde_json::json!({
        "id": id,
        "data": res
    }).to_string();
    Ok(Response::builder()
        .header("Content-Type", "application/json")
        .header("Server-Timing", starttime.elapsed())
        .body(jsonout.into()).unwrap())
}

fn parse_url_query(uri: &hyper::Uri, query_term: &str) -> Result<Vec<String>, io::Error> {
    let query = uri.query().ok_or(io::Error::new(io::ErrorKind::Other, "Can't pull query"))?;
    let match_indices = query.match_indices(query_term).next().
        ok_or(io::Error::new(ErrorKind::Other, format!("{} query not found", query_term)))?.0;

    let query: Vec<String> = query[match_indices + query_term.len()..].split(|x| x == '+').map(|x| x.to_string()).collect();
    debug!(parsed = ?query);

    Ok(query)
}


async fn route_request(req: Request<Body>, data: Arc<ApplicationState>) -> Result<Response<Body>, io::Error> {
    let uri = req.uri().path();
    if uri.starts_with("/search") {
        let q = parse_url_query(req.uri(), "?q=")?;
        handle_request(data.deref(), &*q).await
    } else if uri.starts_with("/highlight") {
        let q = parse_url_query(req.uri(), "?id=")?.into_iter().next().ok_or(make_err("ID not found"))?;
        let q: u32 = q.parse().map_err(|_| make_err(&*format!("Couldn't parse int: {}", q)))?;
        highlight_handler(data.deref(), q).await
    } else {
        Err(io::Error::new(ErrorKind::Other, format!("no matching path found for {}", uri)))
    }
}


pub fn get_server(state: Arc<ApplicationState>) -> BoxFuture<'static, Result<(), hyper::Error>> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 8000));

    let make_svc = make_service_fn(move |_conn| {
        let state = state.clone();
        // service_fn converts our function into a `Service`
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                tokio::spawn(async {
                    println!("Spawned task");
                });
                route_request(req, state.clone())
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    return Box::pin(server);
}