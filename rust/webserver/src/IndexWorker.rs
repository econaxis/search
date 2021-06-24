use std::sync::{mpsc, Arc, Mutex};
use super::RustVecInterface::{C_SSK};
use super::cffi;
use std::{thread, fs};
use std::ops::{Deref, DerefMut};
use std::ffi::{CString};
use std::os::raw::c_char;
use std::path::Path;

use std::task::{Context, Poll};


use tracing::{info, span, debug, Level, event};


use serde::Serialize;
use std::io::Read;

type WorkMessage = Option<(Vec<String>, Arc<Mutex<SharedState>>)>;

struct SharedState {
    pub data: Option<Vec<u8>>,
    pub waker: Option<std::task::Waker>,
}

impl SharedState {
    fn set_data_and_wake(&mut self, r: Vec<u8>) {
        self.data.replace(r);
        self.waker.take().unwrap().wake();
    }
    fn get_data(&mut self) -> Option<Vec<u8>> {
        self.data.take()
    }
}



pub struct IndexWorker {
    thread_handle: Option<std::thread::JoinHandle<()>>,
    indices: Arc<Vec<C_SSK>>,
    sender: Mutex<mpsc::Sender<WorkMessage>>,
}

unsafe impl Send for IndexWorker {}

impl Clone for IndexWorker {
    fn clone(&self) -> Self {
        debug!("Cloning IndexWorker");

        let (sender, receiver) = mpsc::channel();

        let indices: Vec<C_SSK> = self.indices.iter().cloned().collect();

        let indices = Arc::new(indices);

        let thread_handle = Self::create_thread(receiver, indices.clone());
        IndexWorker {
            thread_handle: Some(thread_handle),
            indices,
            sender: Mutex::new(sender),
        }
    }
}

fn str_to_char_char(a: &[String]) -> Vec<CString> {
    let strs: Vec<CString> = a.iter().map(|x| {
        CString::new(x.as_bytes()).unwrap()
    }).collect();
    strs
}

fn search_indices_wrapper(thread_indices: &Arc<Vec<C_SSK>>, query: &[String]) -> Vec<u8> {
    let chars = str_to_char_char(query);

    let chars: Vec<*const c_char> = chars.iter().map(|x| x.as_ptr()).collect();
    let chars: *const *const c_char = chars.as_ptr();

    let outputvec = Vec::new();
    let indices_ptrptr: Vec<*const cffi::ctypes::SortedKeysIndexStub> = thread_indices.iter().map(|x| *x.as_ref()).collect();


    unsafe {
        let _sp = span!(Level::DEBUG, "Search multi index").entered();
        cffi::search_multi_indices(thread_indices.len() as i32, indices_ptrptr.as_ptr(), query.len() as i32,
                                   chars, &outputvec)
    };
    outputvec
}



impl IndexWorker {
    pub fn new(suffices: Vec<String>) -> Self {
        let (sender, receiver) = mpsc::channel();

        let indices: Vec<C_SSK> = suffices.iter().map(|suffix| {
            C_SSK::from_file_suffix(suffix.as_ref())
        }).collect();

        let indices = Arc::new(indices);
        let thread_handle = Self::create_thread(receiver, indices.clone());
        IndexWorker {
            thread_handle: Some(thread_handle),
            indices,
            sender: Mutex::new(sender),
        }
    }

    fn create_thread(receiver: mpsc::Receiver<WorkMessage>,
                     thread_indices: Arc<Vec<C_SSK>>) -> thread::JoinHandle<()> {
        let builder = thread::Builder::new().name("index-worker".to_string());
        builder.spawn(move || {

            loop {
                let received = receiver.recv().unwrap();
                if let Some((query, sharedstate)) = received {
                    info!("Sending query: {:?}", query);
                    let outputvec = search_indices_wrapper(&thread_indices, &query);
                    sharedstate.lock().unwrap().set_data_and_wake(outputvec);
                } else {
                    // Option is none, so we exit the loop and close the thread.
                    break;
                }
            }
        }).unwrap()
    }


    pub async fn send_query_async(&self, query: &[String]) -> Vec<u8> {
        // Returns list of filenames.
        let mut sent = false;
        let ss = Arc::new(Mutex::new(SharedState {
            data: None,
            waker: None,
        }));
        let pollfn = |cx: &mut Context<'_>| {
            ss.lock().unwrap().waker.replace(cx.waker().clone());
            if !sent {
                self.sender.lock().unwrap().send(Some((Vec::from(query), ss.clone()))).unwrap();
                sent = true;
                Poll::Pending
            } else {
                match ss.lock().unwrap().get_data() {
                    None => Poll::Pending,
                    Some(x) => Poll::Ready(x)
                }
            }
        };

        futures::future::poll_fn(pollfn).await
    }
}


impl Drop for IndexWorker {
    fn drop(&mut self) {
        debug!("Dropping IW");
        self.sender.lock().unwrap().send(None).unwrap();
        self.thread_handle.take().unwrap().join().unwrap();
    }
}