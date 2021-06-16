use std::sync::{mpsc, Arc, Mutex};
use super::RustVecInterface::{VecDPP, C_SSK};
use super::cffi;
use std::{thread, fs};
use std::ops::{Deref, DerefMut};
use std::ffi::{CString};
use std::os::raw::c_char;
use std::path::Path;

use std::task::{Context, Poll};


use tracing::{info, span, debug, Level, event};


use serde::Serialize;
use std::collections::HashMap;
use lazy_static::lazy_static;
use std::cell::{RefCell};

struct SharedState {
    pub data: Option<ResultsList>,
    pub waker: Option<std::task::Waker>,
}

pub struct IndexWorker {
    thread_handle: Option<std::thread::JoinHandle<()>>,
    indices: Arc<Vec<C_SSK>>,
    sender: Mutex<mpsc::Sender<Option<(Vec<String>, Arc<Mutex<SharedState>>)>>>,
}

unsafe impl Send for IndexWorker {}


#[derive(Clone, Serialize, Default)]
pub struct ResultsList(Vec<(u32, String)>);

impl ResultsList {
    pub fn join(mut self, mut other: ResultsList) -> Self {
        self.0.append(&mut other.0);
        self
    }

    pub fn sort(&mut self) {
        self.0.as_mut_slice().sort_by_key(|tup| tup.0);
    }
}


impl From<Vec<(u32, String)>> for ResultsList {
    fn from(t: Vec<(u32, String)>) -> Self {
        Self(t)
    }
}

impl Deref for ResultsList {
    type Target = Vec<(u32, String)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ResultsList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Clone for IndexWorker {
    fn clone(&self) -> Self {
        debug!("Cloning IndexWorker");

        let (sender, receiver) = mpsc::channel();

        let indices: Vec<C_SSK> = self.indices.iter().map(|x| {
            x.clone()
        }).collect();

        let indices = Arc::new(indices);

        let thread_handle = Self::create_thread(receiver, indices.clone(), Vec::new());
        IndexWorker {
            thread_handle: Some(thread_handle),
            indices,
            sender: Mutex::new(sender),
        }
    }
}

lazy_static! {
    pub static ref filename_map : Mutex<RefCell<HashMap<String, String>>> = {
      Mutex::new(RefCell::new(HashMap::new()))
    };
}


fn str_to_char_char(a: &Vec<String>) -> Vec<CString> {
    let strs: Vec<CString> = a.iter().map(|x| {
        CString::new(x.as_bytes()).unwrap()
    }).collect();
    strs
}

impl IndexWorker {
    pub fn new(suffices: Vec<String>) -> Self {
        let (sender, receiver) = mpsc::channel();

        let indices: Vec<C_SSK> = suffices.iter().map(|suffix| {
            let _sp = event!(Level::DEBUG, index_name = suffix.as_str(), "loading file index");
            C_SSK::from_file_suffix(suffix.as_ref())
        }).collect();

        let indices = Arc::new(indices);

        let thread_indices = indices.clone();
        let thread_handle = Self::create_thread(receiver, thread_indices, suffices);
        IndexWorker {
            thread_handle: Some(thread_handle),
            indices,
            sender: Mutex::new(sender),
        }
    }

    fn create_thread(receiver: mpsc::Receiver<Option<(Vec<String>, Arc<Mutex<SharedState>>)>>,
                     thread_indices: Arc<Vec<C_SSK>>, suffices: Vec<String>) -> thread::JoinHandle<()> {
        let builder = thread::Builder::new().name("index-worker".to_string());
        builder.spawn(move || {
            loop {
                let received: Option<(Vec<String>, Arc<Mutex<SharedState>>)> = receiver.recv().unwrap();
                if let Some((query, sharedstate)) = received {
                    info!("Sending query: {:?}", query);
                    let chars = str_to_char_char(&query);

                    let chars: Vec<*const c_char> = chars.iter().map(|x| x.as_ptr()).collect();
                    let chars: *const *const c_char = chars.as_ptr();

                    let outputvec = VecDPP::new();
                    let indices_ptrptr: Vec<*const cffi::ctypes::SortedKeysIndexStub> = thread_indices.iter().map(|x| *x.as_ref()).collect();


                    unsafe {
                        let _sp = span!(Level::DEBUG, "Search multi index").entered();
                        cffi::search_multi_indices(thread_indices.len() as i32, indices_ptrptr.as_ptr(), query.len() as i32,
                                                   chars, &outputvec as *const VecDPP)
                    };
                    debug!("Matched {} files. Max score: {}", outputvec.len(),
                           max_score = outputvec.first().unwrap_or(&Default::default()).1);

                    let buf = [0u8; 300];

                    let mut results = ResultsList::from(Vec::new());
                    // let mut filenames_hash = HashSet::new();

                    let _sp = span!(Level::DEBUG, "Get filenames").entered();

                    for i in outputvec.deref() {
                        let len = unsafe {
                            cffi::query_for_filename(*thread_indices[i.2 as usize].as_ref(), i.0 as u32, buf.as_ptr() as *const c_char, buf.len() as u32)
                        } as usize;

                        // The last character at index `len`, is the null terminator.
                        let str = String::from_utf8_lossy(&buf[0..len - 1]).into_owned();

                        // Deduplicate names
                        // If we have two different StubIndex that somehow cover the same document,
                        // then it will lead to this document being included twice.
                        if results.iter().find(|x| x.1 == str).is_none() {
                            debug!(id = i.0, path = %str);

                            if let Some(suf) = suffices.get(i.2 as usize) {
                                filename_map.lock().unwrap().borrow_mut().insert(str.clone(), suf.clone());
                            }
                            results.push((i.1, str.to_owned()));
                        }
                    }
                    _sp.exit();
                    let mut sharedstate = sharedstate.lock().unwrap();

                    // Put the results into a sharedstate object, so the parent caller can access it.
                    sharedstate.data.replace(results);
                    sharedstate.waker.take().unwrap().wake();
                } else {
                    // Option is none, so we exit the loop and close the thread.
                    break;
                }
            }
        }).unwrap()
    }


    pub async fn send_query_async(&self, query: &[String]) -> ResultsList {
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
                match ss.lock().unwrap().data.take() {
                    None => Poll::Pending,
                    Some(x) => Poll::Ready(x)
                }
            }
        };

        futures::future::poll_fn(pollfn).await
    }
}

pub fn load_file_to_string(p: &Path) -> Option<String> {
    fs::read_to_string(p).ok()
}

impl Drop for IndexWorker {
    fn drop(&mut self) {
        debug!("Dropping IW");
        self.sender.lock().unwrap().send(None).unwrap();
        self.thread_handle.take().unwrap().join().unwrap();
    }
}