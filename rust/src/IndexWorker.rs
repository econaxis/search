use std::sync::{mpsc, Arc, Mutex, Condvar};
use super::RustVecInterface::{VecDPP, C_SSK};
use super::cffi;
use std::{thread, fs, task};
use std::ops::{Deref, DerefMut};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;
use std::future::Future;
use std::task::{Context, Poll};
use std::pin::Pin;
use std::io::Read;
use std::time::Duration;
use std::sync::mpsc::TrySendError::Full;
use tracing::{info, span, debug, Level, event};

struct SharedState {
    pub data: Option<Vec<String>>,
    pub waker: Option<std::task::Waker>,
}

pub struct FutureTask {
    ss: Arc<Mutex<SharedState>>,
}


impl Future for FutureTask {
    type Output = Vec<String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let data = &mut self.ss.lock().unwrap();
        match data.data.take() {
            None => {
                std::mem::replace(&mut data.waker, Some(cx.waker().clone()));
                Poll::Pending
            }
            Some(x) => {
                println!("Received from poll: {:?}  ", x);
                Poll::Ready(x)
            }
        }
    }
}

pub struct IndexWorker {
    thread_handle: Option<std::thread::JoinHandle<()>>,
    indices: Arc<Mutex<Vec<C_SSK>>>,
    sender: mpsc::Sender<Option<(Vec<String>, Arc<Mutex<SharedState>>)>>,
}

fn str_to_char_char(a: &Vec<String>) -> Vec<CString> {
    let strs: Vec<CString> = a.iter().map(|x| {
        CString::new(x.as_bytes()).unwrap()
    }).collect();
    strs
}

impl IndexWorker {
    pub fn new<T: AsRef<str>>(suffices: &[T]) -> Self {
        let (sender, receiver) = mpsc::channel();

        let indices: Vec<C_SSK> = suffices.iter().map(|suffix| {
            let _sp = event!(Level::DEBUG, index_name = suffix.as_ref(), "loading file index");
            C_SSK::from_file_suffix(suffix.as_ref())
        }).collect();

        let indices = Arc::new(Mutex::new(indices));

        let thread_indices = indices.clone();
        let thread_handle = thread::spawn(move || {
            loop {
                let received: Option<(Vec<String>, Arc<Mutex<SharedState>>)> = receiver.recv().unwrap();
                if let Some((query, mut sharedstate)) = received {
                    info!("Sending query: {:?}", query);
                    let chars = str_to_char_char(&query);

                    let chars: Vec<*const c_char> = chars.iter().map(|x| x.as_ptr()).collect();
                    let chars: *const *const c_char = chars.as_ptr();

                    let outputvec = VecDPP::new();
                    let lockguard = thread_indices.lock().unwrap();
                    let indices_ptrptr: Vec<*const cffi::ctypes::SortedKeysIndexStub> = lockguard.iter().map(|x| *x.as_ref()).collect();


                    unsafe {
                        let _sp = span!(Level::INFO, "Search multi index").entered();
                        cffi::search_multi_indices(lockguard.len() as i32, indices_ptrptr.as_ptr(), query.len() as i32,
                                                   chars, &outputvec as *const VecDPP)
                    };
                    event!(Level::DEBUG, "Matched {} files. Max score: {}", outputvec.len(), max_score = outputvec.first().unwrap().1);

                    let buf = [0u8; 700];

                    let mut filenames: Vec<String> = Vec::new();
                    let _sp = span!(Level::INFO, "Get filenames").entered();
                    for i in &*outputvec {
                        let len = unsafe {
                            cffi::query_for_filename(*lockguard[i.2 as usize].as_ref(), i.0 as u32, buf.as_ptr() as *const c_char, buf.len() as u32)
                        } as usize;

                        if len >= buf.len() {
                            continue;
                        }

                        let str = CStr::from_bytes_with_nul(&buf[0..len]).unwrap();
                        filenames.push(str.to_string_lossy().into_owned());
                    }
                    std::mem::drop(_sp);

                    let mut sharedstate = sharedstate.lock().unwrap();
                    sharedstate.data.replace(filenames);
                    sharedstate.waker.take().unwrap().wake();
                } else {
                    // Option is none, so we exit the loop and close the thread.
                    break;
                }
            }
        });
        IndexWorker {
            thread_handle: Some(thread_handle),
            indices,
            sender,
        }
    }


    pub async fn send_query_async(&mut self, query: &Vec<String>) -> Vec<String> {
        // Returns list of filenames.
        let mut sent = false;
        let ss = Arc::new(Mutex::new(SharedState {
            data: Default::default(),
            waker: None,
        }));
        let pollfn = |cx: &mut Context<'_>| {
            if !sent {
                ss.lock().unwrap().waker.replace(cx.waker().clone());
                self.sender.send(Some((query.clone(), ss.clone())));
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

    //
    // pub fn poll_for_results(&mut self) -> Vec<String> {
    //     self.condvar.wait_while(self.result_buffer.lock().unwrap(), |x| {
    //         x.0 == self.prevresult
    //     });
    //
    //     let ret = self.result_buffer.lock().unwrap().1.clone();
    //
    //     let indices = self.indices.lock().unwrap();
    //
    //     let buf = [0u8; 1000];
    //
    //     let mut filenames: Vec<String> = Vec::new();
    //     for i in ret.deref() {
    //         let len = unsafe {
    //             cffi::query_for_filename(*indices[i.2 as usize].as_ref(), i.0 as u32, buf.as_ptr() as *const c_char, buf.len() as u32)
    //         } as usize;
    //
    //         if len >= buf.len() {
    //             continue;
    //         }
    //
    //         let str = CStr::from_bytes_with_nul(&buf[0..len]).unwrap();
    //         filenames.push(str.to_string_lossy().into_owned());
    //     }
    //     filenames
    // }
}

pub fn load_file_to_string(p: &Path) -> Option<String> {
    fs::read_to_string(p).ok()
}

impl Drop for IndexWorker {
    fn drop(&mut self) {
        self.sender.send(None);
        self.thread_handle.take().unwrap().join();
    }
}