use std::time::Duration;
use std::os::unix::prelude::JoinHandleExt;
use nix::sys::ptrace;
use nix::sys::signal::{Signal, kill, signal, SigHandler};
use nix::unistd::{fork, ForkResult};
use libc::c_int;
use nix::sys::wait::wait;

extern "C" fn stop(a: c_int) {
    println!("stop received");
}
extern "C" fn cont(a: c_int) {
    println!("cont received");
}


fn main() {
    match unsafe { fork() } {
        Ok(ForkResult::Child) => unsafe {
            ptrace::traceme();
            // signal(Signal::SIGSTOP, SigHandler::SigIgn).unwrap();
            // signal(Signal::SIGCONT, SigHandler::SigIgn).unwrap();
            loop {
                println!("a");
                std::thread::sleep(Duration::from_millis(300));
            }
        }
        Ok(ForkResult::Parent { child: pid }) => {
            ptrace::attach(pid).unwrap();
            wait();
            ptrace::cont(pid, None).unwrap();
            loop {
                kill(pid, Signal::SIGSTOP).unwrap();
                println!("stopped");
                std::thread::sleep(Duration::from_millis(1500));
                ptrace::step(pid, None).unwrap();
                wait();
                ptrace::cont(pid, None).unwrap();
                println!("started");
                std::thread::sleep(Duration::from_millis(1500));
            }
        },
        Err(err) => {
            panic!(err)
        }
    }
}
