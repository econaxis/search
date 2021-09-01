use crate::{ObjectPath, TypedValue};
use crate::rpc_handler::{DatabaseInterface, NetworkResult};
use crate::rwtransaction_wrapper::{LockDataRef, ValueWithMVCC};
use std::iter::FromIterator;

pub struct LocalReplicationHandler<A> {
    nodes: Vec<Box<A>>,
    replication_factor: u8,
}
impl<A: DatabaseInterface> DatabaseInterface for LocalReplicationHandler<A> {
    fn new_transaction(&self, txn: &LockDataRef) -> NetworkResult<(), String>{
        self.iter(|a| a.new_transaction(txn));
        NetworkResult::default()
    }

    fn serve_read(&self, txn: LockDataRef, key: &ObjectPath) -> NetworkResult<ValueWithMVCC, String> {
        self.nodes.get(0).unwrap().serve_read(txn, key)
    }

    fn serve_range_read(&self, txn: LockDataRef, key: &ObjectPath) -> NetworkResult<Vec<(ObjectPath, ValueWithMVCC)>, String> {
        self.nodes.get(0).unwrap().serve_range_read(txn, key)
    }

    fn serve_write(&self, txn: LockDataRef, key: &ObjectPath, value: TypedValue) -> NetworkResult<(), String> {
        self.iter_result(|a| a.serve_write(txn, key, value.clone()))
    }

    fn commit(&self, txn: LockDataRef) -> NetworkResult<(), String> {
        self.iter(|a| {
            a.commit(txn);
        });
        Default::default()
    }

    fn abort(&self, p0: LockDataRef) -> NetworkResult<(), String> {
        self.iter(|a| a.abort(p0));
        Default::default()
    }
}

impl<A> LocalReplicationHandler<A> {
    pub fn new<Creator: Fn() -> A>(num: u8, creator: Creator) -> Self {
        let boxer = || Box::new(creator());
        let iter = std::iter::repeat_with(boxer).take(num as usize);
        Self {
            nodes: Vec::from_iter(iter),
            replication_factor: num as u8
        }
    }
    fn iter_result<Ret, Err, Func: FnMut(&Box<A>) -> NetworkResult<Ret, Err>>(&self, function: Func) -> NetworkResult<Ret, Err> {
        self.nodes.iter().map(function).reduce(|a, b| a.and(b)).unwrap()
    }
    fn iter<Ret, Func: FnMut(&Box<A>) -> Ret>(&self, function: Func) -> Ret {
        self.nodes.iter().map(function).last().unwrap()
    }
}
