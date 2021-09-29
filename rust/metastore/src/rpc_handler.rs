use std::convert::Infallible;
use std::iter::FromIterator;
use std::ops::{ControlFlow, FromResidual, Try};

use crate::replicated_slave::SelfContainedDb;
use crate::rwtransaction_wrapper::{LockDataRef, TypedValue, ValueWithMVCC};
use crate::ObjectPath;

#[derive(Debug)]
pub struct NetworkError(std::io::Error);

impl From<NetworkError> for String {
    fn from(a: NetworkError) -> Self {
        a.0.to_string()
    }
}
pub struct NetworkResult<R, E>(pub Result<Result<R, E>, NetworkError>);

impl<R, E> NetworkResult<R, E> {
    pub fn and(self, res: NetworkResult<R, E>) -> NetworkResult<R, E> {
        match self.0 {
            Ok(_) => res,
            Err(_) => self,
        }
    }
}

impl<R, E> From<Result<R, E>> for NetworkResult<R, E> {
    fn from(a: Result<R, E>) -> Self {
        Self(Ok(a))
    }
}

impl Default for NetworkResult<(), String> {
    fn default() -> Self {
        Self::from(Result::Ok(()))
    }
}


// todo: make an interface for GRPC only, specifically for async stuff.
// todo: terminate instruction.
pub trait DatabaseInterface {
    fn new_transaction(&self, txn: &LockDataRef) -> NetworkResult<(), String>;
    fn serve_read(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
    ) -> NetworkResult<ValueWithMVCC, String>;
    fn serve_range_read(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
    ) -> NetworkResult<Vec<(ObjectPath, ValueWithMVCC)>, String>;
    fn serve_write(
        &self,
        txn: LockDataRef,
        key: &ObjectPath,
        value: TypedValue,
    ) -> NetworkResult<(), String>;
    fn commit(&self, txn: LockDataRef) -> NetworkResult<(), String>;
    fn abort(&self, p0: LockDataRef) -> NetworkResult<(), String>;
}

impl<R, E> FromResidual<Result<std::convert::Infallible, NetworkError>> for NetworkResult<R, E> {
    fn from_residual(residual: Result<std::convert::Infallible, NetworkError>) -> Self {
        Self(Err(residual.unwrap_err()))
    }
}

impl<R, E> Try for NetworkResult<R, E> {
    type Output = Result<R, E>;
    type Residual = Result<std::convert::Infallible, NetworkError>;

    fn from_output(output: Self::Output) -> Self {
        NetworkResult(Ok(output))
    }

    fn branch(self) -> ControlFlow<Self::Residual, Self::Output> {
        match self.0 {
            Ok(a) => ControlFlow::Continue(a),
            Err(a) => ControlFlow::Break(Err(a)),
        }
    }
}
