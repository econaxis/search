#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LockDataRefId {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Empty {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadRequest {
    #[prost(message, optional, tag = "1")]
    pub txn: ::core::option::Option<LockDataRefId>,
    #[prost(string, tag = "2")]
    pub key: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Kv {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteRequest {
    #[prost(message, optional, tag = "1")]
    pub txn: ::core::option::Option<LockDataRefId>,
    #[prost(message, optional, tag = "2")]
    pub kv: ::core::option::Option<Kv>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    #[prost(oneof = "value::Res", tags = "1, 2")]
    pub res: ::core::option::Option<value::Res>,
}
/// Nested message and enum types in `Value`.
pub mod value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Res {
        #[prost(string, tag = "1")]
        Val(::prost::alloc::string::String),
        #[prost(string, tag = "2")]
        Error(::prost::alloc::string::String),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WriteError {
    #[prost(oneof = "write_error::Res", tags = "1, 2")]
    pub res: ::core::option::Option<write_error::Res>,
}
/// Nested message and enum types in `WriteError`.
pub mod write_error {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Res {
        #[prost(string, tag = "1")]
        Error(::prost::alloc::string::String),
        #[prost(string, tag = "2")]
        Ok(::prost::alloc::string::String),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KvCollection {
    #[prost(message, repeated, tag = "1")]
    pub val: ::prost::alloc::vec::Vec<Kv>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueRanged {
    #[prost(oneof = "value_ranged::Res", tags = "1, 2")]
    pub res: ::core::option::Option<value_ranged::Res>,
}
/// Nested message and enum types in `ValueRanged`.
pub mod value_ranged {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Res {
        #[prost(message, tag = "1")]
        Val(super::KvCollection),
        #[prost(string, tag = "2")]
        Error(::prost::alloc::string::String),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Json {
    #[prost(string, tag = "1")]
    pub inner: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JsonWriteRequest {
    #[prost(string, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<Json>,
}
#[doc = r" Generated client implementations."]
pub mod replicator_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct ReplicatorClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ReplicatorClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ReplicatorClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + Sync + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ReplicatorClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            ReplicatorClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        #[doc = " Creates a new transaction"]
        pub async fn new_with_time(
            &mut self,
            request: impl tonic::IntoRequest<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.Replicator/new_with_time");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn serve_read(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadRequest>,
        ) -> Result<tonic::Response<super::Value>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.Replicator/serve_read");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn serve_range_read(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadRequest>,
        ) -> Result<tonic::Response<super::ValueRanged>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/metastore.Replicator/serve_range_read");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn serve_write(
            &mut self,
            request: impl tonic::IntoRequest<super::WriteRequest>,
        ) -> Result<tonic::Response<super::WriteError>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.Replicator/serve_write");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn commit(
            &mut self,
            request: impl tonic::IntoRequest<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.Replicator/commit");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn abort(
            &mut self,
            request: impl tonic::IntoRequest<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.Replicator/abort");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod main_replicator_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct MainReplicatorClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MainReplicatorClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MainReplicatorClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + Sync + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MainReplicatorClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            MainReplicatorClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn create_transaction(
            &mut self,
            request: impl tonic::IntoRequest<super::Empty>,
        ) -> Result<tonic::Response<super::LockDataRefId>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/metastore.MainReplicator/create_transaction",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn read(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadRequest>,
        ) -> Result<tonic::Response<super::Json>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.MainReplicator/read");
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Todo: better return type for write error/success messages"]
        pub async fn write(
            &mut self,
            request: impl tonic::IntoRequest<super::JsonWriteRequest>,
        ) -> Result<tonic::Response<super::Json>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.MainReplicator/write");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn abort(
            &mut self,
            request: impl tonic::IntoRequest<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.MainReplicator/abort");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn commit(
            &mut self,
            request: impl tonic::IntoRequest<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/metastore.MainReplicator/commit");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod replicator_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ReplicatorServer."]
    #[async_trait]
    pub trait Replicator: Send + Sync + 'static {
        #[doc = " Creates a new transaction"]
        async fn new_with_time(
            &self,
            request: tonic::Request<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status>;
        async fn serve_read(
            &self,
            request: tonic::Request<super::ReadRequest>,
        ) -> Result<tonic::Response<super::Value>, tonic::Status>;
        async fn serve_range_read(
            &self,
            request: tonic::Request<super::ReadRequest>,
        ) -> Result<tonic::Response<super::ValueRanged>, tonic::Status>;
        async fn serve_write(
            &self,
            request: tonic::Request<super::WriteRequest>,
        ) -> Result<tonic::Response<super::WriteError>, tonic::Status>;
        async fn commit(
            &self,
            request: tonic::Request<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status>;
        async fn abort(
            &self,
            request: tonic::Request<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ReplicatorServer<T: Replicator> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Replicator> ReplicatorServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ReplicatorServer<T>
    where
        T: Replicator,
        B: Body + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/metastore.Replicator/new_with_time" => {
                    #[allow(non_camel_case_types)]
                    struct new_with_timeSvc<T: Replicator>(pub Arc<T>);
                    impl<T: Replicator> tonic::server::UnaryService<super::LockDataRefId> for new_with_timeSvc<T> {
                        type Response = super::Empty;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LockDataRefId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).new_with_time(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = new_with_timeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.Replicator/serve_read" => {
                    #[allow(non_camel_case_types)]
                    struct serve_readSvc<T: Replicator>(pub Arc<T>);
                    impl<T: Replicator> tonic::server::UnaryService<super::ReadRequest> for serve_readSvc<T> {
                        type Response = super::Value;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReadRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).serve_read(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = serve_readSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.Replicator/serve_range_read" => {
                    #[allow(non_camel_case_types)]
                    struct serve_range_readSvc<T: Replicator>(pub Arc<T>);
                    impl<T: Replicator> tonic::server::UnaryService<super::ReadRequest> for serve_range_readSvc<T> {
                        type Response = super::ValueRanged;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReadRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).serve_range_read(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = serve_range_readSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.Replicator/serve_write" => {
                    #[allow(non_camel_case_types)]
                    struct serve_writeSvc<T: Replicator>(pub Arc<T>);
                    impl<T: Replicator> tonic::server::UnaryService<super::WriteRequest> for serve_writeSvc<T> {
                        type Response = super::WriteError;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::WriteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).serve_write(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = serve_writeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.Replicator/commit" => {
                    #[allow(non_camel_case_types)]
                    struct commitSvc<T: Replicator>(pub Arc<T>);
                    impl<T: Replicator> tonic::server::UnaryService<super::LockDataRefId> for commitSvc<T> {
                        type Response = super::Empty;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LockDataRefId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).commit(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = commitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.Replicator/abort" => {
                    #[allow(non_camel_case_types)]
                    struct abortSvc<T: Replicator>(pub Arc<T>);
                    impl<T: Replicator> tonic::server::UnaryService<super::LockDataRefId> for abortSvc<T> {
                        type Response = super::Empty;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LockDataRefId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).abort(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = abortSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Replicator> Clone for ReplicatorServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Replicator> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Replicator> tonic::transport::NamedService for ReplicatorServer<T> {
        const NAME: &'static str = "metastore.Replicator";
    }
}
#[doc = r" Generated server implementations."]
pub mod main_replicator_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with MainReplicatorServer."]
    #[async_trait]
    pub trait MainReplicator: Send + Sync + 'static {
        async fn create_transaction(
            &self,
            request: tonic::Request<super::Empty>,
        ) -> Result<tonic::Response<super::LockDataRefId>, tonic::Status>;
        async fn read(
            &self,
            request: tonic::Request<super::ReadRequest>,
        ) -> Result<tonic::Response<super::Json>, tonic::Status>;
        #[doc = " Todo: better return type for write error/success messages"]
        async fn write(
            &self,
            request: tonic::Request<super::JsonWriteRequest>,
        ) -> Result<tonic::Response<super::Json>, tonic::Status>;
        async fn abort(
            &self,
            request: tonic::Request<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status>;
        async fn commit(
            &self,
            request: tonic::Request<super::LockDataRefId>,
        ) -> Result<tonic::Response<super::Empty>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MainReplicatorServer<T: MainReplicator> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: MainReplicator> MainReplicatorServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for MainReplicatorServer<T>
    where
        T: MainReplicator,
        B: Body + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/metastore.MainReplicator/create_transaction" => {
                    #[allow(non_camel_case_types)]
                    struct create_transactionSvc<T: MainReplicator>(pub Arc<T>);
                    impl<T: MainReplicator> tonic::server::UnaryService<super::Empty> for create_transactionSvc<T> {
                        type Response = super::LockDataRefId;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::Empty>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create_transaction(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = create_transactionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.MainReplicator/read" => {
                    #[allow(non_camel_case_types)]
                    struct readSvc<T: MainReplicator>(pub Arc<T>);
                    impl<T: MainReplicator> tonic::server::UnaryService<super::ReadRequest> for readSvc<T> {
                        type Response = super::Json;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReadRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).read(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = readSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.MainReplicator/write" => {
                    #[allow(non_camel_case_types)]
                    struct writeSvc<T: MainReplicator>(pub Arc<T>);
                    impl<T: MainReplicator> tonic::server::UnaryService<super::JsonWriteRequest> for writeSvc<T> {
                        type Response = super::Json;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::JsonWriteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).write(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = writeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.MainReplicator/abort" => {
                    #[allow(non_camel_case_types)]
                    struct abortSvc<T: MainReplicator>(pub Arc<T>);
                    impl<T: MainReplicator> tonic::server::UnaryService<super::LockDataRefId> for abortSvc<T> {
                        type Response = super::Empty;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LockDataRefId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).abort(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = abortSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/metastore.MainReplicator/commit" => {
                    #[allow(non_camel_case_types)]
                    struct commitSvc<T: MainReplicator>(pub Arc<T>);
                    impl<T: MainReplicator> tonic::server::UnaryService<super::LockDataRefId> for commitSvc<T> {
                        type Response = super::Empty;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LockDataRefId>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).commit(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = commitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: MainReplicator> Clone for MainReplicatorServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: MainReplicator> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: MainReplicator> tonic::transport::NamedService for MainReplicatorServer<T> {
        const NAME: &'static str = "metastore.MainReplicator";
    }
}
