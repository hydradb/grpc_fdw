use futures::Stream;
use pg::fdw_server::{Fdw, FdwServer};
use pg::{DeleteRequest, ExecuteRequest, InsertRequest, ResultSet, UpdateRequest};
use prost_types::Value;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};

pub mod pg {
    tonic::include_proto!("pg");
}

#[derive(Debug, Default)]
pub struct EchoFdw {
    rows: Arc<Vec<ResultSet>>,
}

#[tonic::async_trait]
impl Fdw for EchoFdw {
    type ExecuteStream =
        Pin<Box<dyn Stream<Item = Result<ResultSet, Status>> + Send + Sync + 'static>>;

    type InsertStream =
        Pin<Box<dyn Stream<Item = Result<ResultSet, Status>> + Send + Sync + 'static>>;

    type UpdateStream =
        Pin<Box<dyn Stream<Item = Result<ResultSet, Status>> + Send + Sync + 'static>>;

    type DeleteStream =
        Pin<Box<dyn Stream<Item = Result<ResultSet, Status>> + Send + Sync + 'static>>;

    async fn execute(
        &self,
        _request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let rows = self.rows.clone();

        tokio::spawn(async move {
            for row in &rows[..] {
                tx.send(Ok(row.clone())).await.unwrap()
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn insert(
        &self,
        _request: Request<InsertRequest>,
    ) -> Result<tonic::Response<Self::InsertStream>, Status> {
        let (_tx, rx) = mpsc::channel(4);
        let mut _rows = &self.rows;

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn update(
        &self,
        _request: Request<UpdateRequest>,
    ) -> Result<tonic::Response<Self::UpdateStream>, Status> {
        let (_tx, rx) = mpsc::channel(4);
        let mut _rows = &self.rows;

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    async fn delete(
        &self,
        _request: Request<DeleteRequest>,
    ) -> Result<Response<Self::DeleteStream>, tonic::Status> {
        let (_tx, rx) = mpsc::channel(4);
        let mut _rows = &self.rows;

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let result = ResultSet {
        values: vec![
            Value {
                kind: Some(prost_types::value::Kind::NumberValue(1 as f64)),
            },
            Value {
                kind: Some(prost_types::value::Kind::StringValue(
                    "Server Says Hello".into(),
                )),
            },
            Value {
                kind: Some(prost_types::value::Kind::StringValue("PG-FDWServer".into())),
            },
        ],
    };

    let fdw = EchoFdw {
        rows: Arc::new(vec![result]),
    };

    Server::builder()
        .add_service(FdwServer::new(fdw))
        .serve(addr)
        .await?;

    Ok(())
}
