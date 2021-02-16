use futures::{Stream, StreamExt};
use pg::fdw_server::{Fdw, FdwServer};
use pg::{ExecuteRequest, HelloReply, HelloRequest, ResultSet};
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
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = pg::HelloReply {
            message: format!("Hello {}!", request.into_inner().name).into(),
        };

        Ok(Response::new(reply))
    }

    type ExecuteStream =
        Pin<Box<dyn Stream<Item = Result<ResultSet, Status>> + Send + Sync + 'static>>;

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let result = ResultSet {
        values: vec![
            Value {
                kind: Some(prost_types::value::Kind::StringValue(
                    "Server Says Hello".into(),
                )), 
            },
            Value {
                kind: Some(prost_types::value::Kind::StringValue(
                    "PG-FDWServer".into(),
                )), 
            }
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
