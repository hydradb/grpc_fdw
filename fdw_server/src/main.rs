use futures::Stream;
use pg::fdw_server::{Fdw, FdwServer};
use pg::{ExecuteRequest, ResultSet};
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
                kind: Some(prost_types::value::Kind::StringValue("PG-FDWServer".into())),
            },
            Value {
                kind: Some(prost_types::value::Kind::NumberValue(22 as f64)),
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
