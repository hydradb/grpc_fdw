use tonic::{transport::Server, Request, Response, Status};

use pg::fdw_server::{Fdw, FdwServer};
use pg::{HelloReply, HelloRequest};

pub mod pg {
    tonic::include_proto!("pg");
}

#[derive(Debug, Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Fdw for MyGreeter {
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let greeter = MyGreeter::default();

    Server::builder()
        .add_service(FdwServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
