use tokio::runtime::{Builder, Runtime};

pub mod hello_world {
    tonic::include_proto!("helloworld");
}

use hello_world::{greeter_client::GreeterClient, HelloReply, HelloRequest};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = StdError> = ::std::result::Result<T, E>;

#[derive(Debug)]
pub struct Client {
    client: GreeterClient<tonic::transport::Channel>,
    rt: Runtime,
}

impl Client {
    pub fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let client = rt.block_on(GreeterClient::connect(dst))?;

        Ok(Self { rt, client })
    }

    pub fn say_hello(
        &mut self,
        request: impl tonic::IntoRequest<HelloRequest>,
    ) -> Result<tonic::Response<HelloReply>, tonic::Status> {
        self.rt.block_on(self.client.say_hello(request))
    }
}
