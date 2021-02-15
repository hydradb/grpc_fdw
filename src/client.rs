use pgx::*;
use prost_types::value::Kind;
use tokio::runtime::{Builder, Runtime};
use tonic::Streaming;

pub mod pg {
    tonic::include_proto!("pg");
}

use pg::{fdw_client::FdwClient, ExecuteRequest, HelloReply, HelloRequest, ResultSet};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = StdError> = ::std::result::Result<T, E>;

#[derive(Debug)]
pub struct Client {
    client: FdwClient<tonic::transport::Channel>,
    rt: Runtime,
}

fn from_value(value: prost_types::Value) -> String {
    match value.kind {
        Some(Kind::StringValue(str)) => str,
        _ => "not_f".into(),
    }
}

fn unpack(msg: ResultSet) -> Vec<String> {
    msg.values.into_iter().map(from_value).collect()
}

impl Client {
    pub fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<StdError>,
    {
        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let client = rt.block_on(FdwClient::connect(dst))?;

        Ok(Self { rt, client })
    }

    pub fn say_hello(
        &mut self,
        request: impl tonic::IntoRequest<HelloRequest>,
    ) -> Result<tonic::Response<HelloReply>, tonic::Status> {
        self.rt.block_on(self.client.say_hello(request))
    }
    // Result<tonic::Response<Streaming<ResultSet>>, tonic::Status>
    pub fn execute(
        &mut self,
        request: impl tonic::IntoRequest<ExecuteRequest>,
    ) -> impl Iterator<Item = Vec<String>> {
        let mut stream = self
            .rt
            .block_on(self.client.execute(request))
            .unwrap()
            .into_inner();
        let mut v = Vec::new();
        while let Some(msg) = self.rt.block_on(stream.message()).unwrap() {
            v.push(unpack(msg));
        }

        v.into_iter()
    }
}
