use tokio::runtime::{Builder, Runtime};

pub mod pg {
    tonic::include_proto!("pg");
}

use pg::{fdw_client::FdwClient, ExecuteRequest, ResultSet};

pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = StdError> = ::std::result::Result<T, E>;

#[derive(Debug)]
pub struct Client {
    client: FdwClient<tonic::transport::Channel>,
    rt: Runtime,
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

    pub fn execute(&mut self, request: impl tonic::IntoRequest<ExecuteRequest>) -> Vec<ResultSet> {
        let mut stream = self
            .rt
            .block_on(self.client.execute(request))
            .unwrap()
            .into_inner();
        let mut v = Vec::new();
        while let Some(msg) = self.rt.block_on(stream.message()).unwrap() {
            v.push(msg);
        }

        v
    }
}
