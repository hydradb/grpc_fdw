use core::convert::TryFrom;
use pgx::*;
use std::collections::HashMap;


mod client;
mod rs;

pg_module_magic!();

struct FdwWrapper(Vec<client::pg::ResultSet>);

impl Iterator for FdwWrapper {
    type Item = Vec<Option<pg_sys::Datum>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.pop() {
            Some(rs) => Some(rs.into_datums()),
            None => None,
        }
    }
}

fn tupdesc_into_map(desc: &PgTupleDesc) -> HashMap<String, client::pg::Type> {
    let mut map = HashMap::new();
    for (i, attr) in desc.iter().enumerate() {

        let name: String = attr.name().into();

        map.insert(name, client::pg::Type {
            index: i as i32,
            oid: client::pg::Oid::Textoid as i32
        });
    };

    map
}

struct GRPCFdw {
    client: *mut client::Client,
}

impl GRPCFdw {
    pub fn connect(opts: &pgx_fdw::FdwOptions) -> Self {
        let uri = opts.server_opts.get("server_uri").unwrap();
        let endpoint = tonic::transport::Endpoint::try_from(uri.clone()).unwrap();
        let client = client::Client::connect(endpoint).unwrap();

        Self {
            client: Box::into_raw(Box::new(client)) as *mut client::Client,
        }
    }
}

impl pgx_fdw::ForeignData for GRPCFdw {
    type Item = Option<pg_sys::Datum>;
    type RowIterator = FdwWrapper;

    fn begin(opts: &pgx_fdw::FdwOptions) -> Self {
        GRPCFdw::connect(&opts)
    }

    fn execute(&mut self, desc: &PgTupleDesc) -> Self::RowIterator {
        let mut client = PgBox::<client::Client>::from_pg(self.client);
        let request = tonic::Request::new(client::pg::ExecuteRequest {
            table: "Tonic".into(),
            tupdesc: tupdesc_into_map(desc)
        });

        let response = client.execute(request);

        FdwWrapper(response).into_iter()
    }
}

/// ```sql
/// CREATE FUNCTION grpc_fdw_handler() RETURNS fdw_handler LANGUAGE c AS 'MODULE_PATHNAME', 'grpc_fdw_handler_wrapper';
/// ```
#[pg_extern]
fn grpc_fdw_handler() -> pg_sys::Datum {
    pgx_fdw::FdwState::<GRPCFdw>::into_datum()
}

extension_sql!(
    r#"
    CREATE FOREIGN DATA WRAPPER grpc_fdw_handler HANDLER grpc_fdw_handler NO VALIDATOR;
    CREATE SERVER user_srv FOREIGN DATA WRAPPER grpc_fdw_handler OPTIONS (server_uri 'http://[::1]:50051');
    create foreign table hello_world (
        message text,
        from_server text,
        server_version integer
    ) server user_srv options (
        table_option '1',
        table_option2 '2'
    );
"#
);

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_hello_grpc_fdw() {
        // assert_eq!("Hello, grpc_fdw", crate::hello_grpc_fdw());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
