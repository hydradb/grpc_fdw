use core::convert::TryFrom;
use pgx::*;
mod client;

pg_module_magic!();

impl client::pg::ResultSet {
    pub fn into_datums(self) -> Vec<Option<pg_sys::Datum>> {
        self.values
            .into_iter()
            .map(Self::value_into_datum)
            .collect()
    }

    fn value_into_datum(value: prost_types::Value) -> Option<pg_sys::Datum> {
        match value.kind {
            Some(prost_types::value::Kind::StringValue(str)) => str.into_datum(),
            Some(prost_types::value::Kind::NullValue(_)) => None,
            Some(prost_types::value::Kind::BoolValue(boolean)) => boolean.into_datum(),
            Some(prost_types::value::Kind::NumberValue(n)) => n.into_datum(),
            //Some(Kind::StructValue(sv)) => {
            //    let mut obj = Map::new();
            //    for (key, value) in sv.fields.iter() {
            //        obj.insert(String::from(key), as_json(&value.kind));
            //    }

            //    Value::Object(obj)
            //}
            // Some(Kind::ListValue(list)) => {
            //     Value::Array(list.values.iter().map(|v| as_json(&v.kind)).collect())
            // }
            // None => None,
            _ => None,
        }
    }
}
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

struct GRPCFdw {
    client: *mut client::Client,
}

impl GRPCFdw {
    pub fn connect(opts: &pgx_fdw::FdwOptions) -> Self {
        let uri = opts.server_opts.get("server_uri").unwrap();
        let endpoint = tonic::transport::Endpoint::try_from(uri.clone()).unwrap();
        let client = client::Client::connect(endpoint).unwrap();
        let boxed_client = Box::into_raw(Box::new(client)) as *mut client::Client;

        Self {
            client: boxed_client,
        }
    }
}

impl pgx_fdw::ForeignData for GRPCFdw {
    type Item = Option<pg_sys::Datum>;
    type RowIterator = FdwWrapper;

    fn begin(opts: &pgx_fdw::FdwOptions) -> Self {
        GRPCFdw::connect(&opts)
    }

    fn execute(&mut self, _desc: &PgTupleDesc) -> Self::RowIterator {
        let mut client = PgBox::<client::Client>::from_pg(self.client);
        let request = tonic::Request::new(client::pg::ExecuteRequest {
            table: "Tonic".into(),
        });

        warning!("OPTIONS {:?}", client);

        let response = client.execute(request);

        let wrapper = FdwWrapper(response);

        wrapper.into_iter()
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
