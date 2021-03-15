use core::convert::TryFrom;
use pgx::*;
use proto_value::ProtoValue;
use std::collections::HashMap;

mod client;
mod oid;
mod proto_value;
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
    desc.iter()
        .enumerate()
        .map(|(i, attr)| {
            (
                attr.name().into(),
                client::pg::Type {
                    index: i as i32,
                    oid: client::pg::Oid::from(attr.type_oid()) as i32,
                },
            )
        })
        .collect()
}

fn into_values(row: Vec<pgx_fdw::Tuple>) -> Vec<prost_types::Value> {
    row.iter()
        .map(|(_name, datum, typeoid)| match typeoid {
            PgOid::BuiltIn(built_in) => ProtoValue::from_tuple(built_in, datum, typeoid).0,
            PgOid::Custom(_) => ProtoValue::from_tuple(&PgBuiltInOids::ANYOID, datum, typeoid).0,
            PgOid::InvalidOid => error!("InvalidOid")
        })
        .collect()
}

struct GRPCFdw {
    client: *mut client::Client,
    table_name: String,
    namespace: String,
}

impl GRPCFdw {
    pub fn connect(opts: &pgx_fdw::FdwOptions) -> Self {
        let uri = opts.server_opts.get("server_uri").unwrap();
        let endpoint = tonic::transport::Endpoint::try_from(uri.clone()).unwrap();
        let client = client::Client::connect(endpoint).unwrap();

        Self {
            client: Box::into_raw(Box::new(client)) as *mut client::Client,
            table_name: opts.table_name.clone(),
            namespace: opts.table_namespace.clone(),
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
            table: self.table_name.clone(),
            tupdesc: tupdesc_into_map(desc),
        });

        let response = client.execute(request);

        FdwWrapper(response).into_iter()
    }

    fn insert(&self, desc: &PgTupleDesc, row: Vec<pgx_fdw::Tuple>) -> Option<Vec<pgx_fdw::Tuple>> {
        let mut client = PgBox::<client::Client>::from_pg(self.client);
        let request = tonic::Request::new(client::pg::InsertRequest {
            table: self.table_name.clone(),
            tupdesc: tupdesc_into_map(desc),
            tuples: into_values(row),
        });

        let _ = client.insert(request);
        None
    }

    fn update(
        &self,
        desc: &PgTupleDesc,
        row: Vec<pgx_fdw::Tuple>,
        indices: Vec<pgx_fdw::Tuple>,
    ) -> Option<Vec<pgx_fdw::Tuple>> {
        let mut client = PgBox::<client::Client>::from_pg(self.client);
        let request = tonic::Request::new(client::pg::UpdateRequest {
            table: self.table_name.clone(),
            tupdesc: tupdesc_into_map(desc),
            tuples: into_values(row),
            indices: into_values(indices),
        });

        let _ = client.update(request);
        None
    }

    fn delete(
        &self,
        desc: &PgTupleDesc,
        tuples: Vec<pgx_fdw::Tuple>,
    ) -> Option<Vec<pgx_fdw::Tuple>> {
        let mut client = PgBox::<client::Client>::from_pg(self.client);
        let request = tonic::Request::new(client::pg::DeleteRequest {
            table: self.table_name.clone(),
            tupdesc: tupdesc_into_map(desc),
            indices: into_values(tuples),
        });

        let _ = client.delete(request);
        None
    }
}

/// ```sql
/// CREATE FUNCTION grpc_fdw_handler() RETURNS fdw_handler LANGUAGE c AS 'MODULE_PATHNAME', 'grpc_fdw_handler_wrapper';
/// ```
#[pg_extern]
fn grpc_fdw_handler() -> pg_sys::Datum {
    pgx_fdw::FdwState::<GRPCFdw>::into_datum()
}

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
