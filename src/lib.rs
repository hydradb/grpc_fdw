use core::convert::TryFrom;
use pgx::*;
use prost_types::value::*;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};

mod client;
mod oid;
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

fn from_json(value: &serde_json::Value) -> prost_types::Value {
    let kind = match value {
        JsonValue::Null => Kind::NullValue(0),
        JsonValue::Number(num) => Kind::NumberValue(num.as_f64().unwrap()),
        JsonValue::String(str) => Kind::StringValue(str.clone()),
        JsonValue::Bool(bool) => Kind::BoolValue(bool.clone()),
        JsonValue::Array(arr) => {
            let values = arr.iter().map(|v| from_json(v)).collect();
            let lv = prost_types::ListValue { values };

            Kind::ListValue(lv)
        }
        JsonValue::Object(map) => {
            let fields: BTreeMap<String, prost_types::Value> =
                map.iter().map(|(k, v)| (k.clone(), from_json(v))).collect();

            Kind::StructValue(prost_types::Struct { fields })
        }
    };

    prost_types::Value { kind: Some(kind) }
}

fn from_datum<T: FromDatum>(datum: &Option<pg_sys::Datum>, typoid: &PgOid) -> Option<T> {
    match datum {
        Some(d) => unsafe { T::from_datum(*d, false, typoid.value()) },
        None => None,
    }
}

fn into_value(
    oid: &pgx::PgBuiltInOids,
    datum: &Option<pg_sys::Datum>,
    typeoid: &PgOid,
) -> prost_types::Value {
    match oid {
        PgBuiltInOids::JSONBOID => {
            let JsonB(v) = from_datum::<JsonB>(datum, typeoid).unwrap();

            from_json(&v)
        }
        PgBuiltInOids::TEXTOID => {
            let v = from_datum::<String>(datum, typeoid).unwrap();

            prost_types::Value {
                kind: Some(Kind::StringValue(v)),
            }
        }
        PgBuiltInOids::INT8OID => {
            let v = from_datum::<i32>(datum, typeoid).unwrap();
            prost_types::Value {
                kind: Some(Kind::NumberValue(v.into())),
            }
        }
        _ => error!("TODO"),
    }
}

fn into_values(row: Vec<pgx_fdw::Tuple>) -> Vec<prost_types::Value> {
    row.iter()
        .map(|(_name, datum, typeoid)| match typeoid {
            PgOid::BuiltIn(built_in) => into_value(built_in, datum, typeoid),
            PgOid::Custom(_) => into_value(&PgBuiltInOids::ANYOID, datum, typeoid),
            PgOid::InvalidOid => error!("Invalid Oid"),
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
        _desc: &PgTupleDesc,
        _row: Vec<pgx_fdw::Tuple>,
        _indices: Vec<pgx_fdw::Tuple>,
    ) -> Option<Vec<pgx_fdw::Tuple>> {
        todo!()
    }

    fn delete(
        &self,
        _desc: &PgTupleDesc,
        _indices: Vec<pgx_fdw::Tuple>,
    ) -> Option<Vec<pgx_fdw::Tuple>> {
        todo!()
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
