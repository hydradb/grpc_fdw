use pgx::*;
use prost_types::value::*;
use prost_types::Value;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub struct ProtoValue(pub prost_types::Value);

impl<'a> From<&'a JsonValue> for ProtoValue {
    fn from(json: &'a JsonValue) -> Self {
        let kind = match json {
            JsonValue::Null => Kind::NullValue(0),
            JsonValue::Number(num) => Kind::NumberValue(num.as_f64().unwrap()),
            JsonValue::String(str) => Kind::StringValue(str.clone()),
            JsonValue::Bool(bool) => Kind::BoolValue(bool.clone()),
            JsonValue::Array(arr) => {
                let values = arr.iter().map(|v| Self::from(v).0).collect();
                let lv = prost_types::ListValue { values };

                Kind::ListValue(lv)
            }
            JsonValue::Object(map) => {
                let fields: BTreeMap<String, Value> = map
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::from(v).0))
                    .collect();

                Kind::StructValue(prost_types::Struct { fields })
            }
        };

        Self(prost_types::Value { kind: Some(kind) })
    }
}

impl ProtoValue {
    fn from_datum<T: FromDatum>(datum: &Option<pg_sys::Datum>, typoid: &PgOid) -> Option<T> {
        match datum {
            Some(d) => unsafe { T::from_datum(*d, false, typoid.value()) },
            None => None,
        }
    }

    pub fn from_tuple(
        oid: &pgx::PgBuiltInOids,
        datum: &Option<pg_sys::Datum>,
        typeoid: &PgOid,
    ) -> Self {
        match oid {
            PgBuiltInOids::JSONBOID => {
                let JsonB(v) = Self::from_datum::<JsonB>(datum, typeoid).unwrap();

                Self::from(&v)
            }
            PgBuiltInOids::TEXTOID => {
                let v = Self::from_datum::<String>(datum, typeoid).unwrap();

                let value = prost_types::Value {
                    kind: Some(Kind::StringValue(v)),
                };

                Self(value)
            }
            PgBuiltInOids::INT8OID | PgBuiltInOids::INT4OID => {
                let v = Self::from_datum::<i32>(datum, typeoid).unwrap();
                let value = prost_types::Value {
                    kind: Some(Kind::NumberValue(v.into())),
                };

                Self(value)
            }
            pg_oid => {
                warning!("Unsupported OID {:?}", pg_oid);
                error!("FIXME");
            }
        }
    }
}
