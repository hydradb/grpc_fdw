use pgx::*;
use prost_types::value::*;
use serde_json::map::Map;
use serde_json::Value as JsonValue;

impl crate::client::pg::ResultSet {
    pub fn into_datums(self) -> Vec<Option<pg_sys::Datum>> {
        self.values
            .into_iter()
            .map(Self::value_into_datum)
            .collect()
    }

    fn into_json(value: &prost_types::Value) -> serde_json::Value {
        match &value.kind {
            Some(Kind::NullValue(_)) => JsonValue::Null,
            Some(Kind::NumberValue(n)) => {
                JsonValue::Number(serde_json::Number::from_f64(n.to_owned()).expect(""))
            }
            Some(Kind::StringValue(string)) => JsonValue::String(String::from(string)),
            Some(Kind::BoolValue(boolean)) => JsonValue::Bool(boolean.to_owned()),
            Some(Kind::StructValue(sv)) => {
                let mut obj = Map::new();
                for (key, value) in sv.fields.iter() {
                    obj.insert(String::from(key), Self::into_json(&value));
                }

                JsonValue::Object(obj)
            }
            Some(Kind::ListValue(list)) => {
                JsonValue::Array(list.values.iter().map(|v| Self::into_json(&v)).collect())
            }
            None => JsonValue::String(String::from("")),
        }
    }

    fn value_into_datum(value: prost_types::Value) -> Option<pg_sys::Datum> {
        match value.kind {
            Some(Kind::StringValue(str)) => str.into_datum(),
            Some(Kind::NullValue(_)) => None,
            Some(Kind::BoolValue(boolean)) => boolean.into_datum(),
            Some(Kind::NumberValue(n)) => n.into_datum(),
            Some(Kind::StructValue(sv)) => {
                let mut obj = Map::new();
                for (key, value) in sv.fields.iter() {
                    obj.insert(String::from(key), Self::into_json(&value));
                }

                pgx::JsonB(JsonValue::Object(obj)).into_datum()
            }
            //Some(Kind::ListValue(list)) => {
            //    Value::Array(list.values.iter().map(|v| as_json(&v.kind)).collect())
            //}
            None => None,
            _ => None,
        }
    }
}
