use pgx::*;
use prost_types::value::*;

impl crate::client::pg::ResultSet {
    pub fn into_datums(self) -> Vec<Option<pg_sys::Datum>> {
        self.values
            .into_iter()
            .map(Self::value_into_datum)
            .collect()
    }

    fn value_into_datum(value: prost_types::Value) -> Option<pg_sys::Datum> {
        match value.kind {
            Some(Kind::StringValue(str)) => str.into_datum(),
            Some(Kind::NullValue(_)) => None,
            Some(Kind::BoolValue(boolean)) => boolean.into_datum(),
            Some(Kind::NumberValue(n)) => n.into_datum(),
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
