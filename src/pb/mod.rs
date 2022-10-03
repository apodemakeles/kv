pub mod abi;

use std::vec;

use abi::{command_request::RequestData, *};
use http::StatusCode;

use crate::KvError;

impl CommandRequest {
    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hset(Hset {
                table: table.into(),
                pair: Some(Kvpair::new(key, value)),
            })),
        }
    }
    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hget(Hget {
                table: table.into(),
                key: key.into(),
            })),
        }
    }
    pub fn new_hgetall(table: impl Into<String>) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hgetall(Hgetall {
                table: table.into(),
            })),
        }
    }
    pub fn new_hmget(table: impl Into<String>, keys: Vec<String>) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hmget(Hmget {
                table: table.into(),
                keys: keys,
            })),
        }
    }
    pub fn new_hmset(table: impl Into<String>, pairs: Vec<Kvpair>) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hmset(Hmset {
                table: table.into(),
                pairs: pairs,
            })),
        }
    }
    pub fn new_hdel(table: impl Into<String>, key: impl Into<String>) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hdel(Hdel {
                table: table.into(),
                key: key.into(),
            })),
        }
    }
    pub fn new_hmdel(table: impl Into<String>, keys: Vec<String>) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hmdel(Hmdel {
                table: table.into(),
                keys: keys,
            })),
        }
    }
    pub fn new_hexist(table: impl Into<String>, key: impl Into<String>) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hexist(Hexist {
                table: table.into(),
                key: key.into(),
            })),
        }
    }
    pub fn new_hmexist(table: impl Into<String>, keys: Vec<String>) -> Self {
        CommandRequest {
            request_data: Some(RequestData::Hmexist(Hmexist {
                table: table.into(),
                keys: keys,
            })),
        }
    }
}

impl Kvpair {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }

    pub fn empty_value(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s)),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.into())),
        }
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Self {
            value: Some(value::Value::Integer(i.into())),
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self {
            value: Some(value::Value::Bool(b)),
        }
    }
}

impl From<Value> for CommandResponse {
    fn from(v: Value) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values: vec![v],
            ..Default::default()
        }
    }
}

impl From<Vec<Value>> for CommandResponse {
    fn from(v: Vec<Value>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values: v,
            ..Default::default()
        }
    }
}

impl From<Vec<Kvpair>> for CommandResponse {
    fn from(v: Vec<Kvpair>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            pairs: v,
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut result = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _,
            message: e.to_string(),
            values: vec![],
            pairs: vec![],
        };

        match e {
            KvError::NotFound(_, _) => result.status = StatusCode::NOT_FOUND.as_u16() as _,
            KvError::InvalidCommand(_) => result.status = StatusCode::BAD_REQUEST.as_u16() as _,
            _ => {}
        }

        result
    }
}

impl From<Result<Vec<Value>, KvError>> for CommandResponse {
    fn from(r: Result<Vec<Value>, KvError>) -> Self {
        match r {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl From<Result<Vec<Kvpair>, KvError>> for CommandResponse {
    fn from(r: Result<Vec<Kvpair>, KvError>) -> Self {
        match r {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl From<(String, Value)> for Kvpair {
    fn from(tuple: (String, Value)) -> Self {
        Kvpair::new(tuple.0, tuple.1)
    }
}
