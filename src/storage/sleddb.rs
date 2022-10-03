use sled::{Db, IVec};
use std::{convert::TryInto, path::Path, str};

use crate::{KvError, Kvpair, Storage, StorageIter, Value};

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    // 在 sleddb 里，因为它可以 scan_prefix，我们用 prefix
    // 来模拟一个 table。当然，还可以用其它方案。
    fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }

    // 遍历 table 的 key 时，我们直接把 prefix: 当成 table
    fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}

impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, key);
        let result = self.0.get(name.as_bytes())?.map(|v| v.as_ref().try_into());

        result.transpose()
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, &key);
        let data: Vec<u8> = value.try_into()?;
        let result = self.0.insert(name, data)?.map(|v| v.as_ref().try_into());

        result.transpose()
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let name = SledDb::get_full_key(table, &key);

        Ok(self.0.contains_key(name)?)
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, &key);
        let result = self.0.remove(name)?.map(|v| v.as_ref().try_into());

        result.transpose()
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let result = self.0.scan_prefix(prefix).map(|v| v.into()).collect();

        Ok(result)
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let prefix = SledDb::get_table_prefix(table);
        let iter = StorageIter::new(self.0.scan_prefix(prefix));

        Ok(Box::new(iter))
    }
}

impl From<Result<(IVec, IVec), sled::Error>> for Kvpair {
    fn from(v: Result<(IVec, IVec), sled::Error>) -> Self {
        match v {
            Ok((k, v)) => match v.as_ref().try_into() {
                Ok(v) => Kvpair::new(ivec_to_key(k.as_ref()), v),
                Err(_) => Kvpair::default(),
            },
            _ => Kvpair::default(),
        }
    }
}

fn ivec_to_key(ivec: &[u8]) -> &str {
    let s = str::from_utf8(ivec).unwrap();
    let mut iter = s.split(":");
    iter.next();
    iter.next().unwrap()
}
