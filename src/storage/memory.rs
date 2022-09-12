use crate::{KvError, Kvpair, Value};
use dashmap::{iter::Iter, mapref::one::Ref, DashMap};

use super::Storage;

#[derive(Clone, Debug, Default)]
pub struct MemTable {
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_create_table(&self, name: &str) -> Ref<String, DashMap<String, Value>> {
        match self.tables.get(name) {
            Some(table) => table,
            None => {
                let entry = self.tables.entry(name.into()).or_default();
                entry.downgrade()
            }
        }
    }
}

impl Storage for MemTable {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);

        Ok(table.get(key).map(|r| r.value().clone()))
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);

        Ok(table.insert(key, value))
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let table = self.get_or_create_table(table);

        Ok(table.contains_key(key))
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);

        Ok(table.remove(key).map(|r| r.1))
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let table = self.get_or_create_table(table);

        Ok(table
            .iter()
            .map(|entry| Kvpair::new(entry.key(), entry.value().clone()))
            .collect::<Vec<_>>())
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let table = self.get_or_create_table(table);
        let iter = MemStoreIter {
            inner: table.value().iter(),
        };

        Ok(Box::new(iter))
    }
}

// type ValueMap = DashMap<String, Value, RandomState>;
struct MemStoreIter<'a> {
    inner: Iter<'a, String, Value>,
}

impl<'a> Iterator for MemStoreIter<'a> {
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        let x = self.inner.next();
        if let Some(v) = x {
            return Some(Kvpair::new(v.key(), v.value().clone()));
        }

        None
    }
}
