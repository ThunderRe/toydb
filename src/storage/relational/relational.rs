use crate::storage::kv;
use std::fmt::Display;

use super::buffer_pool::BufferPoolManager;

pub struct Relational {
    buffer_pool: BufferPoolManager,
}

impl Display for Relational {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "relational")
    }
}

impl Relational {
    pub fn new() -> Self {
        todo!()
    }
}

impl kv::Store for Relational {
    fn delete(&mut self, key: &[u8]) -> crate::error::Result<()> {
        todo!()
    }

    fn flush(&mut self) -> crate::error::Result<()> {
        todo!()
    }

    fn get(&self, key: &[u8]) -> crate::error::Result<Option<Vec<u8>>> {
        todo!()
    }

    fn scan(&self, range: kv::Range) -> kv::Scan {
        todo!()
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> crate::error::Result<()> {
        todo!()
    }
}
