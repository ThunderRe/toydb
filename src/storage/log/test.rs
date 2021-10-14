use super::{Hybrid, Memory, Range, Scan, Store};
use crate::error::Result;

use std::fmt::Display;
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Log storage backend for testing. Protects an inner Memory backend using a mutex, so it can
/// be cloned and inspected.
#[derive(Clone)]
pub struct Test {
    store: Arc<RwLock<Memory>>,
}

impl Test {
    /// Creates a new Test key-value storage engine.
    pub fn new() -> Self {
        Self { store: Arc::new(RwLock::new(Memory::new())) }
    }
}

impl Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test")
    }
}

impl Store for Test {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.store.write()?.append(entry)
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        self.store.write()?.commit(index)
    }

    fn committed(&self) -> u64 {
        self.store.read().unwrap().committed()
    }

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        self.store.read()?.get(index)
    }

    fn len(&self) -> u64 {
        self.store.read().unwrap().len()
    }

    fn scan(&self, range: Range) -> Scan {
        // Since the mutex guard is scoped to this method, we simply buffer the result.
        Box::new(self.store.read().unwrap().scan(range).collect::<Vec<Result<_>>>().into_iter())
    }

    fn size(&self) -> u64 {
        self.store.read().unwrap().size()
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        self.store.write()?.truncate(index)
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.store.read()?.get_metadata(key)
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.store.write()?.set_metadata(key, value)
    }
}

#[cfg(test)]
impl super::TestSuite<Test> for Test {
    fn setup() -> Result<Self> {
        Ok(Test::new())
    }
}

#[test]
fn tests() -> Result<()> {
    use super::TestSuite;
    Test::test()
}

#[test]
fn test_hybrid_storage() -> Result<()> {
    let path_str = "storage_temp";
    let path = Path::new(path_str);
    let mut engine = Hybrid::new(path, true)?;
    for _i in 0..100 {
        let data_str = "读时模式类似于编程语言中的动态（运行时）类型检查，而写时模式类似于静态（编译时）类型检查。就像静态和动态类型检查的相对优点具有很大的争议性一样【22】，数据库中模式的强制性是一个具有争议的话题，一般来说没有正确或错误的答案。";
        let data_vec = data_str.as_bytes();
        let mut vec: Vec<u8> = Vec::new();
        for data in data_vec {
            vec.push(*data);
        }
        let index = engine.append(vec)?;
        engine.commit(index)?;
    }
    Ok(())
}
