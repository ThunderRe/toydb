use std::path::Path;

use crate::error::Result;

use super::{
    clock_replacer::{ClockReplacer, ClockStatus},
    disk_manager::DiskManager,
    page::TablePage,
};

/// BufferPool struct
pub struct BufferPoolManager {
    disk_manager: DiskManager,
    clock_replacer: ClockReplacer,
}

impl BufferPoolManager {
    pub fn open(dir: &Path, page_count: u32) -> Result<BufferPoolManager> {
        let disk_manager = DiskManager::open(dir)?;

        todo!()
    }

    pub fn fetch_page(&mut self, page_id: u32) -> Result<Option<TablePage>> {
        todo!()
    }

    pub fn create_page(&mut self, page_id: u32) -> Result<Option<TablePage>> {
        todo!()
    }

    pub fn delete_page(&mut self, page_id: u32) -> Result<bool> {
        todo!()
    }

    pub fn flush_page(&mut self, page_id: u32) -> Result<()> {
        todo!()
    }

    pub fn flush_all(&mut self) -> Result<()> {
        todo!()
    }
}
