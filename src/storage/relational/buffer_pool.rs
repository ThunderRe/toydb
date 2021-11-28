use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{error::Error, error::Result, storage::relational::page::PAGE_SIZE};
use crate::storage::relational::page::HeaderPage;

use super::clock_replacer::ClockStatus;
use super::{clock_replacer::ClockReplacer, disk_manager::DiskManager, page::TablePage};

/// BufferPool struct
pub struct BufferPoolManager {
    header_page: HeaderPage,
    disk_manager: DiskManager,
    clock_replacer: ClockReplacer,
}

impl BufferPoolManager {
    pub fn open(dir: &Path, cache_capacity: u32) -> Result<BufferPoolManager> {
        let clock_replacer = ClockReplacer::new(cache_capacity)?;

        let mut disk_manager = DiskManager::open(dir)?;
        let mut header_page_data = [0u8; PAGE_SIZE];
        disk_manager.read_page(0, &mut header_page_data)?;
        let header_page = HeaderPage::new(header_page_data)?;

        Ok(BufferPoolManager { disk_manager, clock_replacer, header_page })
    }

    /// fetch a page from buffer pool
    pub fn fetch_page(&mut self, page_id: u32) -> Result<Option<Arc<Mutex<TablePage>>>> {
        if let Some(cache_page) = self.clock_replacer.poll(page_id)? {
            // in cache
            Ok(Some(cache_page))
        } else {
            // read page from disk
            let mut page_data = self.read_disk_page(page_id)?;

            let mut prev_page_id = None;
            if page_id > 1 {
                prev_page_id = Some(page_id - 1);
            }

            let table_page = TablePage::new(page_id, prev_page_id, page_data)?;

            self.push_cache(table_page)
        }
    }

    pub fn create_page(&mut self, page_id: u32) -> Result<Option<Arc<Mutex<TablePage>>>> {
        if self.disk_manager.have_page(page_id)? {
            return Ok(None);
        }
        let mut prev_page_id: Option<u32> = None;
        if page_id > 1 {
            prev_page_id = Some(page_id - 1);
        }
        let mut page_data = self.read_disk_page(page_id)?;

        let table_page = TablePage::new(page_id, prev_page_id, page_data)?;
        self.push_cache(table_page)
    }

    /// delete page by page_id
    pub fn delete_page(&mut self, page_id: u32) -> Result<bool> {
        if let Some(page) = self.fetch_page(page_id)? {
            let mut deleted_page = page.lock().unwrap();
            deleted_page.get_status_mut().set_deleted(true);
            deleted_page.get_status_mut().edited();

            return Ok(true);
        }
        Ok(false)
    }

    /// flush edit data in to disk
    pub fn flush_page(&mut self, page_id: u32) -> Result<()> {
        if let Some(page) = self.clock_replacer.poll(page_id)? {
            let mut table_page = page.lock().unwrap();
            if table_page.get_status_mut().is_edited() {
                let page_data = table_page.get_data();
                self.disk_manager.write_page(page_id, page_data)?;
            }
        }

        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        for (page_id, data) in self.clock_replacer.get_need_flush() {
            self.disk_manager.write_page(page_id, data)?;
        }
        Ok(())
    }

    /// when buffer pool create or read a page, it should be push to cache.
    /// then, the cache (clock_replacer) will return a ref
    fn push_cache(&mut self, table_page: TablePage) -> Result<Option<Arc<Mutex<TablePage>>>> {
        let page_id = table_page.get_page_id().clone();
        if let Some(remove_page) = self.clock_replacer.push(table_page)? {
            let mut page = remove_page.lock().unwrap();
            page.get_status_mut().set_removed(true);

            if page.get_status_mut().is_edited() {
                let page_data = page.get_data();
                self.disk_manager.write_page(*page.get_page_id(), page_data)?;
            }
        }

        if let Some(page) = self.clock_replacer.poll(page_id)? {
            Ok(Some(page))
        } else {
            Err(Error::Value(String::from(
                r#"have a bug in clock replacer! when dbms push one page, it's can not find it!!!"#,
            )))
        }
    }

    /// read page data from disk by page_id
    fn read_disk_page(&mut self, page_id: u32) -> Result<[u8; PAGE_SIZE]> {
        let mut data = [0u8; PAGE_SIZE];
        self.disk_manager.read_page(page_id, &mut data)?;
        Ok(data)
    }
}
