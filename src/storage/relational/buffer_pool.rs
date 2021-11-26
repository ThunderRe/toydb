use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{error::Error, error::Result, storage::relational::page::PAGE_SIZE};

use super::clock_replacer::ClockStatus;
use super::{clock_replacer::ClockReplacer, disk_manager::DiskManager, page::TablePage};

/// BufferPool struct
pub struct BufferPoolManager {
    disk_manager: DiskManager,
    clock_replacer: ClockReplacer,
}

impl BufferPoolManager {
    pub fn open(dir: &Path, cache_capacity: u32) -> Result<BufferPoolManager> {
        let disk_manager = DiskManager::open(dir)?;
        let clock_replacer = ClockReplacer::new(cache_capacity)?;
        Ok(BufferPoolManager { disk_manager, clock_replacer })
    }

    pub fn fetch_page(&mut self, page_id: u32) -> Result<Option<Arc<Mutex<TablePage>>>> {
        if let Some(cache_page) = self.clock_replacer.poll(page_id)? {
            // in cache
            Ok(Some(cache_page))
        } else {
            // read page from disk
            let mut page_data = [0u8; PAGE_SIZE];
            if let Err(err_info) = self.disk_manager.read_page(page_id, &mut page_data) {
                println!(
                    r#"Wranning! can not find page_id:{} in this db!! error info: {}"#,
                    page_id, err_info
                );
                return Ok(None);
            }

            let mut prev_page_id = None;
            if page_id > 1 {
                prev_page_id = Some(page_id - 1);
            }
            let table_page = TablePage::new(page_id, prev_page_id)?;

            if let Some(remove_page) = self.clock_replacer.push(table_page)? {
                let mut page = remove_page.lock().unwrap();
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
    }

    pub fn create_page(&mut self, page_id: u32) -> Result<Option<Arc<Mutex<TablePage>>>> {
        let mut prev_page_id: Option<u32> = None;
        if page_id > 1 {
            prev_page_id = Some(page_id - 1);
        }

        let table_page = TablePage::new(page_id, prev_page_id)?;
        if let Some(remove_page) = self.clock_replacer.push(table_page)? {
            let mut page = remove_page.lock().unwrap();
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
