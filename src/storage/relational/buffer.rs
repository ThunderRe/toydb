use std::path::Path;
use std::sync::{Arc, Mutex};
use crate::error::Result;
use crate::serialization::ToVecAndByVec;
use crate::storage::relational::DiskManager;
use crate::storage::relational::page::{HeaderPage, TablePage};

/// BufferPoolManager reads disk pages to and from its internal buffer pool
struct BufferPoolManager {
    disk_manager: Arc<Mutex<DiskManager>>,
    header_page: HeaderPage
}

impl BufferPoolManager {

    /// create a new BufferPoolManager by db_dir
    pub fn new(db_dir: &Path) -> Result<BufferPoolManager> {
        let mut disk_manager = DiskManager::new(db_dir)?;
        let mut header_page: HeaderPage;
        if let Some(data) = disk_manager.read_page(0)? {
            if let Some(header) = HeaderPage::by_vec(&data) {
                header_page = header;
            } else {
                header_page = HeaderPage::new()?;
                let data = HeaderPage::to_vec(&header_page);
                disk_manager.write_page(0, data);
            }
        } else {
            header_page = HeaderPage::new()?;
            let data = HeaderPage::to_vec(&header_page);
            disk_manager.write_page(0, data);
        }

        let buffer_pool_manager = BufferPoolManager {
            disk_manager: Arc::new(Mutex::new(disk_manager)),
            header_page
        };

        Ok(buffer_pool_manager)
    }

    /// Fetch the requested page from the buffer pool
    pub fn fetch_page(&mut self, page_id: u32) -> Result<Option<TablePage>> {
        if page_id.eq(&0) {
            // can not read header page
            return Ok(None);
        }

        let mut disk_manager = self.disk_manager.lock()?;
        if let Some(page_data) = disk_manager.read_page(page_id)? {
            if let Some(data) = TablePage::by_vec(&page_data) {
                return Ok(Some(data));
            }
        }

        Ok(None)
    }

    /// unpin the target page from the buffer pool
    pub fn unpin_page(&mut self, page_id: u32, is_dirty: bool) -> Result<bool> {
        todo!()
    }

    /// flushes the target page to disk
    pub fn flush_page(&mut self, page_id: u32) -> Result<bool> {
        todo!()
    }

    /// creates a new page in the buffer pool
    pub fn new_page(&mut self, page_id: u32) -> Result<TablePage> {
        // first we should init

       todo!()
    }

    /// deletes a page from the buffer pool
    pub fn delete_page(&mut self, page_id: u32) -> Result<bool> {
        todo!()
    }

    /// flushes all the pages in the buffer pool to disk
    pub fn flush_all_page(&mut self) -> Result<()> {
        todo!()
    }

}