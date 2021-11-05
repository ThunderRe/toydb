use std::env::consts::OS;
use std::ops::{Deref, DerefMut};
use std::option::Option::Some;
use std::sync::{Arc, Mutex};
use crate::error::{Error, Result};

/// 每个Page的固定大小：4KB
pub const PAGE_SIZE: usize = 4095;

/// LSN's offset in data
pub const OFFSET_LSN: usize = 4;

/// the data page, we have to implement
pub struct Page {
    data: Arc<Mutex<Vec<u8>>>,
    page_id: u32,
    pin_count: u32,
    is_dirty: bool,
}

struct HeaderPage {
    page: Page
}

struct TablePage {
    page: Page
}

impl Page {

    pub fn new() -> Result<Page> {
        Ok(Page {
            data: Arc::new(Mutex::new(vec![0u8; PAGE_SIZE])),
            page_id: 0,
            pin_count: 0,
            is_dirty: false
        })
    }

    pub fn get_data(&self) -> Result<Vec<u8>> {
        let result = self.data.lock()?;
        Ok(result.clone())
    }

    pub fn get_page_id(&self) -> &u32 {
        &self.page_id
    }

    pub fn get_pin_count(&self) -> &u32 {
        &self.pin_count
    }

    pub fn is_dirty(&self) -> &bool {
        &self.is_dirty
    }

    pub fn get_lsn(&self) -> Result<u32> {
        let data = self.get_data()?;
        if data.len() < OFFSET_LSN + 4 {
            return Err(Error::Value("the data size less than OFFSET_LSN + 4".to_string()));
        }
        let n1 = (data.get(OFFSET_LSN).unwrap().clone() as u32) << 24;
        let n2 = (data.get(OFFSET_LSN + 1).unwrap().clone() as u32) << 16;
        let n3 = (data.get(OFFSET_LSN + 2).unwrap().clone() as u32) << 8;
        let n4 = data.get(OFFSET_LSN + 3).unwrap().clone() as u32;
        let lsn = n1 | n2 | n3 | n4;
        Ok(lsn)
    }

    pub fn set_lsn(&mut self, lsn: u32) -> Result<bool> {
        let n4 = (lsn & 0xff) as u8;
        let n3 = (lsn >> 8 & 0xff) as u8;
        let n2 = (lsn >> 16 & 0xff) as u8;
        let n1 = (lsn >> 24 & 0xff) as u8;
        let new_lsn = vec![n1, n2, n3, n4];
        let mut data = self.data.lock()?;
        data.splice(OFFSET_LSN..OFFSET_LSN + 4, new_lsn);
        Ok(true)
    }

}

impl HeaderPage {

}

impl TablePage {

}

/// for the type change, make HeaderPage to Page
/// and not rewrite code
impl Deref for HeaderPage {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl Deref for TablePage {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl DerefMut for HeaderPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

impl DerefMut for TablePage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}