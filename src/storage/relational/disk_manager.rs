use crate::error::{Error, Result};
use crate::storage::relational::page::PAGE_SIZE;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

pub struct DiskManager {
    // db file
    db_file: Mutex<File>,
}

impl DiskManager {
    /// Creates or opens a new disk db, with files in the given directory.
    pub fn open(db_dir: &Path) -> Result<DiskManager> {
        create_dir_all(db_dir)?;
        let db_file =
            OpenOptions::new().read(true).write(true).create(true).open(db_dir.join("toydb.db"))?;
        Ok(DiskManager { db_file: Mutex::new(db_file) })
    }

    /// Write the contents of the specified page into disk file
    pub fn write_page(&mut self, page_id: u32, page_data: &[u8]) -> Result<()> {
        if page_data.len() != PAGE_SIZE {
            return Err(Error::Value("page size was lass than PAGE_SIZE".to_string()));
        }
        let mut db_file = self.db_file.lock()?;
        let mut buf_writer = BufWriter::new(&mut *db_file);
        let offset = page_id * (PAGE_SIZE as u32);
        // set write cursor to offset
        buf_writer.seek(SeekFrom::Start(offset as u64))?;
        buf_writer.write_all(page_data)?;
        // needs to flush to keep disk file in sync
        buf_writer.flush()?;
        drop(buf_writer);

        db_file.sync_data()?;
        Ok(())
    }

    /// Read the contents of the specified page into the given memory area
    /// return. if
    pub fn read_page(&mut self, page_id: u32, buf: &mut [u8]) -> Result<usize> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        if !self.have_page(page_id)? {
            return Ok(0);
        }

        let mut db_file = self.db_file.lock()?;
        db_file.seek(SeekFrom::Start(offset))?;
        db_file.read_exact(buf)?;
        Ok(buf.len())
    }

    /// check this db have page by page id
    fn have_page(&mut self, page_id: u32) -> Result<bool> {
        let file_size = self.get_db_size()?;
        let offset = page_id as u64 * PAGE_SIZE as u64;
        if offset + PAGE_SIZE as u64 > file_size {
            return Ok(false);
        }
        Ok(true)
    }

    /// get the db file size
    fn get_db_size(&self) -> Result<u64> {
        let file = self.db_file.lock()?;
        let metadata = file.metadata()?;
        let size = metadata.len();
        Ok(size)
    }
}

impl Drop for DiskManager {
    fn drop(&mut self) {
        self.db_file.lock().unwrap().sync_all().ok();
    }
}
