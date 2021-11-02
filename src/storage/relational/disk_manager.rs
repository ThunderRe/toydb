use crate::error::{Error, Result};
use super::page::PAGE_SIZE;
use std::fs::{File, create_dir_all, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;


struct DiskManager {
    // write to log file
    log_file: Arc<Mutex<File>>,
    log_name: &'static str,
    // write to db file
    db_file: Arc<Mutex<File>>,
    file_name: &'static str,
    num_flushes: u32,
    num_writes: u32,
    flush_log: bool,
    flush_log_join_handle: Option<JoinHandle<()>>,
}

impl DiskManager {

    /// Creates or opens a new disk db, with files in the given directory.
    pub fn new(db_dir: &Path) -> Result<DiskManager> {
        create_dir_all(db_dir)?;

        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_dir.join("toydb.db"))?;
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_dir.join("toydb.log"))?;

        let dish_manager = DiskManager {
            log_file: Arc::new(Mutex::new(log_file)),
            log_name: "toydb.log",
            db_file: Arc::new(Mutex::new(db_file)),
            file_name: "toydb.db",
            num_flushes: 0,
            num_writes: 0,
            flush_log: false,
            flush_log_join_handle: None,
        };

        Ok(dish_manager)
    }

    /// Write the contents of the specified page into disk file
    pub fn write_page(&mut self, page_id: u32, page_data: &[u8]) -> Result<()> {
        let mut db_file = self.db_file.lock()?;
        let mut buf_writer = BufWriter::new(&mut *db_file);
        let offset = page_id * (PAGE_SIZE as u32);
        // set write cursor to offset
        self.num_writes += 1;
        buf_writer.seek(SeekFrom::Start(offset as u64))?;
        buf_writer.write_all(page_data)?;
        // needs to flush to keep disk file in sync
        buf_writer.flush()?;
        Ok(())
    }

    /// Read the contents of the specified page into the given memory area
    pub fn read_page(&mut self, page_id: u32) -> Result<Option<Vec<u8>>> {
        let file_size = self.get_file_size()?;
        let offset = (page_id * (PAGE_SIZE as u32)) as u64;
        if offset > file_size {
            return Err(Error::Past("I/O error reading past end of file".to_string()));
        }

        let mut page_data = vec![0; PAGE_SIZE];
        let mut db_file = self.db_file.lock()?;
        db_file.seek(SeekFrom::Start(offset))?;
        db_file.read_exact(&mut page_data)?;
        Ok(Some(page_data))
    }

    /// get the db file size
    fn get_file_size(&self) -> Result<u64> {
        let file = self.db_file.lock()?;
        let metadata = file.metadata()?;
        let size = metadata.len();
        Ok(size)
    }

}

impl Drop for DiskManager {
    fn drop(&mut self) {
        self.db_file.lock().unwrap().sync_all().ok();
        self.log_file.lock().unwrap().sync_all().ok();
    }
}