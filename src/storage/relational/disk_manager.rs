use crate::error::{Error, Result};
use crate::storage::relational::page::PAGE_SIZE;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct DiskManager {
    // write to log file
    log_file: Arc<Mutex<File>>,
    log_name: &'static str,
    // write to db file
    db_file: Arc<Mutex<File>>,
    file_name: &'static str,
    num_flushes: u32,
    num_writes: u32,
}

impl DiskManager {
    /// Creates or opens a new disk db, with files in the given directory.
    pub fn new(db_dir: &Path) -> Result<DiskManager> {
        create_dir_all(db_dir)?;
        let db_file =
            OpenOptions::new().read(true).write(true).create(true).open(db_dir.join("toydb.db"))?;
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
        };

        Ok(dish_manager)
    }

    /// Write the contents of the specified page into disk file
    pub fn write_page(&mut self, page_id: u32, page_data: Vec<u8>) -> Result<()> {
        if page_data.len() != PAGE_SIZE {
            return Err(Error::Value("page size was lass than PAGE_SIZE".to_string()));
        }
        let mut db_file = self.db_file.lock()?;
        let mut buf_writer = BufWriter::new(&mut *db_file);
        let offset = page_id * (PAGE_SIZE as u32);
        // set write cursor to offset
        buf_writer.seek(SeekFrom::Start(offset as u64))?;
        buf_writer.write_all(&page_data)?;
        // needs to flush to keep disk file in sync
        buf_writer.flush()?;
        drop(buf_writer);

        self.num_writes += 1;

        db_file.sync_data()?;
        Ok(())
    }

    /// Read the contents of the specified page into the given memory area
    pub fn read_page(&mut self, page_id: u32) -> Result<Option<Vec<u8>>> {
        let file_size = self.get_db_size()?;
        let offset = (page_id * (PAGE_SIZE as u32)) as u64;
        if offset > file_size {
            return Ok(None);
        }

        let mut page_data = vec![0; PAGE_SIZE];
        let mut db_file = self.db_file.lock()?;
        db_file.seek(SeekFrom::Start(offset))?;
        db_file.read_exact(&mut page_data)?;
        Ok(Some(page_data))
    }

    /// Write the contents of the log into disk file
    /// Only return when sync is done, and only perform sequence write
    pub fn write_log(&mut self, log_data: Vec<u8>) -> Result<()> {
        let mut log_file = self.log_file.lock()?;
        let mut buf_writer = BufWriter::new(&mut *log_file);
        buf_writer.write_all(&log_data)?;
        buf_writer.flush()?;
        drop(buf_writer);

        self.num_flushes += 1;

        // check the file was flush
        log_file.sync_data()?;
        Ok(())
    }

    /// Read the contents of the log into the given memory area
    //  Always read from the beginning and perform sequence read
    //  @return: None means already reach the end
    pub fn read_log(&mut self, size: u64, offset: u64) -> Result<Option<Vec<u8>>> {
        if offset > self.get_log_size()? || size <= 0 {
            return Ok(None);
        }
        let mut log_data = vec![0; size as usize];
        let mut log_file = self.log_file.lock()?;
        log_file.seek(SeekFrom::Start(offset))?;
        log_file.read_exact(&mut log_data)?;
        Ok(Some(log_data))
    }

    pub fn get_num_flushes(&self) -> &u32 {
        &self.num_flushes
    }

    pub fn get_num_writes(&self) -> &u32 {
        &self.num_writes
    }

    /// get the db file size
    fn get_db_size(&self) -> Result<u64> {
        let file = self.db_file.lock()?;
        let metadata = file.metadata()?;
        let size = metadata.len();
        Ok(size)
    }

    /// get the log file size
    fn get_log_size(&self) -> Result<u64> {
        let file = self.log_file.lock()?;
        let metadata = file.metadata()?;
        Ok(metadata.len())
    }
}

impl Drop for DiskManager {
    fn drop(&mut self) {
        self.db_file.lock().unwrap().sync_all().ok();
        self.log_file.lock().unwrap().sync_all().ok();
    }
}
