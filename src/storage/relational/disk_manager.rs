use crate::error::{Error, Result};
use crate::storage::relational::page::PAGE_SIZE;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct DiskManager {
    // write to log file
    log_file: Arc<Mutex<File>>,
    // write to db file
    db_file: Arc<Mutex<File>>,
    num_flushes: u32,
    num_writes: u32,
}

impl DiskManager {
    /// Creates or opens a new disk db, with files in the given directory.
    pub fn open(db_dir: &Path) -> Result<DiskManager> {
        create_dir_all(db_dir)?;
        let db_file =
            OpenOptions::new().read(true).write(true).create(true).open(db_dir.join("toydb.db"))?;
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_dir.join("toydb.log"))?;

        let disk_manager = DiskManager {
            log_file: Arc::new(Mutex::new(log_file)),
            db_file: Arc::new(Mutex::new(db_file)),
            num_flushes: 0,
            num_writes: 0,
        };

        Ok(disk_manager)
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

        self.num_writes += 1;

        db_file.sync_data()?;
        Ok(())
    }

    /// Read the contents of the specified page into the given memory area
    pub fn read_page(&mut self, page_id: u32, buf: &mut [u8]) -> Result<()> {
        let offset = page_id as u64 * PAGE_SIZE as u64;
        if !self.have_page(page_id)? {
            return Err(Error::Value("this db can't find page_id".to_string()));
        }

        let mut db_file = self.db_file.lock()?;
        db_file.seek(SeekFrom::Start(offset))?;
        db_file.read_exact(buf)?;
        Ok(())
    }

    /// check this db have page by page id
    pub fn have_page(&mut self, page_id: u32) -> Result<bool> {
        let file_size = self.get_db_size()?;
        let offset = page_id as u64 * PAGE_SIZE as u64;
        if offset + PAGE_SIZE as u64 > file_size {
            return Ok(false);
        }

        Ok(true)
    }

    /// Write the contents of the log into disk file
    /// Only return when sync is done, and only perform sequence write
    pub fn write_log(&mut self, log_data: &[u8]) -> Result<()> {
        let mut log_file = self.log_file.lock()?;
        let mut buf_writer = BufWriter::new(&mut *log_file);
        buf_writer.write_all(log_data)?;
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
    pub fn read_log(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        if offset > self.get_log_size()? || buf.is_empty() {
            return Err(Error::Value(String::from("offset is out of range or buf size is zero.")));
        }
        let mut log_file = self.log_file.lock()?;
        log_file.seek(SeekFrom::Start(offset))?;
        log_file.read_exact(buf)?;
        Ok(())
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
