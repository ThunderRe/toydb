use crate::error::Result;
use std::fs::{File, create_dir_all, OpenOptions};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

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
}
