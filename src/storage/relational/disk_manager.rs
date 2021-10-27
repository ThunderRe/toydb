use std::fs::File;
use std::sync::Mutex;
use std::thread;
use tokio::task::JoinHandle;

struct DishManager {
    // write to log file
    log_file: File,
    log_name: str,
    // write to db file
    // db_file: File,
    db_io_latch: Mutex<File>,
    file_name: str,
    num_flushes: u32,
    num_writes: u32,
    flush_log: bool,
    flush_log_join_handle: JoinHandle<()>,
}

impl DishManager {
    // pub fn new()
}
