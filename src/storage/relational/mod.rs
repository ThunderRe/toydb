mod buffer;
mod clock_replacer;
mod disk_manager;

pub use disk_manager::DiskManager;

mod page;
#[cfg(test)]
mod page_test;
mod tuple;
mod buffer_pool;
