mod buffer;
mod clock_replacer;
mod disk_manager;

pub use disk_manager::DiskManager;

#[cfg(test)]
mod disk_manager_test;
mod page;
#[cfg(test)]
mod page_test;
mod rid;
#[cfg(test)]
mod test;
mod tuple;
mod buffer_pool;
