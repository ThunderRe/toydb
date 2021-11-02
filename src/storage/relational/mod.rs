mod buffer;
mod clock_replacer;
mod disk_manager;
pub mod page;

pub use disk_manager::DiskManager;

#[cfg(test)]
mod disk_manager_test;
#[cfg(test)]
mod test;
