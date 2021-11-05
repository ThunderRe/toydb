use crate::error::{Error, Result};
use crate::storage::relational::{DiskManager};
use std::option::Option::Some;
use crate::storage::relational::page::PAGE_SIZE;

#[test]
fn test_page() -> Result<()> {
    let dir = tempdir::TempDir::new("toydb_disk_manager")?;
    let mut disk_manager = DiskManager::new(dir.as_ref())?;
    let pages = vec![
        vec![1u8, 2, 3, 4],
        vec![1u8, 5, 6, 7],
        vec![1u8, 2, 3, 8],
        vec![1u8, 2, 3, 9],
        vec![1u8, 2, 3, 10],
    ];
    let mut page_id = 1;
    for page in pages {
        let mut page_data = Vec::new();
        for p in page {
            page_data.push(p);
        }

        while page_data.len() < PAGE_SIZE {
            page_data.push(0 as u8);
        }

        disk_manager.write_page(page_id, page_data)?;
        page_id += 1;
    }

    let check_pages = vec![
        vec![1u8, 2, 3, 4],
        vec![1u8, 5, 6, 7],
        vec![1u8, 2, 3, 8],
        vec![1u8, 2, 3, 9],
        vec![1u8, 2, 3, 10],
    ];
    let mut page_id = 1;
    for page in check_pages {
        let read_result = disk_manager.read_page(page_id)?;
        assert_eq!(read_result.is_some(), true);
        let mut result = read_result.ok_or(Error::Abort)?;
        assert_eq!(result.len(), PAGE_SIZE);

        result.split_off(4);
        assert_eq!(page, result);

        page_id += 1;
    }

    assert_eq!(&(5 as u32), disk_manager.get_num_writes());
    Ok(())
}

#[test]
fn test_log() -> Result<()> {
    let dir = tempdir::TempDir::new("toydb_disk_manager")?;
    let mut disk_manager = DiskManager::new(dir.as_ref())?;
    let logs = vec![
        vec![1u8, 2, 3, 4],
        vec![1u8, 5, 6, 7],
        vec![1u8, 2, 3, 8],
        vec![1u8, 2, 3, 9],
        vec![1u8, 2, 3, 10],
    ];
    for log in logs {
        disk_manager.write_log(log)?;
    }

    let check_logs = vec![
        vec![1u8, 2, 3, 4],
        vec![1u8, 5, 6, 7],
        vec![1u8, 2, 3, 8],
        vec![1u8, 2, 3, 9],
        vec![1u8, 2, 3, 10],
    ];
    let mut log_index = 0;
    for check_log in check_logs {
        let offset = log_index * 4;
        let log_result = disk_manager.read_log(4, offset)?;
        assert_eq!(log_result.is_some(), true);
        let mut result = log_result.ok_or(Error::Abort)?;
        assert_eq!(check_log, result);
        log_index += 1;
    }
    assert_eq!(&(5 as u32), disk_manager.get_num_flushes());
    Ok(())
}
