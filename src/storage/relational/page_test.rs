use crate::error::Result;
use crate::storage::relational::page::{HeaderPage, PAGE_SIZE};

struct Record {
    record_name: &'static str,
    root_id: u32,
}

#[test]
fn test_header_page() -> Result<()> {
    let records = [
        Record { record_name: "a", root_id: 1 },
        Record { record_name: "b", root_id: 2 },
        Record { record_name: "c", root_id: 3 },
    ];
    let header_data = [0u8; PAGE_SIZE];
    let mut header_page = HeaderPage::new(header_data)?;
    let record_size = records.len() as u32;

    for record in &records {
        header_page.insert_record(record.record_name, record.root_id)?;
    }
    assert_eq!(record_size, header_page.get_record_count()?);

    for record in &records {
        if let Some(root_id) = header_page.get_root_id(record.record_name)? {
            assert_eq!(root_id, record.root_id);
        }
    }

    Ok(())
}
