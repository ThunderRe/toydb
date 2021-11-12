use crate::error::Result;
use crate::storage::relational::page::HeaderPage;

#[test]
fn test_header_page() -> Result<()> {
    let mut header_page = HeaderPage::new()?;
    header_page.init()?;

    let names = vec!["a", "b", "c"];
    let root_ids = vec![1u32, 2, 3];

    for index in 0..names.len() {
        let name = names[index];
        let root_id = root_ids[index];
        header_page.insert_record(name, root_id)?;
    }
    // check record count
    assert_eq!(header_page.get_record_count()?, 3);

    for index in 0..names.len() {
        let name = names[index];
        let root_id = root_ids[index];
        let find_root_id = header_page.get_root_id(name)?;
        if let Some(root_id_result) = find_root_id {
            assert_eq!(root_id_result, root_id);
        } else {
            assert!(false);
        }
    }

    header_page.update_record("a", 4 as u32)?;
    let update_root_id = header_page.get_root_id("a")?;
    if let Some(root_id_result) = update_root_id {
        assert_eq!(root_id_result, 4);
    } else {
        assert!(false);
    }

    header_page.delete_record("a")?;
    assert_eq!(header_page.get_record_count()?, 2);

    Ok(())
}
