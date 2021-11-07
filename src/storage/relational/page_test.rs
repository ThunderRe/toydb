use crate::error::Result;
use crate::storage::relational::page::Page;


#[test]
fn test_page_lsn() -> Result<()> {
    let mut page = Page::new()?;
    let lsn = 222311u32;
    let set_result = page.set_lsn(lsn)?;
    assert_eq!(set_result, true);

    let get_lsn = page.get_lsn()?;
    assert_eq!(get_lsn, 222311u32);

    Ok(())
}

#[test]
fn test_pagt_data() -> Result<()> {
    let mut page = Page::new()?;
    let push_data = vec![1u8, 2, 3, 4, 5, 6];
    let push_result1 = page.push_data_with_offset(0, push_data)?;
    assert_eq!(push_result1, true);

    let push_data = vec![9u8, 23, 32, 12, 33];
    let push_result2 = page.push_data_with_offset(7, push_data)?;
    assert_eq!(push_result2, true);

    Ok(())
}