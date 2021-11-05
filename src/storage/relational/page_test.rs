use crate::error::Result;
use crate::storage::relational::page::Page;


#[test]
fn test_page() -> Result<()> {
    let mut page = Page::new()?;
    let lsn = 222311u32;
    let set_result = page.set_lsn(lsn)?;
    assert_eq!(set_result, true);

    let get_lsn = page.get_lsn()?;
    assert_eq!(get_lsn, 222311u32);

    Ok(())
}