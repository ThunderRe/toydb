use crate::error::Result;

use super::{buffer_pool::BufferPoolManager, tuple::RID};

#[test]
fn test() -> Result<()> {
    // let dir = tempdir::TempDir::new("toydb-bufferpool")?;
    // let mut buffer_pool = BufferPoolManager::open(dir.as_ref(), 10)?;
    // let page_id = 1;
    // let rid = RID::new(page_id, 0);
    // let data = "Hello World!!";

    // if let Some(page) = buffer_pool.create_page(page_id)? {
    //     let mut table_page = page.lock().unwrap();
    //     if let Some(mut tuple) = table_page.get_tuple(&rid)? {
    //         let mut tuple_data = tuple.get_data_mut();
    //         tuple_data.copy_from_slice(data.as_bytes());
    //         table_page.update_tuple(&tuple);
    //     }
    // }

    Ok(())
}
