use super::{page::TablePage, tuple::Tuple, tuple::RID};
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

struct TablePageTest {
    tuple_data: String,
    check_data: String,
}

#[test]
fn test_table_page() -> Result<()> {
    let page_data = [0u8; PAGE_SIZE];
    let mut table_page = TablePage::new(1, None, page_data)?;

    let tests = [TablePageTest {
        tuple_data: String::from("hello world!!"),
        check_data: String::from("hello world!!")
    },
    TablePageTest {
        tuple_data: String::from("aosidjoiqweoqiwjeqowijdpowaqpdojqwfihnliuhfaieuhr398rhqwejpqwokepqowkep12oie-0k!(!I)(@102931029413740913"),
        check_data: String::from("aosidjoiqweoqiwjeqowijdpowaqpdojqwfihnliuhfaieuhr398rhqwejpqwokepqowkep12oie-0k!(!I)(@102931029413740913"),
    }
    ];

    let mut slot_num = 0;
    for test in tests {
        let tuple_data = test.tuple_data.as_bytes();
        let check_data = test.check_data.as_bytes();

        let mut tuple = Tuple::from_data(Vec::from(tuple_data));
        // insert tuple
        assert!(table_page.insert_tuple(&mut tuple)?);

        let find_rid = RID::new(1, slot_num);
        if let Some(get_tuple) = table_page.get_tuple(&find_rid)? {
            let tuple_data = get_tuple.get_data();
            assert_eq!(tuple_data, check_data);
        } else {
            assert!(false);
        }

        slot_num += 1;
    }

    Ok(())
}
