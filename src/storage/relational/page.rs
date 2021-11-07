use std::ops::{Deref, DerefMut};
use std::option::Option::Some;
use std::sync::{Arc, Mutex};
use crate::error::{Error, Result};
use crate::storage::relational::lock_manager::LockManager;
use crate::storage::relational::log_manager::LogManager;
use crate::storage::relational::rid::RID;
use crate::storage::relational::transaction::Transaction;
use crate::storage::relational::tuple::Tuple;

/// 每个Page的固定大小：4KB
pub const PAGE_SIZE: usize = 4095;

/// LSN's offset in data
pub const OFFSET_LSN: usize = 4;

const DELETE_MASK: u32 = 2147483648;

/// the data page, we have to implement
pub struct Page {
    data: Arc<Mutex<Vec<u8>>>,
    page_id: u32,
    pin_count: u32,
    is_dirty: bool,
}

/// Database use the first page (page_id = 0) as header page to store metadata,
/// in our case, we will contain information about table/index name (length less than
/// 32 bytes) and their corresponding root_id
///
/// Format (size in byte):
///
///  /---------------------------------------------------------------<br>
/// | RecordCount (4) | Entry_1 name (32) | Entry_1 root_id (4) | ... |
///  /---------------------------------------------------------------
///
struct HeaderPage {
    page: Page
}

/// Slotted page format:
///
///  -------------------------------------------------------
/// | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
///                               ^
///                               free space pointer
///
/// Header format (size in bytes):
///
///  /--------------------------------------------------------------------------
/// | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
///  /--------------------------------------------------------------------------
///
///  /--------------------------------------------------------------
/// | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
///  /--------------------------------------------------------------
struct TablePage {
    page: Page
}

impl Page {

    pub fn new() -> Result<Page> {
        Ok(Page {
            data: Arc::new(Mutex::new(vec![0u8; PAGE_SIZE])),
            page_id: 0,
            pin_count: 0,
            is_dirty: false,
        })
    }

    pub fn get_data(&self) -> Result<Vec<u8>> {
        let result = self.data.lock()?;
        Ok(result.clone())
    }

    /// push data with offset
    pub fn push_data_with_offset(&mut self, offset: u32, push_data: Vec<u8>) -> Result<bool> {
        if offset < 0 || (offset + push_data.len() as u32) as usize > PAGE_SIZE {
            return Err(Error::Value(String::from("the offset was out of range")));
        }
        let mut data = self.data.lock()?;
        let start = offset as usize;
        let end = start + push_data.len();
        data.splice(start..end, push_data);
        Ok(true)
    }

    pub fn get_page_id(&self) -> &u32 {
        &self.page_id
    }

    pub fn get_pin_count(&self) -> &u32 {
        &self.pin_count
    }

    pub fn is_dirty(&self) -> &bool {
        &self.is_dirty
    }

    pub fn get_lsn(&self) -> Result<u32> {
        let data = self.get_data()?;
        if data.len() < OFFSET_LSN + 4 {
            return Err(Error::Value("the data size less than OFFSET_LSN + 4".to_string()));
        }
        vec_to_u32(&data, OFFSET_LSN)
    }

    pub fn set_lsn(&mut self, lsn: u32) -> Result<bool> {
        let new_lsn = u32_to_vec(lsn)?;
        self.set_data(new_lsn, OFFSET_LSN, 4)?;
        Ok(true)
    }

    fn set_data(&mut self, data: Vec<u8>, offset: usize, len: usize) -> Result<()> {
        if len < 0 || len > data.len() {
            return Err(Error::Value(String::from("the len is out of range")));
        }

        let mut self_data = self.data.lock()?;
        if offset < 0 || len + offset > self_data.len() {
            return Err(Error::Value(String::from("the offset/len is out of range")));
        }
        self_data.splice(offset..offset + len, data);
        Ok(())
    }

}


impl HeaderPage {

    pub fn init(&mut self) -> Result<()> {
        self.set_record_count(0)
    }

    /// record related
    pub fn insert_record(&mut self, name: &str, root_id: u32) -> Result<bool> {
        todo!()
    }

    pub fn delete_record(&mut self, name: &str) -> Result<bool> {
        todo!()
    }

    pub fn update_record(&mut self, name: &str, root_id: u32) -> Result<bool> {
        todo!()
    }

    /// return root if success
    pub fn get_root_id(&self) -> Result<Option<u32>> {
        todo!()
    }

    pub fn get_record_count(&self) -> Result<u32> {
        todo!()
    }

    /// helper function
    fn set_record_count(&mut self, record_count: u32) -> Result<()> {
        todo!()
    }

    fn find_record(&self, name: &str) -> Result<Option<u32>> {
        todo!()
    }

}

impl TablePage {

    const SIZE_TABLE_PAGE_HEADER:usize = 24;
    const SIZE_TUPLE:usize = 8;
    const OFFSET_PREV_PAGE_ID:usize = 8;
    const OFFSET_NEXT_PAGE_ID:usize = 12;
    const OFFSET_FREE_SPACE:usize = 16;
    const OFFSET_TUPLE_COUNT:usize = 20;
    const OFFSET_TUPLE_OFFSET:usize = 24;   /// naming things is hard
    const OFFSET_TUPLE_SIZE:usize = 28;



    /// init the tablePage header.
    /// page_id: the page ID of this table page
    /// page_size: the size of this table page
    /// prev_page_id: the previous table page ID
    /// log_manager: the log manager in use
    /// txn: the transaction that this page is created in
    pub fn init(page_id: u32, page_size: u32, prev_page_id: u32, log_manager: LogManager, txn: Transaction) -> Result<TablePage> {
        todo!()
    }

    /// return the page ID of this table page
    pub fn get_table_page_id(&self) -> Result<u32> {
        let data = self.get_data()?;
        vec_to_u32(&data, 0)
    }

    /// return the page ID of the previous table page
    pub fn get_prev_page_id(&self) -> Result<u32> {
        let data = self.get_data()?;
        vec_to_u32(&data, TablePage::OFFSET_PREV_PAGE_ID)
    }

    /// return the page ID of the next table page
    pub fn get_next_page_id(&self) -> Result<u32> {
        let data = self.get_data()?;
        vec_to_u32(&data, TablePage::OFFSET_NEXT_PAGE_ID)
    }

    /// set the page id of the previous page in the table
    pub fn set_prev_page_id(&mut self, prev_page_id: u32) -> Result<bool> {
        let new_vec = u32_to_vec(prev_page_id)?;
        self.set_data(new_vec, TablePage::OFFSET_PREV_PAGE_ID, 4)?;
        Ok(true)
    }

    /// set the page id of the next page in the table
    pub fn set_next_page_id(&mut self, next_page_id: u32) -> Result<bool> {
        let new_vec = u32_to_vec(next_page_id)?;
        self.set_data(new_vec, TablePage::OFFSET_NEXT_PAGE_ID, 4)?;
        Ok(true)
    }

    /// insert a tuple into the page
    /// tuple: tuple to insert
    /// txn: transaction performing the insert
    /// lock_manager: the lock manager
    /// log_manager: the log manager
    ///
    /// RID: rid of the inserted tuple
    pub fn insert_tuple(&mut self, tuple: &Tuple, txn: Transaction, lock_manager: LockManager, log_manager: LogManager) -> Result<Option<RID>> {
        todo!()
    }

    /// mark a tuple as deleted. this does not actually delete the tuple
    /// rid: rid of the tuple to mark as deleted
    /// txn: transaction performing the delete
    /// lock_manager: the lock manager
    /// log_manager: the log manager
    pub fn mark_delete(&mut self, rid: &RID, txn: Transaction, lock_manager: LockManager, log_manager: LogManager) -> Result<bool> {
        todo!()
    }

    /// update a tuple
    /// new_tuple: new value of the tuple
    /// rid: rid of the tuple
    /// txn: transaction performing the update
    /// lock_manager: the lock manager
    /// log_manager: the log manager
    pub fn update_tuple(&mut self, new_tuple: &Tuple, rid: &RID, txn: Transaction, lock_manager: LockManager, log_manager: LogManager) -> Result<Option<Tuple>> {
        todo!()
    }

    /// to be called on commit or abort.
    /// Actually perform the delete or rollback an insert
    pub fn apply_delete(&mut self, rid: &RID, txn: Transaction, log_manager: LogManager) -> Result<()> {
        todo!()
    }

    /// to be called on abort.
    /// Rollback a delete,
    /// i.e. the reverses a MarkDelete
    pub fn rollback_delete(&mut self, rid: &RID, txn: Transaction, log_manager: LogManager) -> Result<()> {
        todo!()
    }

    /// read a tuple from a table
    /// rid: rid of the tuple to read
    /// tnx: transaction performing the read
    /// lock_manager: the lock manager
    pub fn get_tuple(&self, rid: &RID, txn: Transaction, lock_manager: LockManager) -> Result<Option<Tuple>> {
        todo!()
    }

    /// return the first tuple if exists
    pub fn get_first_tuple_rid(&self) -> Result<Option<RID>> {
        todo!()
    }

    /// return the next tuple exists
    /// cur_rid: the RID of the current tuple
    pub fn get_next_tuple_rid(&self, cur_rid: &RID) -> Result<Option<RID>> {
        todo!()
    }

    /// return pointer to the end of current free space.
    /// see header commit
    fn get_free_space_pointer(&self) -> Result<usize> {
        let data = self.get_data()?;
        let pointer = vec_to_u32(&data, TablePage::OFFSET_FREE_SPACE)?;
        Ok(pointer as usize)
    }

    /// set the pointer, this should be the end of the current free space
    fn set_free_space_pointer(&mut self, free_space_pointer: u32) -> Result<()> {
        let pointer_data = u32_to_vec(free_space_pointer)?;
        self.set_data(pointer_data, TablePage::OFFSET_FREE_SPACE, 4);
        Ok(())
    }

    /// returned tuple count may be an overestimate because some slots may be empty
    /// return at least the number of tuples in the page
    fn get_tuple_count(&self) -> Result<usize> {
        let data = self.get_data()?;
        let count = vec_to_u32(&data, TablePage::OFFSET_TUPLE_COUNT)?;
        Ok(count as usize)
    }

    /// set the number of tuples in this page
    fn set_tuple_count(&mut self, tuple_count: u32) -> Result<()> {
        let tuple_data = u32_to_vec(tuple_count)?;
        self.set_data(tuple_data, TablePage::OFFSET_TUPLE_COUNT, 4);
        Ok(())
    }

    fn get_free_space_remaining(&self) -> Result<usize> {
        let free_space_pointer = self.get_free_space_pointer()?;
        let tuple_count = self.get_tuple_count()?;
        Ok(free_space_pointer - TablePage::SIZE_TABLE_PAGE_HEADER - TablePage::SIZE_TUPLE * tuple_count)
    }

    /// return tuple offset at slot slot_num
    fn get_tuple_offset_at_slot(&self, slot_num: u32) -> Result<u32> {
        let data = self.get_data()?;
        vec_to_u32(&data, TablePage::OFFSET_TUPLE_OFFSET + TablePage::SIZE_TUPLE * slot_num as usize)
    }

    /// set tuple offset at slot slot_num
    fn set_tuple_offset_at_slot(&mut self, slot_num: u32, offset: u32) -> Result<()> {
        let data = u32_to_vec(offset)?;
        self.set_data(data, TablePage::OFFSET_TUPLE_OFFSET + TablePage::SIZE_TUPLE * slot_num as usize, 4)
    }

    /// return tuple size at slot slot_num
    fn get_tuple_size(&self, slot_num: u32) -> Result<usize> {
        let data = self.get_data()?;
        let size = vec_to_u32(&data, TablePage::OFFSET_TUPLE_SIZE + TablePage::SIZE_TUPLE * slot_num as usize)?;
        Ok(size as usize)
    }

    /// set tuple size at slot slot_num
    fn set_tuple_size(&mut self, slot_num: u32, size: u32) -> Result<()> {
        let data = u32_to_vec(size)?;
        self.set_data(data, TablePage::OFFSET_TUPLE_SIZE + TablePage::SIZE_TUPLE * slot_num as usize, 4)
    }

    /// return true if the tuple is deleted or empty
    pub fn is_deleted(tuple_size: u32) -> bool {
        (tuple_size & DELETE_MASK) != 0 || tuple_size == 0
    }

    /// return tuple size with the deleted flag set
    pub fn set_deleted_flag(tuple_size: u32) -> u32 {
        tuple_size | DELETE_MASK
    }

    /// return tuple size with the deleted flag unset
    pub fn unset_deleted_flag(tuple_size: u32) -> u32 {
        tuple_size & !DELETE_MASK
    }

}

/// for the type change, make HeaderPage to Page
/// and not rewrite code
impl Deref for HeaderPage {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl Deref for TablePage {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl DerefMut for HeaderPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

impl DerefMut for TablePage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

fn vec_to_u32(data: &Vec<u8>, offset: usize) -> Result<u32> {
    if data.len() < offset as usize + 4 {
        return Err(Error::Value(String::from("the offset out of range")));
    }

    let n1 = (data.get(offset).unwrap().clone() as u32) << 24;
    let n2 = (data.get(offset + 1).unwrap().clone() as u32) << 16;
    let n3 = (data.get( offset + 2).unwrap().clone() as u32) << 8;
    let n4 = data.get(offset + 3).unwrap().clone() as u32;
    let lsn = n1 | n2 | n3 | n4;
    Ok(lsn)
}

fn u32_to_vec(num: u32) -> Result<Vec<u8>> {
    let n4 = (num & 0xff) as u8;
    let n3 = (num >> 8 & 0xff) as u8;
    let n2 = (num >> 16 & 0xff) as u8;
    let n1 = (num >> 24 & 0xff) as u8;
    let data = vec![n1, n2, n3, n4];
    Ok(data)
}