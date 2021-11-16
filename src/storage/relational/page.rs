use crate::error::{Error, Result};
use crate::storage::relational::rid::RID;
use crate::storage::relational::tuple::Tuple;
use std::ops::{Deref, DerefMut};
use std::option::Option::Some;
use std::sync::{Arc, Mutex};

/// 每个Page的固定大小：4KB
pub const PAGE_SIZE: usize = 4095;

/// LSN's offset in data
pub const OFFSET_LSN: usize = 4;

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
pub struct HeaderPage {
    page: Page,
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
    page: Page,
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
        self.set_data(&new_lsn, OFFSET_LSN, 4)?;
        Ok(true)
    }

    fn set_data(&mut self, data: &[u8], offset: usize, len: usize) -> Result<()> {
        if len < 0 || len > data.len() {
            return Err(Error::Value(String::from("the len is out of range")));
        }

        let mut self_data = self.data.lock()?;
        if offset < 0 || len + offset > self_data.len() {
            return Err(Error::Value(String::from("the offset/len is out of range")));
        }
        let wriet_data = Vec::from(data);
        self_data.splice(offset..offset + len, wriet_data);
        Ok(())
    }
}

impl HeaderPage {
    pub fn new() -> Result<HeaderPage> {
        Ok(HeaderPage { page: Page::new()? })
    }

    pub fn init(&mut self) -> Result<()> {
        self.set_record_count(0)
    }

    /// record related
    pub fn insert_record(&mut self, name: &str, root_id: u32) -> Result<bool> {
        if name.len() > 32 {
            return Ok(false);
        }
        // check for duplicate name
        if self.find_record(name)?.is_some() {
            return Ok(false);
        }

        let record_count = self.get_record_count()?;
        let offset = 4 + record_count * 36;

        // insert name
        let record_data = Vec::from(name.as_bytes());
        let record_data_len = record_data.len();
        self.set_data(&record_data, offset as usize, record_data_len)?;

        // insert root_id
        let root_id_data = u32_to_vec(root_id)?;
        self.set_data(&root_id_data, offset as usize + 32, 4)?;

        // add record
        self.set_record_count(record_count + 1)?;
        Ok(true)
    }

    pub fn delete_record(&mut self, name: &str) -> Result<bool> {
        let record_count = self.get_record_count()?;
        if record_count == 0 {
            return Ok(false);
        }

        return if let Some(record_num) = self.find_record(name)? {
            // the record start offset
            let offset = record_num as usize * 36 + 4;
            // find need move data len
            let len = (record_count - record_num - 1) as usize * 36;
            let start_index = offset + 36;
            let end_index = start_index + len;

            let data = self.get_data()?;
            let move_data = Vec::from(&data[start_index..end_index]);
            self.set_data(&move_data, offset, len)?;

            self.set_record_count(record_count - 1)?;
            Ok(true)
        } else {
            // record not exits
            Ok(false)
        };
    }

    pub fn update_record(&mut self, name: &str, root_id: u32) -> Result<bool> {
        if let Some(record_num) = self.find_record(name)? {
            let offset = record_num * 36 + 4;
            let root_id_data = u32_to_vec(root_id)?;
            self.set_data(&root_id_data, offset as usize + 32, 4)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// return root if success
    pub fn get_root_id(&self, name: &str) -> Result<Option<u32>> {
        if let Some(record_num) = self.find_record(name)? {
            let offset = record_num * 36 + 4;
            let data = self.get_data()?;
            let root_id = vec_to_u32(&data, offset as usize + 32)?;
            return Ok(Some(root_id));
        }
        Ok(None)
    }

    pub fn get_record_count(&self) -> Result<u32> {
        let data = self.get_data()?;
        vec_to_u32(&data, 0)
    }

    /// helper function
    fn set_record_count(&mut self, record_count: u32) -> Result<()> {
        let record_data = u32_to_vec(record_count)?;
        self.set_data(&record_data, 0, 4)?;
        Ok(())
    }

    fn find_record(&self, name: &str) -> Result<Option<u32>> {
        if name.len() > 32 {
            return Ok(None);
        }
        let record_count = self.get_record_count()? as usize;
        if record_count == 0 {
            return Ok(None);
        }
        let data = self.get_data()?;
        for record_num in 0..record_count {
            let offset = record_num * 36 + 4;
            let name_data = &data[offset..offset + 32];
            let mut blank_index = name_data.len() - 1;
            for index in 0..name_data.len() {
                if name_data[name_data.len() - index - 1] != 0u8 {
                    blank_index = name_data.len() - index - 1;
                    break;
                }
            }
            let real_name_data = &name_data[0..blank_index + 1];
            let local_name = String::from_utf8_lossy(real_name_data);
            if local_name.to_string().eq(name) {
                return Ok(Some(record_num as u32));
            }
        }
        Ok(None)
    }
}

impl TablePage {
    /// table page's header end offset
    /// or slot arrays start offset
    const SIZE_TABLE_PAGE_HEADER: usize = 24;

    /// one tuple meta data size in slot array,
    /// include tuple offset and tuple size
    const SIZE_TUPLE: usize = 8;

    const OFFSET_PREV_PAGE_ID: usize = 8;
    const OFFSET_NEXT_PAGE_ID: usize = 12;
    const OFFSET_FREE_SPACE: usize = 16;
    const OFFSET_TUPLE_COUNT: usize = 20;
    const OFFSET_TUPLE_OFFSET: usize = 24;
    /// naming things is hard
    const OFFSET_TUPLE_SIZE: usize = 28;

    // delete flag, the 32nd bit of tuple_size is the delte flag bit
    const DELETE_MASK: u32 = 2147483648;

    /// init the tablePage header.
    /// page_id: the page ID of this table page
    /// page_size: the size of this table page
    /// prev_page_id: the previous table page ID
    pub fn init(page_id: u32, page_size: u32, prev_page_id: u32) -> Result<TablePage> {
        let page = Page {
            page_id,
            pin_count: 0,
            is_dirty: false,
            data: Arc::new(Mutex::new(vec![0u8; page_size as usize])),
        };
        let mut table_page = TablePage { page };

        // init data, the page id can't changed
        let page_id_vec = u32_to_vec(page_id)?;
        table_page.set_data(&page_id_vec, 0, 4)?;

        table_page.set_tuple_count(0)?;
        table_page.set_prev_page_id(prev_page_id)?;
        table_page.set_free_space_pointer(page_size)?;

        Ok(table_page)
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
        self.set_data(&new_vec, TablePage::OFFSET_PREV_PAGE_ID, 4)?;
        Ok(true)
    }

    /// set the page id of the next page in the table
    pub fn set_next_page_id(&mut self, next_page_id: u32) -> Result<bool> {
        let new_vec = u32_to_vec(next_page_id)?;
        self.set_data(&new_vec, TablePage::OFFSET_NEXT_PAGE_ID, 4)?;
        Ok(true)
    }

    /// insert a tuple into the page
    /// tuple: tuple to insert
    ///
    /// RID: rid of the inserted tuple
    pub fn insert_tuple(&mut self, tuple: &mut Tuple, rid: &mut RID) -> Result<bool> {
        if tuple.get_length() == 0 {
            return Err(Error::Value(String::from("Can't have empty tuple!")));
        }
        if self.get_free_space_remaining()? < tuple.get_length() + TablePage::SIZE_TUPLE {
            return Ok(false);
        }
        let slot_num = self.get_tuple_count()?;
        let free_space_pointer = self.get_free_space_pointer()? - tuple.get_length() as u32;
        let save_data = tuple.get_data();
        let save_len = save_data.len();

        self.set_data(save_data, free_space_pointer as usize, save_len)?;
        self.set_free_space_pointer(free_space_pointer)?;
        self.set_tuple_offset_at_slot(slot_num, free_space_pointer)?;
        self.set_tuple_size(slot_num, save_len as u32)?;
        self.set_tuple_count(slot_num + 1)?;

        rid.set(*self.get_page_id(), slot_num);

        Ok(true)
    }

    /// mark a tuple as deleted. this does not actually delete the tuple
    /// rid: rid of the tuple to mark as deleted
    pub fn mark_delete(&mut self, rid: &RID) -> Result<bool> {
        let slot_num = *rid.get_slot_num();
        if self.get_tuple_count()? <= slot_num {
            return Ok(false);
        }
        let tuple_size = self.get_tuple_size(slot_num)?;
        if TablePage::is_deleted(tuple_size as u32) {
            return Ok(false);
        }

        if tuple_size > 0 {
            self.set_tuple_size(slot_num, TablePage::set_deleted_flag(tuple_size as u32))?;
        }
        Ok(true)
    }

    /// update a tuple
    /// new_tuple: new value of the tuple
    /// old_tuple: old value of the tuple
    /// rid: rid of the tuple
    /// although we only need to change the data of the tuple, we need to wrap it with a tuple object when passing it.
    pub fn update_tuple(&mut self, new_tuple: &Tuple, _old_tuple: Tuple, rid: &RID) -> Result<bool> {
        let new_tuple_len = new_tuple.get_length();
        let slot_num = *rid.get_slot_num();
        if new_tuple_len == 0 {
            return Err(Error::Value(String::from("Can't have empty tuple!")));
        }
        if slot_num >= self.get_tuple_count()? {
            return Ok(false);
        }

        let tuple_size = self.get_tuple_size(slot_num)?;
        if TablePage::is_deleted(tuple_size as u32) {
            return Ok(false);
        }
        // the free space and old tuple size was less than new tuple size
        if self.get_free_space_remaining()? + tuple_size < new_tuple_len {
            return Ok(false);
        }

        // if we should add log manager later, we should build old tuple and log it
        // TODO it

        let tuple_offset = self.get_tuple_offset_at_slot(slot_num)? as usize;
        let free_space_pointer = self.get_free_space_pointer()? as usize;
        if tuple_offset < free_space_pointer {
            return Err(Error::Value(String::from("Offset should appear after current free space position.")));
        }

        // move the before data of the tuple_offset,to make room for the new tuple
        let move_prev_data = &self.get_data()?[free_space_pointer..tuple_offset];
        let move_prev_data_len = move_prev_data.len();
        self.set_data(move_prev_data, free_space_pointer + tuple_size - new_tuple_len, move_prev_data_len)?;

        // update free space pointer
        self.set_free_space_pointer((free_space_pointer + tuple_size - new_tuple_len) as u32)?;

        // update tuple data
        let update_data = new_tuple.get_data();
        self.set_data(update_data, tuple_offset + tuple_size - new_tuple_len, new_tuple_len)?;
        
        // update all tuple offset
        // we just update slot num < solt_num
        for i in 0..slot_num as usize {
            let solt_offset_i = self.get_tuple_offset_at_slot(i as u32)?;
            self.set_tuple_offset_at_slot(i as u32, solt_offset_i + tuple_size as u32 - new_tuple_len as u32)?;
        }

        // set meta data
        self.set_tuple_offset_at_slot(slot_num, (tuple_offset + tuple_size - new_tuple_len) as u32)?;
        self.set_tuple_size(slot_num, new_tuple_len as u32)?;

        Ok(true)
    }

    /// to be called on commit or abort.
    /// Actually perform the delete or rollback an insert
    pub fn apply_delete(&mut self, rid: &RID) -> Result<()> {
        todo!()
    }

    /// to be called on abort.
    /// Rollback a delete,
    /// i.e. the reverses a MarkDelete
    pub fn rollback_delete(&mut self, rid: &RID) -> Result<()> {
        todo!()
    }

    /// read a tuple from a table
    /// rid: rid of the tuple to read
    /// tnx: transaction performing the read
    /// lock_manager: the lock manager
    pub fn get_tuple(&self, rid: &RID) -> Result<Option<Tuple>> {
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
    fn get_free_space_pointer(&self) -> Result<u32> {
        let data = self.get_data()?;
        let pointer = vec_to_u32(&data, TablePage::OFFSET_FREE_SPACE)?;
        Ok(pointer)
    }

    /// set the pointer, this should be the end of the current free space
    fn set_free_space_pointer(&mut self, free_space_pointer: u32) -> Result<()> {
        let pointer_data = u32_to_vec(free_space_pointer)?;
        self.set_data(&pointer_data, TablePage::OFFSET_FREE_SPACE, 4)?;
        Ok(())
    }

    /// returned tuple count may be an overestimate because some slots may be empty
    /// return at least the number of tuples in the page
    fn get_tuple_count(&self) -> Result<u32> {
        let data = self.get_data()?;
        let count = vec_to_u32(&data, TablePage::OFFSET_TUPLE_COUNT)?;
        Ok(count)
    }

    /// set the number of tuples in this page
    fn set_tuple_count(&mut self, tuple_count: u32) -> Result<()> {
        let tuple_data = u32_to_vec(tuple_count)?;
        self.set_data(&tuple_data, TablePage::OFFSET_TUPLE_COUNT, 4)?;
        Ok(())
    }

    fn get_free_space_remaining(&self) -> Result<usize> {
        let free_space_pointer = self.get_free_space_pointer()? as usize;
        let tuple_count = self.get_tuple_count()? as usize;
        Ok(free_space_pointer
            - TablePage::SIZE_TABLE_PAGE_HEADER
            - TablePage::SIZE_TUPLE * tuple_count)
    }

    /// return tuple offset at slot slot_num
    /// slot_num start from 0
    fn get_tuple_offset_at_slot(&self, slot_num: u32) -> Result<u32> {
        let data = self.get_data()?;
        vec_to_u32(
            &data,
            TablePage::OFFSET_TUPLE_OFFSET + TablePage::SIZE_TUPLE * slot_num as usize,
        )
    }

    /// set tuple offset at slot slot_num
    fn set_tuple_offset_at_slot(&mut self, slot_num: u32, offset: u32) -> Result<()> {
        let data = u32_to_vec(offset)?;
        self.set_data(
            &data,
            TablePage::OFFSET_TUPLE_OFFSET + TablePage::SIZE_TUPLE * slot_num as usize,
            4,
        )
    }

    /// return tuple size at slot slot_num
    fn get_tuple_size(&self, slot_num: u32) -> Result<usize> {
        let data = self.get_data()?;
        let size = vec_to_u32(
            &data,
            TablePage::OFFSET_TUPLE_SIZE + TablePage::SIZE_TUPLE * slot_num as usize,
        )?;
        Ok(size as usize)
    }

    /// set tuple size at slot slot_num
    fn set_tuple_size(&mut self, slot_num: u32, size: u32) -> Result<()> {
        let data = u32_to_vec(size)?;
        self.set_data(
            &data,
            TablePage::OFFSET_TUPLE_SIZE + TablePage::SIZE_TUPLE * slot_num as usize,
            4,
        )
    }

    /// return true if the tuple is deleted or empty
    pub fn is_deleted(tuple_size: u32) -> bool {
        (tuple_size & TablePage::DELETE_MASK) != 0 || tuple_size == 0
    }

    /// return tuple size with the deleted flag set
    pub fn set_deleted_flag(tuple_size: u32) -> u32 {
        tuple_size | TablePage::DELETE_MASK
    }

    /// return tuple size with the deleted flag unset
    pub fn unset_deleted_flag(tuple_size: u32) -> u32 {
        tuple_size & !TablePage::DELETE_MASK
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
    let n3 = (data.get(offset + 2).unwrap().clone() as u32) << 8;
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
