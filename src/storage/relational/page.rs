use crate::error::{Error, Result};
use crate::storage::relational::rid::RID;
use crate::storage::relational::tuple::Tuple;
use std::ops::{Deref, DerefMut};
use std::option::Option::Some;
use crate::serialization::ToVecAndByVec;

use super::tuple::RID;

/// 每个Page的固定大小：4KB
pub const PAGE_SIZE: usize = 4095;

/// the data page, we have to implement
pub struct Page {
    data: [u8; PAGE_SIZE],
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
pub struct TablePage {
    page: Page,
}

impl Page {
    pub fn new() -> Result<Page> {
        Ok(Page {
            data: [0u8; PAGE_SIZE],
            page_id: 0,
            pin_count: 0,
            is_dirty: false,
        })
    }

    /// read data from page
    pub fn read_data(&self, data: &mut [u8], offset: usize, len: usize) -> Result<usize> {
        if offset > data.len() {
            return Err(Error::Value("offset is out of range".to_string()));
        }
        let mut end = offset + len;
        if end > offset + data.len() {
            end = offset + data.len();
        }
        if end > self.data.len() {
            end = self.data.len();
        }

        let read_data = &self.data[offset..end];
        data.copy_from_slice(read_data);
        Ok(end - offset)
    }

    /// write data to page
    pub fn write_data(&mut self, data: &[u8], offset: usize, len: usize) -> Result<usize> {
        if offset > data.len() {
            return Err(Error::Value("offset is out of range".to_string()));
        }
        let mut end = offset + len;
        if end > offset + data.len() {
            end = offset + data.len();
        }
        if end > self.data.len() {
            end = self.data.len();
        }

        // be careful! We need to strictly ensure the consistency of the length of the written data
        let write_data = &data[..end - offset];
        let write_splice = &mut self.data[offset..end];
        write_splice.copy_from_slice(write_data);

        Ok(end - offset)
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

}

impl HeaderPage {

    pub fn new() -> Result<HeaderPage> {
        let header_page = HeaderPage {
            page : Page::new()?
        };
        header_page.set_record_count(0);
        Ok(header_page)
    }

    /// record related
    pub fn insert_record(&mut self, name: &str, root_id: u32) -> Result<bool> {
        if name.len() > 32 {
            return Ok(false);
        }
        // check for duplicate name
        if let Some(_) = self.find_record(name)? {
            return Ok(false);
        }
        let record_count = self.get_record_count()?;

        // insert name
        let name_offset = 4 + record_count as usize * 36;
        let name_data = name.as_bytes();
        self.write_data(name_data, name_offset, 32)?;

        // insert root_id
        let root_id_offset = name_offset + 32;
        let root_id_data = root_id.to_le_bytes();
        self.write_data(&root_id_data, root_id_offset, 4)?;

        // add record
        self.set_record_count(record_count + 1)?;
        Ok(true)
    }

    pub fn delete_record(&mut self, name: &str) -> Result<bool> {
        let record_count = self.get_record_count()?;
        if record_count == 0 {
            return Ok(false);
        }

        if let Some(record_num) = self.find_record(name)? {
            // the record start offset
            let offset = record_num as usize * 36 + 4;
            // find need move data len
            let start_pointer = offset + 36;
            let end_pointer = record_count as usize * 36 + 4;

            self.data.copy_within(start_pointer..end_pointer, offset);
            self.set_record_count(record_count - 1)?;
            Ok(true)
        } else {
            // record not exits
            Ok(false)
        }
    }

    pub fn update_record(&mut self, name: &str, root_id: u32) -> Result<bool> {
        if let Some(record_num) = self.find_record(name)? {
            let offset = record_num as usize * 36 + 4;
            let root_id_data = root_id.to_le_bytes();
            self.write_data(&root_id_data, offset, 4)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// return root if success
    pub fn get_root_id(&self, name: &str) -> Result<Option<u32>> {
        if let Some(record_num) = self.find_record(name)? {
            let offset = record_num as usize * 36 + 4 + 32;
            let root_id_data = [0u8; 4];
            self.read_data(&mut root_id_data, offset, 4)?;
            return Ok(Some(u32::from_le_bytes(root_id_data)));
        }
        Ok(None)
    }

    pub fn get_record_count(&self) -> Result<u32> {
        let record_count_data = [0u8; 4];
        self.read_data(&mut record_count_data, 0, 4)?;
        Ok(u32::from_le_bytes(record_count_data))
    }

    fn set_record_count(&mut self, record_count: u32) -> Result<()> {
        let data = record_count.to_le_bytes();
        self.write_data(&data, 0, 4)?;
        Ok(())
    }

    fn find_record(&self, name: &str) -> Result<Option<u32>> {
        if name.len() > 32 {
            return Ok(None);
        }
        let record_count = self.get_record_count()?;
        if record_count == 0 {
            return Ok(None);
        }
        let source_name = name.as_bytes();
        let read_name = [0u8; 32];
        let read_root_id = [0u8; 4];

        for record_num in 0..record_count as usize {
            let name_offset = record_num * 36 + 4;
            self.read_data(&mut read_name, name_offset, 32)?;
            if source_name.eq(&read_name) {
                self.read_data(&mut read_root_id, name_offset + 32, 4)?;
                return Ok(Some(u32::from_le_bytes(read_root_id)));
            }
        }

        Ok(None)
    }
}


impl TablePage {

    ///LSN's offset in data
    const OFFSET_LSN: usize = 4;
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
    pub fn new(page_id: u32, page_size: u32, prev_page_id: Option<u32>) -> Result<TablePage> {
        let page = Page::new()?;
        let mut table_page = TablePage { page };

        // init data, the page id can't changed
        let page_id_data = page_id.to_le_bytes();
        table_page.write_data(&page_id_data, 0, 4)?;

        table_page.set_tuple_count(0)?;
        if let Some(prev_id) = prev_page_id {
            table_page.set_prev_page_id(prev_id)?;
        }
        table_page.set_free_space_pointer(page_size)?;

        Ok(table_page)
    }

    /// get lsn from table page
    pub fn get_lsn(&self) -> Result<u32> {
        let lsn_data = [0u8; 4];
        self.read_data(&mut lsn_data, TablePage::OFFSET_LSN, 4)?;
        Ok(u32::from_ne_bytes(lsn_data))
    }

    /// return the page ID of this table page
    pub fn get_table_page_id(&self) -> Result<u32> {
        let page_id_data = [0u8; 4];
        self.read_data(&mut page_id_data, 0, 4)?;
        Ok(u32::from_le_bytes(page_id_data))
    }

    /// return the page ID of the previous table page
    pub fn get_prev_page_id(&self) -> Result<u32> {
        let prev_data = [0u8; 4];
        self.read_data(&mut prev_data, TablePage::OFFSET_PREV_PAGE_ID, 4)?;
        Ok(u32::from_le_bytes(prev_data))
    }

    /// return the page ID of the next table page
    pub fn get_next_page_id(&self) -> Result<u32> {
        let prev_data = [0u8; 4];
        self.read_data(&mut prev_data, TablePage::OFFSET_NEXT_PAGE_ID, 4)?;
        Ok(u32::from_le_bytes(prev_data))
    }

    /// set the page id of the previous page in the table
    pub fn set_prev_page_id(&mut self, prev_page_id: u32) -> Result<()> {
        let prev_data = prev_page_id.to_le_bytes();
        self.write_data(&prev_data, TablePage::OFFSET_PREV_PAGE_ID, 4)?;
        Ok(())
    }

    /// set the page id of the next page in the table
    pub fn set_next_page_id(&mut self, next_page_id: u32) -> Result<()> {
        let next_page = next_page_id.to_le_bytes();
        self.write_data(&next_page, TablePage::OFFSET_NEXT_PAGE_ID, 4)?;
        Ok(())

    }

    /// insert a tuple into the page
    pub fn insert_tuple(&mut self, tuple: &mut Tuple) -> Result<bool> {
        if tuple.get_length() == 0 {
            return Err(Error::Value(String::from("Can't have empty tuple!")));
        }
        if let None = tuple.get_rid() {
            return Ok(false);
        }
        if self.get_free_space_remaining()? < tuple.get_length() + TablePage::SIZE_TUPLE {
            return Ok(false);
        }

        // if we have blank slot, we can use it
        let slot_num = match self.get_free_slot_array()? {
            Some(i) => i,
            None => self.get_tuple_count()?,
        };

        let free_space_pointer = self.get_free_space_pointer()? - tuple.get_length() as u32;
        let save_data = tuple.get_data();
        let save_len = save_data.len();
        self.write_data(save_data, free_space_pointer as usize, save_len)?;

        self.set_free_space_pointer(free_space_pointer)?;
        self.set_tuple_offset_at_slot(slot_num, free_space_pointer)?;
        self.set_tuple_size(slot_num, save_len as u32)?;
        if slot_num == self.get_tuple_count()? {
            self.set_tuple_count(slot_num + 1)?;
        }

        let rid = RID::new(*self.get_page_id(), slot_num);
        tuple.set_rid(rid);
        tuple.allocated();
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

    /// to be called on commit or abort.
    /// Actually perform the delete or rollback an insert
    pub fn apply_delete(&mut self, rid: &RID) -> Result<()> {
        let slot_num = *rid.get_slot_num();
        if *self.get_page_id() != *rid.get_page_id() {
            return Err(Error::Value(String::from("the tuple is not storaged in this page")));
        }
        if slot_num >= self.get_tuple_count()? {
            return Err(Error::Value(String::from("Cannot have more slots than tuples.")));
        }

        let tuple_size = self.get_tuple_size(slot_num)?;
        if !TablePage::is_deleted(tuple_size as u32) {
            return Err(Error::Value(String::from("The tuple was not deleted.")));
        }
        let tuple_offset = self.get_tuple_offset_at_slot(slot_num)?;
        let free_space_pointer = self.get_free_space_pointer()?;
        if tuple_offset < free_space_pointer {
            return Err(Error::Value(String::from("Free space appearss before tuples.")));
        }

        // remove slot
        self.set_tuple_size(slot_num, 0)?;
        self.set_tuple_offset_at_slot(slot_num, 0)?;

        self.set_free_space_pointer(free_space_pointer + tuple_size as u32)?;

        // move data
        self.data.copy_within(free_space_pointer as usize..tuple_offset as usize, (free_space_pointer + tuple_size) as usize);

        // update slot
        for slot_num_i in 0..self.get_tuple_count()? {
            let tuple_offset_i = self.get_tuple_offset_at_slot(slot_num_i)?;
            if tuple_offset_i < tuple_offset {
                self.set_tuple_offset_at_slot(slot_num_i, tuple_offset_i + tuple_size)?;   
            }
        }

        Ok(())
    }

    /// update a tuple
    /// new_tuple: new value of the tuple
    /// old_tuple: old value of the tuple
    /// rid: rid of the tuple
    /// although we only need to change the data of the tuple, we need to wrap it with a tuple object when passing it.
    pub fn update_tuple(
        &mut self,
        new_tuple: &Tuple,
        _old_tuple: Tuple,
        rid: &RID,
    ) -> Result<bool> {
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
            return Err(Error::Value(String::from(
                "Offset should appear after current free space position.",
            )));
        }

        // move the before data of the tuple_offset,to make room for the new tuple
        let move_prev_data = &self.get_data()?[free_space_pointer..tuple_offset];
        let move_prev_data_len = move_prev_data.len();
        self.set_data(
            move_prev_data,
            free_space_pointer + tuple_size - new_tuple_len,
            move_prev_data_len,
        )?;

        // update free space pointer
        self.set_free_space_pointer((free_space_pointer + tuple_size - new_tuple_len) as u32)?;

        // update tuple data
        let update_data = new_tuple.get_data();
        self.set_data(update_data, tuple_offset + tuple_size - new_tuple_len, new_tuple_len)?;

        // update all tuple offset
        // we just update slot num < solt_num
        for i in 0..slot_num as usize {
            let solt_offset_i = self.get_tuple_offset_at_slot(i as u32)?;
            self.set_tuple_offset_at_slot(
                i as u32,
                solt_offset_i + tuple_size as u32 - new_tuple_len as u32,
            )?;
        }

        // set meta data
        self.set_tuple_offset_at_slot(
            slot_num,
            (tuple_offset + tuple_size - new_tuple_len) as u32,
        )?;
        self.set_tuple_size(slot_num, new_tuple_len as u32)?;

        Ok(true)
    }


    /// to be called on abort.
    /// Rollback a delete,
    /// i.e. the reverses a MarkDelete
    pub fn rollback_delete(&mut self, rid: &RID) -> Result<()> {
        let slot_num = *rid.get_slot_num();
        if slot_num >= self.get_tuple_count()? {
            return Err(Error::Value(String::from("we can't have more slots than tuples.")));
        }

        let tuple_size = self.get_tuple_size(slot_num)?;
        if TablePage::is_deleted(tuple_size as u32) {
            self.set_tuple_size(slot_num, TablePage::unset_deleted_flag(tuple_size as u32))?;
        }

        Ok(())
    }

    /// read a tuple from a table
    /// rid: rid of the tuple to read
    pub fn get_tuple(&self, rid: &RID) -> Result<Option<Tuple>> {
        let slot_num = *rid.get_slot_num();
        if slot_num >= self.get_tuple_count()? {
            return Ok(None);
        }
        let tuple_size = self.get_tuple_size(slot_num)?;
        if TablePage::is_deleted(tuple_size as u32) {
            return Ok(None);
        }

        let tuple_offset = self.get_tuple_offset_at_slot(slot_num)?;
        let data_ref =
            &self.get_data()?[(tuple_offset as usize)..(tuple_size as usize + tuple_size)];

        let tuple_data = Vec::from(data_ref);
        let tuple_rid = RID::from(*self.get_page_id(), slot_num);
        let tuple = Tuple::new(tuple_data, tuple_rid);
        Ok(Some(tuple))
    }

    /// return the first tuple if exists
    pub fn get_first_tuple_rid(&self) -> Result<Option<RID>> {
        let tuple_count = self.get_tuple_count()?;
        if tuple_count == 0 {
            return Ok(None);
        }
        let page_id = *self.get_page_id();

        for slot_num in 0..tuple_count {
            let tuple_size = self.get_tuple_size(slot_num)?;
            if !TablePage::is_deleted(tuple_size as u32) {
                let rid = RID::from(page_id, slot_num);
                return Ok(Some(rid));
            }
        }
        Ok(None)
    }

    /// return the next tuple exists
    /// cur_rid: the RID of the current tuple
    pub fn get_next_tuple_rid(&self, cur_rid: &RID) -> Result<Option<RID>> {
        let page_id = *self.get_page_id();
        if !page_id.eq(cur_rid.get_page_id()) {
            return Ok(None);
        }
        let slot_num = *cur_rid.get_slot_num();
        for i in (slot_num + 1)..self.get_tuple_count()? {
            let tuple_size = self.get_tuple_size(i)?;
            if !TablePage::is_deleted(tuple_size as u32) {
                let rid = RID::from(page_id, i);
                return Ok(Some(rid));
            }
        }

        Ok(None)
    }

    /// foreach slot array we haved, if tuple_size equals 0,
    /// we should return slot_num of the tuple
    fn get_free_slot_array(&self) -> Result<Option<u32>> {
        let tuple_count = self.get_tuple_count()?;
        for i in 0..tuple_count {
            let tuple_size = self.get_tuple_size(i)?;
            if tuple_size == 0 {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    /// return pointer to the end of current free space.
    /// see header commit
    fn get_free_space_pointer(&self) -> Result<u32> {
        let pointer = [0u8; 4];
        self.read_data(&mut pointer, TablePage::OFFSET_FREE_SPACE, 4)?;
        Ok(u32::from_le_bytes(pointer))
    }

    /// set the pointer, this should be the end of the current free space
    fn set_free_space_pointer(&mut self, free_space_pointer: u32) -> Result<()> {
        let pointer_data = free_space_pointer.to_le_bytes();
        self.write_data(&pointer_data, TablePage::OFFSET_FREE_SPACE, 4)?;
        Ok(())
    }

    /// returned tuple count may be an overestimate because some slots may be empty
    /// return at least the number of tuples in the page
    fn get_tuple_count(&self) -> Result<u32> {
        let count = [0u8; 4];
        self.read_data(&mut count, TablePage::OFFSET_TUPLE_COUNT, 4)?;
        Ok(u32::from_le_bytes(count))
    }

    /// set the number of tuples in this page
    fn set_tuple_count(&mut self, tuple_count: u32) -> Result<()> {
        let tuple_data = tuple_count.to_le_bytes();
        self.write_data(&tuple_data, TablePage::OFFSET_TUPLE_COUNT, 4)?;
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
        let offset = [0u8; 4];
        self.read_data(&mut offset, TablePage::OFFSET_TUPLE_OFFSET + TablePage::SIZE_TUPLE * slot_num as usize, 4)?;
        Ok(u32::from_le_bytes(offset))
    }

    /// set tuple offset at slot slot_num
    fn set_tuple_offset_at_slot(&mut self, slot_num: u32, offset: u32) -> Result<()> {
        let offset_data = offset.to_le_bytes();
        self.write_data(&offset_data, TablePage::OFFSET_TUPLE_OFFSET + TablePage::SIZE_TUPLE * slot_num as usize, 4)?;
        Ok(())
    }

    /// return tuple size at slot slot_num
    fn get_tuple_size(&self, slot_num: u32) -> Result<u32> {
        let tuple_size = [0u8; 4];
        self.read_data(&mut tuple_size, TablePage::OFFSET_TUPLE_SIZE + TablePage::SIZE_TUPLE * slot_num as usize, 4)?;
        Ok(u32::from_le_bytes(tuple_size))
    }

    /// set tuple size at slot slot_num
    fn set_tuple_size(&mut self, slot_num: u32, size: u32) -> Result<()> {
        let tuple_size = size.to_le_bytes();
        self.write_data(&tuple_size, TablePage::OFFSET_TUPLE_SIZE + TablePage::SIZE_TUPLE * slot_num as usize, 4)?;
        Ok(())
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

impl ToVecAndByVec<HeaderPage> for HeaderPage {
    fn to_vec(t: &HeaderPage) -> Vec<u8> {
        todo!()
    }

    fn by_vec(data: &Vec<u8>) -> Option<HeaderPage> {
        todo!()
    }
}

impl ToVecAndByVec<TablePage> for TablePage {
    fn to_vec(t: &TablePage) -> Vec<u8> {
        todo!()
    }

    fn by_vec(data: &Vec<u8>) -> Option<TablePage> {
        todo!()
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
