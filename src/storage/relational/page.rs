use super::clock_replacer::ClockStatus;
use super::tuple::RID;
use crate::error::{Error, Result};
use crate::storage::relational::tuple::Tuple;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::option::Option::Some;
use std::str;

/// Page size: 4KB
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
/// | PageId (4)| Deleted (1)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
///  /--------------------------------------------------------------------------
///
///  /--------------------------------------------------------------
/// | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
///  /--------------------------------------------------------------
pub struct TablePage {
    page: Page,
    status: ClockStatus,
}

impl Page {
    pub fn new(page_id: u32, data: [u8; PAGE_SIZE]) -> Result<Page> {
        Ok(Page { data, page_id, pin_count: 0, is_dirty: false })
    }

    /// read data from page
    pub fn read_data(&self, data: &mut [u8], offset: usize, len: usize) -> Result<usize> {
        if offset > self.data.len() {
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
        if offset > self.data.len() {
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
    pub fn new(data: [u8; PAGE_SIZE]) -> Result<HeaderPage> {
        let mut header_page = HeaderPage { page: Page::new(0, data)? };
        header_page.set_record_count(0)?;
        Ok(header_page)
    }

    /// record related
    pub fn insert_record(&mut self, name: &str, root_id: u32) -> Result<bool> {
        if name.len() > 32 {
            return Ok(false);
        }
        // check for duplicate name
        if self.find_record_num(name)?.is_some() {
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

        if let Some(record_num) = self.find_record_num(name)? {
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
        if let Some(record_num) = self.find_record_num(name)? {
            let offset = record_num as usize * 36 + 4;
            let root_id_data = root_id.to_le_bytes();
            self.write_data(&root_id_data, offset, 4)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// return root if success
    pub fn get_root_id(&self, name: &str) -> Result<Option<u32>> {
        if let Some(record_num) = self.find_record_num(name)? {
            let offset = record_num as usize * 36 + 4 + 32;
            let mut root_id_data = [0u8; 4];
            self.read_data(&mut root_id_data, offset, 4)?;
            return Ok(Some(u32::from_le_bytes(root_id_data)));
        }
        Ok(None)
    }

    pub fn get_record_count(&self) -> Result<u32> {
        let mut record_count_data = [0u8; 4];
        self.read_data(&mut record_count_data, 0, 4)?;
        Ok(u32::from_le_bytes(record_count_data))
    }

    fn set_record_count(&mut self, record_count: u32) -> Result<()> {
        let data = record_count.to_le_bytes();
        self.write_data(&data, 0, 4)?;
        Ok(())
    }

    fn find_record_num(&self, name: &str) -> Result<Option<u32>> {
        if name.len() > 32 {
            return Ok(None);
        }
        let record_count = self.get_record_count()?;
        if record_count == 0 {
            return Ok(None);
        }

        for record_num in 0..record_count as usize {
            let mut read_name = [0u8; 32];
            let name_offset = record_num * 36 + 4;
            self.read_data(&mut read_name, name_offset, 32)?;
            let mut real_name = str::from_utf8(&read_name).unwrap();
            real_name = real_name.trim_end_matches(|c| c == '\0');
            if real_name.eq(name) {
                return Ok(Some(record_num as u32));
            }
        }
        Ok(None)
    }
}

impl TablePage {
    /// table page's header end offset
    /// or slot arrays start offset
    const SIZE_TABLE_PAGE_HEADER: usize = 25;

    /// one tuple meta data size in slot array,
    /// include tuple offset and tuple size
    const SIZE_TUPLE: usize = 8;

    /// Deleted flag offset, this size just one byte
    const OFFSET_DELETED: usize = 4;
    const OFFSET_LSN: usize = 5;
    const OFFSET_PREV_PAGE_ID: usize = 9;
    const OFFSET_NEXT_PAGE_ID: usize = 13;
    const OFFSET_FREE_SPACE: usize = 17;
    const OFFSET_TUPLE_COUNT: usize = 21;
    const OFFSET_TUPLE_OFFSET: usize = 25;
    /// naming things is hard
    const OFFSET_TUPLE_SIZE: usize = 29;

    // delete flag, the 32nd bit of tuple_size is the delete flag bit
    const DELETE_MASK: u32 = 1 << (size_of::<u32>() * 8 - 1);

    /// init the tablePage header.
    /// page_id: the page ID of this table page
    /// page_size: the size of this table page
    /// prev_page_id: the previous table page ID
    pub fn new(
        page_id: u32,
        prev_page_id: Option<u32>,
        data: [u8; PAGE_SIZE],
    ) -> Result<TablePage> {
        if page_id == 0 {
            return Err(Error::Value(String::from("table page id can not set 0!")));
        }
        let page = Page::new(page_id, data)?;
        let mut table_page = TablePage { page, status: ClockStatus::empty() };
        // used = true, when page created
        table_page.status.used();

        // init data, the page id can't changed
        let page_id_data = page_id.to_le_bytes();
        table_page.write_data(&page_id_data, 0, 4)?;

        table_page.set_tuple_count(0)?;
        if let Some(prev_id) = prev_page_id {
            table_page.set_prev_page_id(prev_id)?;
        }
        table_page.set_free_space_pointer(PAGE_SIZE as u32)?;

        Ok(table_page)
    }

    /// get lsn from table page
    pub fn get_lsn(&mut self) -> Result<u32> {
        self.status.used();
        let mut lsn_data = [0u8; 4];
        self.read_data(&mut lsn_data, TablePage::OFFSET_LSN, 4)?;
        Ok(u32::from_ne_bytes(lsn_data))
    }

    /// return the page ID of this table page
    pub fn get_table_page_id(&mut self) -> Result<u32> {
        self.status.used();
        let mut page_id_data = [0u8; 4];
        self.read_data(&mut page_id_data, 0, 4)?;
        Ok(u32::from_le_bytes(page_id_data))
    }

    /// return the page ID of the previous table page
    pub fn get_prev_page_id(&mut self) -> Result<u32> {
        self.status.used();
        let mut prev_data = [0u8; 4];
        self.read_data(&mut prev_data, TablePage::OFFSET_PREV_PAGE_ID, 4)?;
        Ok(u32::from_le_bytes(prev_data))
    }

    /// return the page ID of the next table page
    pub fn get_next_page_id(&mut self) -> Result<u32> {
        self.status.used();
        let mut prev_data = [0u8; 4];
        self.read_data(&mut prev_data, TablePage::OFFSET_NEXT_PAGE_ID, 4)?;
        Ok(u32::from_le_bytes(prev_data))
    }

    /// set the page id of the previous page in the table
    pub fn set_prev_page_id(&mut self, prev_page_id: u32) -> Result<()> {
        self.status.edited();
        let prev_data = prev_page_id.to_le_bytes();
        self.write_data(&prev_data, TablePage::OFFSET_PREV_PAGE_ID, 4)?;
        Ok(())
    }

    /// set the page id of the next page in the table
    pub fn set_next_page_id(&mut self, next_page_id: u32) -> Result<()> {
        self.status.edited();
        let next_page = next_page_id.to_le_bytes();
        self.write_data(&next_page, TablePage::OFFSET_NEXT_PAGE_ID, 4)?;
        Ok(())
    }

    /// insert a tuple into the page
    pub fn insert_tuple(&mut self, tuple: &mut Tuple) -> Result<bool> {
        if tuple.get_length() == 0 {
            return Err(Error::Value(String::from("Can't have empty tuple!")));
        }
        if self.get_free_space_remaining()? < (tuple.get_length() + TablePage::SIZE_TUPLE) as u32 {
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
        self.status.edited();
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

        self.status.edited();
        Ok(true)
    }

    /// delete this page
    pub fn delete_page(&mut self) -> Result<bool> {
        if self.page_is_deleted()? {
            return Ok(true);
        }
        let mut delete_flag = [1u8];
        self.write_data(&mut delete_flag, TablePage::OFFSET_DELETED, 1)?;
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
        self.data.copy_within(
            free_space_pointer as usize..tuple_offset as usize,
            (free_space_pointer + tuple_size) as usize,
        );

        // update slot
        for slot_num_i in 0..self.get_tuple_count()? {
            let tuple_offset_i = self.get_tuple_offset_at_slot(slot_num_i)?;
            if tuple_offset_i < tuple_offset {
                self.set_tuple_offset_at_slot(slot_num_i, tuple_offset_i + tuple_size)?;
            }
        }

        self.status.edited();
        Ok(())
    }

    /// update a tuple
    /// new_tuple: new value of the tuple
    /// old_tuple: old value of the tuple
    /// rid: rid of the tuple
    /// although we only need to change the data of the tuple, we need to wrap it with a tuple object when passing it.
    pub fn update_tuple(&mut self, tuple: &Tuple) -> Result<()> {
        let rid = tuple
            .get_rid()
            .ok_or(Error::Value(String::from("The tuple has not rid when update!!")))?;
        let new_tuple_size = tuple.get_length();
        let slot_num = *rid.get_slot_num();

        if new_tuple_size == 0 {
            return Err(Error::Value(String::from("Can't have empty tuple!")));
        }
        if slot_num >= self.get_tuple_count()? {
            return Err(Error::Value(String::from("Slot num has out of range")));
        }

        let tuple_size = self.get_tuple_size(slot_num)?;
        if TablePage::is_deleted(tuple_size) {
            return Err(Error::Value(String::from("this tuple was mark deleted")));
        }

        // the free space and old tuple size was less than new tuple size
        if self.get_free_space_remaining()? + tuple_size < new_tuple_size as u32 {
            return Err(Error::Value(String::from("there is not enough space on this page")));
        }

        let tuple_offset = self.get_tuple_offset_at_slot(slot_num)? as usize;
        let free_space_pointer = self.get_free_space_pointer()? as usize;
        if tuple_offset < free_space_pointer {
            return Err(Error::Value(String::from(
                "Offset should appear after current free space position.",
            )));
        }

        // move the before data of the tuple_offset,to make room for the new tuple
        self.data.copy_within(
            free_space_pointer..tuple_offset,
            free_space_pointer + tuple_size as usize - new_tuple_size,
        );

        // update free space pointer
        self.set_free_space_pointer(
            (free_space_pointer + tuple_size as usize - new_tuple_size) as u32,
        )?;

        // update tuple data
        let update_data = tuple.get_data();
        self.write_data(
            update_data,
            tuple_offset + tuple_size as usize - new_tuple_size,
            new_tuple_size,
        )?;

        // update all tuple offset
        for slot_num_i in 0..self.get_tuple_count()? {
            let slot_offset_i = self.get_tuple_offset_at_slot(slot_num_i)?;
            if slot_offset_i < tuple_offset as u32 {
                self.set_tuple_offset_at_slot(
                    slot_num_i,
                    slot_offset_i + tuple_size - new_tuple_size as u32,
                )?;
            }
        }

        // set meta data
        self.set_tuple_offset_at_slot(
            slot_num,
            tuple_offset as u32 + tuple_size - new_tuple_size as u32,
        )?;
        self.set_tuple_size(slot_num, new_tuple_size as u32)?;

        self.status.edited();
        Ok(())
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
        if TablePage::is_deleted(tuple_size) {
            self.set_tuple_size(slot_num, TablePage::unset_deleted_flag(tuple_size))?;
        }
        self.status.edited();
        Ok(())
    }

    /// read a tuple from a table
    /// rid: rid of the tuple to read
    pub fn get_tuple(&mut self, rid: &RID) -> Result<Option<Tuple>> {
        let page_id = *self.get_page_id();
        if page_id != *rid.get_page_id() {
            return Err(Error::Value(String::from("the page id is not include this page")));
        }
        let slot_num = *rid.get_slot_num();
        if slot_num >= self.get_tuple_count()? {
            return Ok(None);
        }
        let tuple_size = self.get_tuple_size(slot_num)?;
        if TablePage::is_deleted(tuple_size) {
            return Ok(None);
        }

        let tuple_offset = self.get_tuple_offset_at_slot(slot_num)?;
        let mut tuple_data = vec![0u8; tuple_size as usize];
        self.read_data(&mut tuple_data, tuple_offset as usize, tuple_size as usize)?;

        let tuple_rid = RID::new(page_id, slot_num);
        let mut tuple = Tuple::from_data(tuple_data);
        tuple.set_rid(tuple_rid);
        tuple.allocated();
        self.status.used();
        Ok(Some(tuple))
    }

    /// return the first tuple if exists
    pub fn get_first_tuple_rid(&mut self) -> Result<Option<RID>> {
        let tuple_count = self.get_tuple_count()?;
        if tuple_count == 0 {
            return Ok(None);
        }
        let page_id = *self.get_page_id();

        for slot_num in 0..tuple_count {
            let tuple_size = self.get_tuple_size(slot_num)?;
            if !TablePage::is_deleted(tuple_size) {
                let rid = RID::new(page_id, slot_num);
                self.status.used();
                return Ok(Some(rid));
            }
        }
        Ok(None)
    }

    /// return the next tuple exists
    /// cur_rid: the RID of the current tuple
    pub fn get_next_tuple_rid(&mut self, cur_rid: &RID) -> Result<Option<RID>> {
        let page_id = *self.get_page_id();
        if !page_id.eq(cur_rid.get_page_id()) {
            return Err(Error::Value(String::from(
                "this page id was not equals page_id in this cur_rid",
            )));
        }

        let slot_num = *cur_rid.get_slot_num();
        for slot_num_i in (slot_num + 1)..self.get_tuple_count()? {
            let tuple_size = self.get_tuple_size(slot_num_i)?;
            if !TablePage::is_deleted(tuple_size) {
                let rid = RID::new(page_id, slot_num_i);
                self.status.edited();
                return Ok(Some(rid));
            }
        }

        Ok(None)
    }

    /// get the ClockStatus from the table page to edit by ClockReplacer
    pub fn get_status_mut(&mut self) -> &mut ClockStatus {
        &mut self.status
    }

    /// check this page was deleted
    pub fn page_is_deleted(&self) -> Result<bool> {
        let mut flag = [0u8];
        self.read_data(&mut flag, TablePage::OFFSET_DELETED, 1)?;
        Ok(flag[0] == 0)
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
        let mut pointer = [0u8; 4];
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
        let mut count = [0u8; 4];
        self.read_data(&mut count, TablePage::OFFSET_TUPLE_COUNT, 4)?;
        Ok(u32::from_le_bytes(count))
    }

    /// set the number of tuples in this page
    fn set_tuple_count(&mut self, tuple_count: u32) -> Result<()> {
        let tuple_data = tuple_count.to_le_bytes();
        self.write_data(&tuple_data, TablePage::OFFSET_TUPLE_COUNT, 4)?;
        Ok(())
    }

    fn get_free_space_remaining(&self) -> Result<u32> {
        let free_space_pointer = self.get_free_space_pointer()?;
        let tuple_count = self.get_tuple_count()?;
        Ok(free_space_pointer
            - TablePage::SIZE_TABLE_PAGE_HEADER as u32
            - TablePage::SIZE_TUPLE as u32 * tuple_count)
    }

    /// return tuple offset at slot slot_num
    /// slot_num start from 0
    fn get_tuple_offset_at_slot(&self, slot_num: u32) -> Result<u32> {
        let mut offset = [0u8; 4];
        self.read_data(
            &mut offset,
            TablePage::OFFSET_TUPLE_OFFSET + TablePage::SIZE_TUPLE * slot_num as usize,
            4,
        )?;
        Ok(u32::from_le_bytes(offset))
    }

    /// set tuple offset at slot slot_num
    fn set_tuple_offset_at_slot(&mut self, slot_num: u32, offset: u32) -> Result<()> {
        let offset_data = offset.to_le_bytes();
        self.write_data(
            &offset_data,
            TablePage::OFFSET_TUPLE_OFFSET + TablePage::SIZE_TUPLE * slot_num as usize,
            4,
        )?;
        Ok(())
    }

    /// return tuple size at slot slot_num
    fn get_tuple_size(&self, slot_num: u32) -> Result<u32> {
        let mut tuple_size = [0u8; 4];
        self.read_data(
            &mut tuple_size,
            TablePage::OFFSET_TUPLE_SIZE + TablePage::SIZE_TUPLE * slot_num as usize,
            4,
        )?;
        Ok(u32::from_le_bytes(tuple_size))
    }

    /// set tuple size at slot slot_num
    fn set_tuple_size(&mut self, slot_num: u32, size: u32) -> Result<()> {
        let tuple_size = size.to_le_bytes();
        self.write_data(
            &tuple_size,
            TablePage::OFFSET_TUPLE_SIZE + TablePage::SIZE_TUPLE * slot_num as usize,
            4,
        )?;
        Ok(())
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
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
