pub struct RID {
    page_id: u32,
    slot_num: u32,
    is_inited: bool,
}

impl RID {
    pub fn new() -> RID {
        RID { page_id: 0, slot_num: 0, is_inited: false }
    }

    pub fn from(page_id: u32, slot_num: u32) -> RID {
        RID { page_id: page_id, slot_num: slot_num, is_inited: true }
    }

    pub fn set(&mut self, page_id: u32, slot_num: u32) {
        self.page_id = page_id;
        self.slot_num = slot_num;
    }

    pub fn get_slot_num(&self) -> &u32 {
        &self.slot_num
    }
}
