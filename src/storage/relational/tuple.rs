pub struct Tuple {
    data: Vec<u8>,
    rid: Option<RID>,
    allocated: bool,
}

pub struct RID {
    page_id: u32,
    slot_num: u32,
}

impl Tuple {
    pub fn empty() -> Tuple {
        Tuple { data: Vec::new(), rid: None, allocated: false }
    }

    pub fn from_data(data: Vec<u8>) -> Tuple {
        Tuple { data, rid: None, allocated: false }
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    pub fn get_data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// return RID of current tuple
    pub fn get_rid(&self) -> Option<&RID> {
        match &self.rid {
            Some(rid) => Some(rid),
            None => None,
        }
    }

    pub fn set_rid(&mut self, rid: RID) -> bool {
        if let Some(_) = self.rid {
            return false;
        }
        self.rid = Some(rid);
        true
    }

    /// Get length of the tuple
    pub fn get_length(&self) -> usize {
        self.data.len()
    }

    pub fn allocated(&mut self) {
        self.allocated = true;
    }
}

impl RID {
    pub fn new(page_id: u32, slot_num: u32) -> RID {
        RID { page_id, slot_num }
    }

    pub fn get_page_id(&self) -> &u32 {
        &self.page_id
    }

    pub fn get_slot_num(&self) -> &u32 {
        &self.slot_num
    }
}
