use super::rid::RID;

pub struct Tuple {
    data: Vec<u8>,
    rid: Option<RID>,
    allocated: bool,
}

impl Tuple {
    pub fn empty() -> Tuple {
        Tuple { data: Vec::new(), rid: None, allocated: false }
    }

    /// Get the data of this tuple in the table's backing store
    pub fn get_data(&self) -> &Vec<u8> {
        &self.data
    }

    /// return RID of current tuple
    pub fn get_rid(&self) -> Option<&RID> {
        match &self.rid {
            Some(rid) => Some(rid),
            None => None,
        }
    }

    /// Get length of the tuple
    pub fn get_length(&self) -> usize {
        self.data.len()
    }
}
