use super::page::TablePage;
use crate::error::{Error, Result};

/// Cache Page, and decide on page replacement behavior
pub struct ClockReplacer {
    clock_hand: u32,
    pages: Vec<TablePage>,
    capacity: u32,
    page_count: u32,
}

pub enum ExpellLevel {
    HIGH,
    NORMAL,
    MEDIUM,
    LOW,
}

pub struct ClockStatus {
    used: bool,
    edited: bool,
}

impl ClockStatus {
    pub fn empty() -> ClockStatus {
        ClockStatus { used: false, edited: false }
    }

    pub fn used(&mut self) {
        self.used = true;
    }

    pub fn edited(&mut self) {
        self.edited = true;
    }

    pub fn un_used(&mut self) {
        self.used = false;
    }

    pub fn un_edited(&mut self) {
        self.edited = false;
    }

    pub fn level(&self) -> ExpellLevel {
        ExpellLevel::from(&self)
    }
}

impl ExpellLevel {
    pub fn from(clock_status: &ClockStatus) -> ExpellLevel {
        if !clock_status.used && !clock_status.edited {
            return ExpellLevel::HIGH;
        } else if clock_status.used && !clock_status.edited {
            return ExpellLevel::NORMAL;
        } else if !clock_status.used && clock_status.edited {
            return ExpellLevel::MEDIUM;
        } else {
            return ExpellLevel::LOW;
        }
    }
}

impl ClockReplacer {
    pub fn new(capacity: u32) -> ClockReplacer {
        ClockReplacer { clock_hand: 0, pages: Vec::new(), capacity, page_count: 0 }
    }

    pub fn get_page(&mut self, page_id: u32) -> Result<Option<TablePage>> {
        let match_page =
            self.pages.iter().filter(|p| page_id.eq(p.get_page_id())).collect::<Vec<&TablePage>>();

        todo!()
    }
}
