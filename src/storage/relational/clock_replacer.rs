use super::page::TablePage;
use crate::error::{Error, Result};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

/// Cache Page, and decide on page replacement behavior
pub struct ClockReplacer {
    clock_hand: u32,
    pages: Vec<Arc<Mutex<TablePage>>>,
    capacity: u32,
}

#[derive(Eq, Hash, PartialEq)]
pub enum ExpelLevel {
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

    pub fn is_edited(&self) -> bool {
        self.edited.clone()
    }

    pub fn level(&self) -> ExpelLevel {
        ExpelLevel::from(&self)
    }
}

impl ExpelLevel {
    pub fn from(clock_status: &ClockStatus) -> ExpelLevel {
        return if !clock_status.used && !clock_status.edited {
            ExpelLevel::HIGH
        } else if clock_status.used && !clock_status.edited {
            ExpelLevel::NORMAL
        } else if !clock_status.used && clock_status.edited {
            ExpelLevel::MEDIUM
        } else {
            ExpelLevel::LOW
        };
    }
}

impl ClockReplacer {
    pub fn new(capacity: u32) -> Result<ClockReplacer> {
        if capacity == 0 {
            return Err(Error::Value(String::from("capacity can't be zero!")));
        }
        Ok(ClockReplacer { clock_hand: 0, pages: Vec::new(), capacity })
    }

    pub fn poll(&self, page_id: u32) -> Result<Option<Arc<Mutex<TablePage>>>> {
        let pages = &self.pages;
        let mut filter_page = pages
            .iter()
            .filter(|p| page_id.eq(p.lock().unwrap().get_page_id()))
            .collect::<Vec<_>>();
        if let Some(page) = filter_page.get_mut(0) {
            let page_rc = Arc::clone(page);
            return Ok(Some(page_rc));
        }

        Ok(None)
    }

    /// push a new page. if a page should be remove, return it
    pub fn push(&mut self, page: TablePage) -> Result<Option<Arc<Mutex<TablePage>>>> {
        let push_page = Arc::new(Mutex::new(page));
        if let Some(index) = self.check_hand()? {
            let remove_page = self.pages.remove(index);
            self.pages.insert(index, push_page);
            return Ok(Some(remove_page));
        } else {
            self.pages.push(push_page);
        }
        Ok(None)
    }

    /// clockwise!!!
    /// return:
    ///     None - There is still space, push directly
    ///     Some - The cache is full, turn the clock hand and find the page to be removed
    fn check_hand(&mut self) -> Result<Option<usize>> {
        if self.capacity as usize > self.pages.len() {
            return Ok(None);
        }

        let mut remove_index: Option<usize> = None;
        let mut loop_counter = 0;
        let mut have_err = false;

        loop {
            if loop_counter >= 4 {
                have_err = true;
                break;
            }
            let group_map = self.group_by_level();
            if let Some(high) = group_map.get(&ExpelLevel::HIGH) {
                if let Some(index) = high.get(0) {
                    self.clock_hand = *index;
                    remove_index = Some(*index as usize);
                    break;
                }
            }
            if let Some(normal) = group_map.get(&ExpelLevel::NORMAL) {
                if let Some(index) = normal.get(0) {
                    self.clock_hand = *index;
                    remove_index = Some(*index as usize);
                    break;
                }
            }
            if let Some(medium) = group_map.get(&ExpelLevel::MEDIUM) {
                if let Some(index) = medium.get(0) {
                    self.clock_hand = *index;
                    remove_index = Some(*index as usize);
                    break;
                }
            }

            self.clockwise();

            // we should not remove pages of this level
            // if let Some(low) = group_map.get(&ExpelLevel::LOW) {
            //     if let Some(index) = low.get(0) {
            //         self.clock_hand = *index;
            //         remove_index = Some(*index as usize);
            //         return Ok(Some(*index as usize));
            //     }
            // }
            loop_counter += 1;
        }

        if have_err {
            return Err(Error::Value(String::from(
                "Clock Replacer can not find any page by remove memory",
            )));
        }
        Ok(remove_index)
    }

    fn group_by_level(&self) -> HashMap<ExpelLevel, Vec<u32>> {
        let mut result_map: HashMap<ExpelLevel, Vec<u32>> = HashMap::new();
        let mut index: u32 = 0;
        for page in &self.pages {
            let mut table_page = page.lock().unwrap();
            let level = table_page.get_status_mut().level();

            if let Some(value) = result_map.get_mut(&level) {
                value.push(index);
            } else {
                let list = vec![index];
                result_map.insert(level, list);
            }
        }
        result_map
    }

    /// clockwise to clear used tag
    fn clockwise(&self) {
        let pages = &self.pages;
        for page in pages {
            let arc_page = Arc::clone(page);
            let mut mutex_page = arc_page.lock().unwrap();
            mutex_page.get_status_mut().un_used();
        }
    }
}
