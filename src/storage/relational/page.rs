use crate::error::Result;

/// 每个Page的固定大小：4KB
pub const PAGE_SIZE: usize = 4096;

pub trait Page {
    /// 返回包含在此页面中的实际数据
    fn get_data(&self) -> Result<Vec<u8>>;

    /// 返回当前Page的PageId
    fn get_page_id(&self) -> Result<u32>;

    //返回当前Page的引脚数
    fn get_pin_count(&self) -> Result<u32>;

    /// 当前Page是否是脏的
    fn is_dirty(&self) -> Result<bool>;

    /// 获取Page的写锁
    fn write_latch(&self) -> Result<()>;

    /// 释放Page的写锁
    fn write_unlatch(&self) -> Result<()>;

    /// 获取Page的读锁
    fn read_latch(&self) -> Result<()>;

    /// 释放Page的读锁
    fn read_unlatch(&self) -> Result<()>;

    /// 获取当前Page的日志序列号
    fn get_log_sequence_number(&self) -> Result<u32>;

    /// 设置日志序列号
    fn set_log_sequence_number(&self) -> Result<()>;
}

pub struct ToyPage {
    data: Box<[u8; PAGE_SIZE]>,
    page_id: i32,
    pin_count: u32,
    is_dirty: bool,
}

impl ToyPage {
    pub fn new() -> Result<ToyPage> {
        let data = [0 as u8; PAGE_SIZE];
        let page = ToyPage { data: Box::new(data), page_id: -1, pin_count: 0, is_dirty: false };
        Ok(page)
    }
}

impl Page for ToyPage {
    fn get_data(&self) -> Result<Vec<u8>> {
        todo!()
    }

    fn get_page_id(&self) -> Result<u32> {
        todo!()
    }

    fn get_pin_count(&self) -> Result<u32> {
        todo!()
    }

    fn is_dirty(&self) -> Result<bool> {
        todo!()
    }

    fn write_latch(&self) -> Result<()> {
        todo!()
    }

    fn write_unlatch(&self) -> Result<()> {
        todo!()
    }

    fn read_latch(&self) -> Result<()> {
        todo!()
    }

    fn read_unlatch(&self) -> Result<()> {
        todo!()
    }

    fn get_log_sequence_number(&self) -> Result<u32> {
        todo!()
    }

    fn set_log_sequence_number(&self) -> Result<()> {
        todo!()
    }
}
