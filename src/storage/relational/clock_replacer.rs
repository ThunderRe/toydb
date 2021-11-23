use crate::error::Result;
use std::collections::HashMap;
use std::option::Option::Some;

/// Replacer 实现了时钟替换策略，它近似于最近最少使用的策略
pub trait Replacer {
    /// 从时钟指针的当前位置开始，找到在`ClockReplacer` 中并且其ref 标志设置为false 的第一帧。
    /// 如果一个帧在 `ClockReplacer` 中，但它的 ref 标志设置为 true，请将其更改为 false。
    /// 这应该是更新时钟指针的唯一方法。
    fn victim(&mut self, frame_id: u32) -> Result<bool>;

    /// 在将页面固定到 BufferPoolManager 中的框架后，应调用此方法。
    /// 它应该从 ClockReplacer 中删除包含固定页面的框架。
    fn pin(&mut self, frame_id: &u32) -> Result<()>;

    /// 当页面的 pin_count 变为 0 时，应调用此方法。
    /// 此方法应将包含未固定页面的框架添加到 ClockReplacer。
    fn un_pin(&mut self, frame_id: &u32) -> Result<()>;

    /// 此方法返回当前在 ClockReplacer 中的帧数。
    fn size(&mut self) -> usize;
}

#[derive(Clone, Debug)]
struct Frame {
    flag: bool,
    // 引用计数
    ref_count: u32,
    page_id: u32,
}

pub struct ClockReplacer {
    queue: Vec<Frame>,
    map: HashMap<u32, usize>,
    size: usize,
}

impl ClockReplacer {
    pub fn new(size: u32) -> Result<Self> {
        Ok(ClockReplacer { queue: Vec::new(), map: HashMap::new(), size: size as usize })
    }

    /// 进行一次扫描
    fn scan(&mut self) {
        for frame in self.queue.iter_mut() {
            if frame.ref_count.eq(&0) {
                frame.flag = false;
            }
        }
    }

    /// 移除某个page_id对应的frame
    fn remove(&mut self, page_id: &u32) {
        if let Some(queue_index) = self.map.get(page_id) {
            self.queue.remove(*queue_index);
            self.map.remove(page_id);

            self.flush_index();
        }
    }

    /// 刷新缓存
    fn flush_index(&mut self) {
        self.map.clear();
        for (index, frame) in self.queue.iter().enumerate() {
            self.map.insert(frame.page_id, Clone::clone(&index));
        }
    }

    /// 添加某个frame
    fn add(&mut self, page_id: u32) -> Result<()> {
        let frame = Frame { flag: true, ref_count: 0, page_id };
        self.queue.push(frame);
        for (queue_index, frame) in self.queue.iter().enumerate() {
            if frame.page_id == page_id {
                self.map.insert(page_id, queue_index);
            }
        }
        Ok(())
    }

    // fn find(&mut self, page_id: &u32) -> Option<&Frame> {
    //     if let Some(queue_index) = self.map.get(page_id) {
    //         return self.queue.get(*queue_index);
    //     }
    //     None
    // }

    fn find_mut(&mut self, page_id: &u32) -> Option<&mut Frame> {
        if let Some(queue_index) = self.map.get(page_id) {
            return self.queue.get_mut(*queue_index);
        }
        None
    }

    /// 寻找一个能够被移除的Frame Id,并将其移除
    fn try_remove(&mut self) -> Result<bool> {
        if self.size() < self.size {
            return Ok(true);
        }
        self.scan();
        for (key, value) in self.map.clone().iter() {
            if let Some(frame) = self.queue.get(*value) {
                if !frame.flag {
                    self.remove(key);
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

}

impl Replacer for ClockReplacer {
    fn victim(&mut self, frame_id: u32) -> Result<bool> {
        if let Some(frame) = self.find_mut(&frame_id) {
            frame.flag = true;
            return Ok(true);
        } else if !self.try_remove()? {
            return Ok(false);
        }

        if self.size() < self.size {
            self.add(frame_id)?;
        }
        Ok(false)
    }

    fn pin(&mut self, frame_id: &u32) -> Result<()> {
        if let Some(frame) = self.find_mut(frame_id) {
            frame.ref_count += 1;
        }
        Ok(())
    }

    fn un_pin(&mut self, frame_id: &u32) -> Result<()> {
        if let Some(frame) = self.find_mut(frame_id) {
            frame.ref_count -= 1;
        }
        Ok(())
    }

    fn size(&mut self) -> usize {
        self.map.len()
    }
}
