use crate::error::Result;

/// ClockReplacer 实现了时钟替换策略，它近似于最近最少使用的策略
pub trait ClockReplacer {
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
    fn size(&mut self) -> Result<u64>;
}
