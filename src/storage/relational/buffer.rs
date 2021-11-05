use crate::error::Result;


// ///
// pub trait BufferPoolManager {
//     /// 从缓冲池中获取请求的页面。
//     fn fetch_page(&mut self, page_id: &u32) -> Result<ToyPage>;
//
//     /// 从缓冲池中取消锁定目标页面。
//     /// is_dirty 如果页面应该被标记为脏，则为 true，否则为 false
//     fn unpin_page(&mut self, page_id: &u32, is_dirty: bool) -> Result<bool>;
//
//     /// 将对应的页面刷入磁盘
//     fn flush_page(&mut self, page_id: &u32) -> Result<bool>;
//
//     /// 在缓冲池中创建一个新页面。
//     fn new_page(&mut self, page_id: u32) -> Result<ToyPage>;
//
//     /// 从缓冲池中删除一个页面。
//     fn delete_page(&mut self, page_id: u32) -> Result<bool>;
//
//     /// 将缓冲池中的所有页面刷新到磁盘。
//     fn flush_all_page(&mut self) -> Result<()>;
// }
