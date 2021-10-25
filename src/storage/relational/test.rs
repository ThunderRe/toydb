use super::clock_replacer::ClockReplacer;
use crate::error::Result;
use crate::storage::relational::clock_replacer::Replacer;

#[derive(Clone, Debug)]
struct FrameTest {
    flag: bool,
    page_id: u32,
}

#[test]
fn check_replacer_test() -> Result<()> {
    let mut clock_replacer = ClockReplacer::new(3)?;
    let list = vec![2, 3, 2, 1, 5, 2, 4, 5, 3, 2, 5, 2];
    for page_id in list {
        clock_replacer.victim(page_id as u32);

        if page_id == 4 {
            clock_replacer.pin(&4);
        }
    }
    assert_eq!(clock_replacer.size(), 3);
    Ok(())
}
