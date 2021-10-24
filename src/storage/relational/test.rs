use super::clock_replacer::ClockReplacer;
use crate::error::Result;
use crate::storage::relational::clock_replacer::Replacer;
use std::collections::HashMap;

#[derive(Clone, Debug)]
struct FrameTest {
    flag: bool,
    page_id: u32,
}

#[test]
fn test_vec_with_hashmap() -> Result<()> {
    let mut hash_map: HashMap<u32, u32> = HashMap::new();
    let mut queue: Vec<FrameTest> = Vec::new();
    for i in 0..10 {
        let page_id = i as u32;
        let frame = FrameTest { flag: true, page_id };
        queue.push(frame);
        hash_map.insert(page_id, (queue.len() - 1) as u32);
    }
    println!("{:?}", &queue);
    println!("{:?}", &hash_map);

    let queue_index = hash_map.get(&2).unwrap();
    let frame = queue.get_mut(*queue_index as usize).unwrap();
    frame.flag = !frame.flag;
    println!("{:?}", &queue);
    Ok(())
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
    clock_replacer.println_all();
    Ok(())
}
