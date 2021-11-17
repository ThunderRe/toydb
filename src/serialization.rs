pub trait ToVecAndByVec<T> {
    fn to_vec(t: &T) -> Vec<u8>;
    fn by_vec(data: &Vec<u8>) -> Option<T>;
}