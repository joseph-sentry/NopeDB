use chrono::{DateTime, Local};

pub trait KnowsSize {
    fn bit_width() -> i16;
}

impl KnowsSize for i8 {
    fn bit_width() -> i16 {
        return 1;
    }
}

impl KnowsSize for i16 {
    fn bit_width() -> i16 {
        return 2;
    }
}

impl KnowsSize for i32 {
    fn bit_width() -> i16 {
        return 4;
    }
}

impl KnowsSize for i64 {
    fn bit_width() -> i16 {
        return 8;
    }
}

impl KnowsSize for i128 {
    fn bit_width() -> i16 {
        return 16;
    }
}

impl KnowsSize for u8 {
    fn bit_width() -> i16 {
        return 1;
    }
}

impl KnowsSize for u16 {
    fn bit_width() -> i16 {
        return 2;
    }
}

impl KnowsSize for u32 {
    fn bit_width() -> i16 {
        return 4;
    }
}

impl KnowsSize for u64 {
    fn bit_width() -> i16 {
        return 8;
    }
}

impl KnowsSize for u128 {
    fn bit_width() -> i16 {
        return 16;
    }
}

impl KnowsSize for DateTime<Local> {
    fn bit_width() -> i16 {
        return 8;
    }
}

impl KnowsSize for String {
    fn bit_width() -> i16 {
        return -1;
    }
}
