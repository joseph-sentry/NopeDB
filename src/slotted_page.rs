use std::fmt;
use std::{collections::BTreeMap, fmt::Debug};

use crate::fixed::KnowsSize;
use crate::BLOCK_SIZE;
use chrono::{DateTime, Local};
use serde::{
    de::{Error, Visitor},
    Deserialize, Serialize,
};

const PAGE_TYPE_MASK: u16 = 0b1000000000000000;
const NUM_CELLS_MASK: u16 = 0b0111111111111111;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum PageType {
    Variable,
    Fixed,
}

pub struct SlottedPage<K, V> {
    page_type: PageType,
    pub num_cells: u16,
    pub cells: BTreeMap<K, Option<V>>,
    space_left: u32, // assume that it's variable size (bad), each keyval is u16 + len(serialized(key)) + len(serialized(val))
}

impl<K: Serialize + KnowsSize + Ord, V: Serialize + KnowsSize> SlottedPage<K, V> {
    pub fn new() -> Self {
        let key_bit_width = K::bit_width();
        let val_bit_width = V::bit_width() + 1;
        let mut page_type = PageType::Fixed;

        if val_bit_width < 0 || key_bit_width < 0 {
            page_type = PageType::Variable;
        };
        Self {
            page_type: page_type,
            num_cells: 0,
            cells: BTreeMap::new(),
            space_left: BLOCK_SIZE as u32,
        }
    }

    pub fn add_cell(self: &mut Self, k: K, v: Option<V>) -> Result<(), (K, Option<V>)> {
        let space_this_will_take: usize;
        match self.page_type {
            PageType::Fixed => {
                let key_bit_width = K::bit_width();
                let val_bit_width = V::bit_width() + 1; // because of option
                space_this_will_take = 16 + key_bit_width as usize + val_bit_width as usize;
                // guaranteed to be above 0
            }
            PageType::Variable => {
                let encoded_key = bincode::serialize(&k).unwrap();
                let encoded_val = bincode::serialize(&v).unwrap();
                space_this_will_take = 16 + encoded_key.len() + encoded_val.len();
            }
        }
        if space_this_will_take > self.space_left as usize {
            return Err((k, v));
        }
        self.num_cells += 1;
        self.space_left = self.space_left - space_this_will_take as u32;
        self.cells.insert(k, v);
        Ok(())
    }
}

struct MyOwnDateTime {
    whatever: DateTime<Local>,
}

impl Serialize for MyOwnDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.whatever.timestamp();
        serializer.serialize_i64(self.whatever.timestamp())
    }
}
struct TimeStampVisitor;

impl<'de> Visitor<'de> for TimeStampVisitor {
    type Value = MyOwnDateTime;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an integer between -2^31 and 2^31")
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(MyOwnDateTime {
            whatever: DateTime::from_timestamp(i64::from(value), 0)
                .unwrap()
                .into(),
        })
    }
}

impl<'de> Deserialize<'de> for MyOwnDateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(TimeStampVisitor)
    }
}

/*
Fixed Header format:
| is_variable | num_cells |     key size     |      val size     |
    1 bit        15 bits            2 bytes         2 bytes
Variable Header format:
|  is_variable  |    num_cells    |
    1 bit             15 bits
Slotted page Format:
| header u8 | offset of cell 1 u8 | offset of cell 2 u8 | ... | offset cell x u8| free space | cell x | cell x - 1 | ... | cell 1 |
*/

pub fn encode<K: Serialize + KnowsSize + Debug, V: Serialize + KnowsSize>(
    page: &SlottedPage<K, V>,
) -> Vec<u8> {
    if page.num_cells > u16::pow(2, 15) - 1 {
        panic!("More cells than a 15 bits can represent, this shouldn't ever happen but if it does it's bad {}", page.num_cells);
    }

    let mut encoded_header: Vec<u8> = Vec::new();
    match page.page_type {
        PageType::Fixed => {
            let page_type_bool: u16;
            page_type_bool = 0;
            let num = page_type_bool | page.num_cells;

            encoded_header.extend(bincode::serialize(&num).unwrap());
            encoded_header.extend(bincode::serialize(&K::bit_width()).unwrap());
            encoded_header.extend(bincode::serialize(&(V::bit_width() + 1)).unwrap());
        }
        PageType::Variable => {
            let page_type_bool: u16;
            page_type_bool = PAGE_TYPE_MASK;
            let num = page_type_bool | page.num_cells;
            encoded_header.extend(bincode::serialize(&num).unwrap());
        }
    };

    let mut offsets: Vec<u16> = Vec::new();
    let mut key_vals: Vec<Vec<u8>> = Vec::new();
    match page.page_type {
        PageType::Fixed => {
            let mut offset: u16 = 0;
            for k in page.cells.iter() {
                let mut serialized_key = bincode::serialize(k.0).unwrap();
                let serialized_val = bincode::serialize(k.1).unwrap();
                serialized_key.extend(serialized_val);

                offset += serialized_key.len() as u16;
                offsets.push(offset);
                key_vals.push(serialized_key);
            }

            let mut final_arr = [0; BLOCK_SIZE];
            final_arr[0..6].copy_from_slice(&encoded_header);
            for (i, v) in offsets.iter().enumerate() {
                let offset_start = 6 + i * 2;
                let offset_end = offset_start + 2;
                final_arr[offset_start..offset_end]
                    .copy_from_slice(&bincode::serialize(&v).unwrap());
            }
            for (i, v) in key_vals.iter().enumerate() {
                let cell_start = BLOCK_SIZE as u16 - offsets[i];
                let cell_end = cell_start + v.len() as u16;
                final_arr[cell_start as usize..cell_end as usize].copy_from_slice(v);
            }
            final_arr.to_vec()
        }
        PageType::Variable => {
            let mut offset: u16 = 0;
            for k in page.cells.iter() {
                offsets.push(offset);

                let mut serialized_cell = Vec::new();

                let serialized_key = bincode::serialize(k.0).unwrap();
                let serialized_key_len = bincode::serialize(&serialized_key.len()).unwrap();

                let serialized_val = bincode::serialize(k.1).unwrap();
                let serialized_val_len = bincode::serialize(&serialized_key.len()).unwrap();

                serialized_cell.extend(serialized_key_len);
                serialized_cell.extend(serialized_key);
                serialized_cell.extend(serialized_val_len);
                serialized_cell.extend(serialized_val);

                offset += serialized_cell.len() as u16;
                key_vals.push(serialized_cell);
            }

            let mut final_arr = [0; BLOCK_SIZE];
            final_arr[0..16].copy_from_slice(&encoded_header);
            for (i, v) in offsets.iter().enumerate() {
                let offset_start = 2 + (i * 2);
                let offset_end = offset_start + 2;
                final_arr[offset_start..offset_end]
                    .copy_from_slice(&bincode::serialize(&v).unwrap());
            }
            for (i, v) in key_vals.iter().enumerate() {
                let cell_start = BLOCK_SIZE as u16 - offsets[i];
                let cell_end = cell_start + v.len() as u16;
                final_arr[cell_start as usize..cell_end as usize].copy_from_slice(v);
            }

            final_arr.to_vec()
        }
    }
}

pub fn decode<K: Ord + for<'a> Deserialize<'a> + Debug, V: for<'a> Deserialize<'a> + Debug>(
    buf: &Vec<u8>,
) -> SlottedPage<K, V> {
    let packed_header: u16 = bincode::deserialize(&buf[..2]).unwrap();
    let page_type = packed_header & PAGE_TYPE_MASK;
    let page_type_enum: PageType;
    let space_left: u32;
    if page_type > 0 {
        page_type_enum = PageType::Variable;
        space_left = 4094;
    } else {
        page_type_enum = PageType::Fixed;
        space_left = 4090;
    }

    let num_cells = packed_header & NUM_CELLS_MASK;

    let mut s: SlottedPage<K, V> = SlottedPage {
        num_cells: num_cells,
        page_type: page_type_enum,
        cells: BTreeMap::new(),
        space_left: space_left,
    };
    match s.page_type {
        PageType::Fixed => {
            let key_size: u16 = bincode::deserialize(&buf[2..4]).unwrap();
            let val_size: u16 = bincode::deserialize(&buf[4..6]).unwrap();

            for i in 0..s.num_cells {
                let offset_start = (6 + i * 2) as usize;
                let offset_end = (offset_start + 2) as usize;
                let offset: u16 = bincode::deserialize(&buf[offset_start..offset_end]).unwrap();

                let key_start = BLOCK_SIZE as u16 - offset;
                let key_end = key_start + key_size;

                let val_start = key_end;
                let val_end = val_start + val_size;

                let key: K =
                    bincode::deserialize(&buf[key_start as usize..key_end as usize]).unwrap();
                let value: Option<V> =
                    bincode::deserialize(&buf[val_start as usize..val_end as usize]).unwrap();

                s.cells.insert(key, value);

                s.num_cells += 1;
                s.space_left = s.space_left - 2 - key_size as u32 - val_size as u32
            }
        }
        PageType::Variable => {
            for i in 0..s.num_cells {
                let offset_start = (2 + i * 2) as usize;
                let offset_end = (offset_start + 2) as usize;
                let offset: u16 = bincode::deserialize(&buf[offset_start..offset_end]).unwrap();
                let key_size_start = BLOCK_SIZE - offset as usize;
                let key_size_end = (offset + 2) as usize;
                let key_size: u16 =
                    bincode::deserialize(&buf[key_size_start..key_size_end]).unwrap();

                let key_start = key_size_end;
                let key_end = key_start + key_size as usize;

                let key: K = bincode::deserialize(&buf[key_start..key_end]).unwrap();

                let val_size_start = key_end as usize;
                let val_size_end = (key_end + 2) as usize;
                let val_size: u16 =
                    bincode::deserialize(&buf[val_size_start..val_size_end]).unwrap();

                let val_start = val_size_end;
                let val_end = val_size_end + val_size as usize;

                let val: Option<V> = bincode::deserialize(&buf[val_start..val_end]).unwrap();

                s.cells.insert(key, val);

                s.num_cells += 1;
                s.space_left = s.space_left - 2 - key_size as u32 - val_size as u32
            }
        }
    }
    s
}
