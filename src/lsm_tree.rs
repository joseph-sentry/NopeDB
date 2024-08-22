use std::{
    collections::{btree_map::IntoIter, BTreeMap},
    fmt::Debug,
    fs::{create_dir, remove_file, rename},
    ops::Bound,
};

use serde::{Deserialize, Serialize};

use crate::{
    buffer_manager::BufferManager,
    fixed::KnowsSize,
    slotted_page::{decode, encode, SlottedPage},
    BLOCK_SIZE,
};

pub struct LSMTree<K, V> {
    memtable: BTreeMap<K, Option<V>>,
    memtable_size: usize,
    disktable: String,
    disktable_index: BTreeMap<K, usize>,
    merge_count: usize,
}

impl<
        K: Serialize + for<'a> Deserialize<'a> + Ord + Clone + KnowsSize + Debug,
        V: Serialize + for<'a> Deserialize<'a> + Clone + KnowsSize + Debug,
    > LSMTree<K, V>
{
    pub fn new(name: String, manager: &mut BufferManager) -> Self {
        let filepath = format!("disktables/{}", name);
        match create_dir("disktables") {
            Err(_) => {}
            Ok(()) => {}
        }
        let mut s = Self {
            memtable: BTreeMap::new(),
            disktable: filepath,
            memtable_size: 0,
            disktable_index: BTreeMap::new(),
            merge_count: 0,
        };

        s.build_index(manager);
        s
    }

    pub fn put(self: &mut Self, manager: &mut BufferManager, k: K, v: Option<V>) {
        let encoded_k = bincode::serialize(&k).unwrap();
        let key_size = encoded_k.len();

        let encoded_v = bincode::serialize(&v).unwrap();
        let val_size = encoded_v.len();

        let res = self.memtable.insert(k, v);
        match res {
            None => {}
            Some(x) => {
                let old_val_size = bincode::serialize(&x).unwrap();
                self.memtable_size -= old_val_size.len();
            }
        }

        self.memtable_size += key_size + val_size;

        if self.memtable_size > manager.num_blocks * 2048 {
            self.merge(manager);
            self.build_index(manager);
        }
    }

    fn build_index(self: &mut Self, manager: &mut BufferManager) {
        let mut offset = 0;

        while let Some(s) = self.get_page(&self.disktable, manager, offset) {
            let Some((k, _)) = s.cells.first_key_value() else {
                return;
            };
            self.disktable_index.insert(k.clone(), offset);
            offset += BLOCK_SIZE;
        }
    }

    pub fn get(self: &Self, manager: &mut BufferManager, k: K) -> Option<V> {
        if let Some(x) = self.memtable.get(&k) {
            return x.clone();
        }

        let mut c = self.disktable_index.upper_bound(Bound::Included(&k));
        let prev = c.prev().unwrap();
        let block_offset: usize = *prev.1;

        let Some(block) = manager.get(self.disktable.clone(), block_offset) else {
            return None;
        };
        let b = block.as_ref().borrow();
        let s: SlottedPage<K, V> = decode(&b.bytes);
        let v = s.cells.get(&k);

        match v {
            Some(Some(x)) => Some(x.clone()),
            Some(None) => None,
            None => None,
        }
    }

    pub fn get_page(
        self: &Self,
        file: &String,
        manager: &mut BufferManager,
        offset: usize,
    ) -> Option<SlottedPage<K, V>> {
        let block_option = manager.get(file.clone(), offset);
        if let None = block_option {
            return None;
        }

        match block_option {
            None => None,
            Some(block) => {
                let block_bytes = &block.as_ref().borrow().bytes;
                let page = decode(block_bytes);
                return Some(page);
            }
        }
    }

    fn get_next_disk(
        self: &Self,
        manager: &mut BufferManager,
        iter_option: Option<IntoIter<K, Option<V>>>,
        mut offset: usize,
    ) -> Option<((K, Option<V>), IntoIter<K, Option<V>>, usize)> {
        match iter_option {
            Some(mut iter) => match iter.next() {
                None => {
                    offset += BLOCK_SIZE;
                    let Some(page) = self.get_page(&self.disktable, manager, offset) else {
                        return None;
                    };
                    iter = page.cells.into_iter();
                    let Some(x) = iter.next() else {
                        return None;
                    };
                    Some((x, iter, offset))
                }
                Some(x) => Some((x, iter, offset)),
            },
            None => {
                let Some(page) = self.get_page(&self.disktable, manager, 0) else {
                    return None;
                };
                let mut iter = page.cells.into_iter();
                let Some(x) = iter.next() else {
                    return None;
                };
                Some((x, iter, 0))
            }
        }
    }

    fn write_btreemap_to_disk(
        self: &Self,
        manager: &mut BufferManager,
        mut btreemap_iter: IntoIter<K, Option<V>>,
    ) {
        let tmpfilepath = format!("{}_merge", self.disktable);

        let mut curr_s: SlottedPage<K, V> = SlottedPage::new();
        let mut offset: usize = 0;
        while let Some((k, v)) = btreemap_iter.next() {
            let res = curr_s.add_cell(k, v);
            match res {
                Err((k, v)) => {
                    let encoded_page = encode(&curr_s);
                    manager.write(&tmpfilepath, offset, &encoded_page, BLOCK_SIZE as u32);
                    offset += BLOCK_SIZE;
                    curr_s = SlottedPage::new();
                    match curr_s.add_cell(k, v) {
                        Err((k, v)) => {
                            panic!("Error add cell for values  {:?}, {:?}", k, v);
                        }
                        Ok(()) => {}
                    };
                }
                Ok(()) => {}
            };
        }
        if curr_s.num_cells > 0 {
            let encoded_page = encode(&curr_s);
            manager.write(&tmpfilepath, offset, &encoded_page, BLOCK_SIZE as u32);
        }
        remove_file(&self.disktable).unwrap();
        rename(&tmpfilepath, &self.disktable).unwrap();
        manager.rename(&tmpfilepath, &self.disktable);

        return;
    }

    pub fn merge(self: &mut Self, manager: &mut BufferManager) {
        self.merge_count += 1;
        let mut merged_btree = BTreeMap::new();

        let old_memtable = self.memtable.clone();
        let mut memtable_iter = old_memtable.clone().into_iter();
        self.memtable = BTreeMap::new();
        self.memtable_size = 0;

        let mut disktable_iter: IntoIter<K, Option<V>>;

        let mut curr_offset = 0;

        let mut fetch_mem = false;
        let mut fetch_disk = false;
        let mut curr_disk;

        (curr_disk, disktable_iter, curr_offset) =
            match self.get_next_disk(manager, None, curr_offset) {
                None => {
                    self.write_btreemap_to_disk(manager, memtable_iter);
                    return;
                }
                Some(x) => x,
            };

        let mut curr_mem = memtable_iter.next().unwrap();
        'outer: loop {
            if fetch_mem {
                let Some(next_mem) = memtable_iter.next() else {
                    if fetch_disk {
                        let Some((d, i, o)) =
                            self.get_next_disk(manager, Some(disktable_iter), curr_offset)
                        else {
                            break 'outer;
                        };
                        curr_disk = d;
                        disktable_iter = i;
                        curr_offset = o;
                    }
                    loop {
                        merged_btree.insert(curr_disk.clone().0, curr_disk.clone().1);
                        let Some((d, i, o)) =
                            self.get_next_disk(manager, Some(disktable_iter), curr_offset)
                        else {
                            break 'outer;
                        };
                        curr_disk = d;
                        disktable_iter = i;
                        curr_offset = o;
                    }
                };
                curr_mem = next_mem;
            }
            if fetch_disk {
                match self.get_next_disk(manager, Some(disktable_iter), curr_offset) {
                    Some((d, i, o)) => {
                        curr_disk = d;
                        disktable_iter = i;
                        curr_offset = o;
                    }
                    None => loop {
                        merged_btree.insert(curr_mem.clone().0, curr_mem.clone().1);
                        let Some(next_mem) = memtable_iter.next() else {
                            break 'outer;
                        };
                        curr_mem = next_mem;
                    },
                };
            }

            let mem_key = curr_mem.clone().0;
            let disk_key = curr_disk.clone().0;

            if mem_key == disk_key {
                merged_btree.insert(curr_mem.clone().0, curr_mem.clone().1);
                fetch_mem = true;
                fetch_disk = true;
            } else if mem_key < disk_key {
                merged_btree.insert(curr_mem.clone().0, curr_mem.clone().1);
                fetch_mem = true;
                fetch_disk = false;
            } else {
                merged_btree.insert(curr_disk.clone().0, curr_disk.clone().1);
                fetch_mem = false;
                fetch_disk = true;
            }
        }

        let merged_iter = merged_btree.into_iter();

        self.write_btreemap_to_disk(manager, merged_iter);
        return;
    }
}
