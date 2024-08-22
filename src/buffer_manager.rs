use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::{FileExt, OpenOptionsExt},
    rc::Rc,
};

use crate::BLOCK_SIZE;

pub const O_DIRECT: i32 = 0o0040000; // Double check value
pub const O_CREAT: i32 = 0o0000100;

#[derive(Debug)]
pub struct Block {
    pub bytes: Vec<u8>,
    key: (String, usize),
    dirty_bit: bool,
}

// LRU buffer manager
pub struct BufferManager {
    pub num_blocks: usize,
    blocks: VecDeque<Rc<RefCell<Block>>>,
}

impl BufferManager {
    pub fn new(num_blocks: usize) -> Self {
        let v: VecDeque<Rc<RefCell<Block>>> = VecDeque::with_capacity(num_blocks);
        Self {
            num_blocks: num_blocks,
            blocks: v,
        }
    }

    fn renew(self: &mut Self, index: usize) {
        let b = self.blocks.remove(index);
        match b {
            Some(x) => {
                self.blocks.push_front(x);
            }
            None => {}
        }
    }

    fn add(self: &mut Self, b: Rc<RefCell<Block>>) {
        if self.blocks.len() == self.num_blocks {
            // if page is dirty write it out to disk
            let block = self.blocks.pop_back().unwrap();
            let filepath = &block.borrow().key.0;
            let dirty_bit = block.borrow().dirty_bit;

            if dirty_bit {
                let mut fd = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(O_DIRECT)
                    .open(filepath)
                    .unwrap();
                let offset = block.borrow().key.1;
                let block_offset = offset - (offset % BLOCK_SIZE);

                let _n = fd
                    .write_at(&block.borrow().bytes, block_offset as u64)
                    .unwrap();
                fd.flush().unwrap();

                let mut s = Vec::new();
                fd.seek(SeekFrom::Start(0)).unwrap();
                fd.read_to_end(&mut s).unwrap();
            }
        }

        self.blocks.push_front(b);
    }

    pub fn get(self: &mut Self, file: String, offset: usize) -> Option<Rc<RefCell<Block>>> {
        let block_offset = offset - (offset % BLOCK_SIZE);
        match self.blocks.iter().position(|x| {
            return x.as_ref().borrow().key.0 == file.clone()
                && x.as_ref().borrow().key.1 == block_offset;
        }) {
            Some(x) => {
                self.renew(x);
                return Some(self.blocks[0].clone());
            }
            None => {
                // read from disk
                let fd = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .custom_flags(O_DIRECT)
                    .open(&file)
                    .unwrap();

                let len = fd.metadata().unwrap().len();
                if (block_offset + BLOCK_SIZE) as u64 > len {
                    return None;
                }

                let mut buf = [0; BLOCK_SIZE];
                fd.read_at(&mut buf, block_offset as u64).unwrap();

                let new_block = Rc::new(RefCell::new(Block {
                    bytes: buf.to_vec(),
                    key: (file, block_offset),
                    dirty_bit: false,
                }));

                self.add(new_block.clone());

                return Some(new_block);
            }
        }
    }

    pub fn rename(self: &mut Self, from: &String, to: &String) {
        self.blocks
            .retain(|x| x.as_ref().borrow().key.0 != to.clone());

        // TODO: this is probably a map operation
        let thing = self.blocks.iter();
        for t in thing {
            let mut a = t.as_ref().borrow_mut();
            if a.key.0 == from.clone() {
                a.key.0 = to.clone();
            }
        }
    }

    pub fn write(self: &mut Self, file: &String, offset: usize, buf: &Vec<u8>, buf_size: u32) {
        let block_offset = offset - (offset % BLOCK_SIZE);
        self.get(file.clone(), block_offset);

        match self.blocks.iter().position(|x| {
            return x.as_ref().borrow().key == (file.clone(), block_offset);
        }) {
            Some(x) => {
                let mut block = self.blocks[x].as_ref().borrow_mut();
                let in_block_offset = offset % BLOCK_SIZE;

                block.bytes[in_block_offset..in_block_offset + buf_size as usize]
                    .copy_from_slice(buf);
                block.dirty_bit = true;
            }
            None => {
                if block_offset == 0 {
                    // try to create the file
                    // TODO: do this properly
                    let _fd = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .custom_flags(O_DIRECT)
                        .open(&file)
                        .unwrap();
                }
                // create new page in file?
                let new_block = Rc::new(RefCell::new(Block {
                    bytes: buf.clone(),
                    dirty_bit: true,
                    key: (file.clone(), block_offset),
                }));
                self.add(new_block);
            }
        }
    }

    pub fn flush(self: &mut Self) {
        for block_ref in self.blocks.iter_mut() {
            let mut b = block_ref.as_ref().borrow_mut();
            if b.dirty_bit {
                let mut fd = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(O_DIRECT)
                    .open(b.key.0.clone())
                    .unwrap();
                let offset = b.key.1;
                let block_offset = offset - (offset % BLOCK_SIZE);

                let _n = fd.write_at(&b.bytes, block_offset as u64).unwrap();
                fd.flush().unwrap();

                let mut s = Vec::new();
                fd.seek(SeekFrom::Start(0)).unwrap();
                fd.read_to_end(&mut s).unwrap();
            }
            b.dirty_bit = false;
        }
    }
}
