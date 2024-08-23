#![feature(btree_cursors)]

pub mod buffer_manager;
pub mod fixed;
pub mod lsm_tree;
pub mod slotted_page;
pub mod storage_engine;

use lsm_tree::LSMTree;

use buffer_manager::BufferManager;

const BLOCK_SIZE: usize = 4096;

fn main() {
    let avail_mem = usize::pow(2, 24);
    let num_blocks = avail_mem / BLOCK_SIZE;
    let mut manager: BufferManager = buffer_manager::BufferManager::new(num_blocks);

    let mut l: LSMTree<u128, u128> = LSMTree::new("thing".to_string(), &mut manager);

    for i in 0u128..1000000u128 {
        l.put(&mut manager, i, Some(i + 1));
        match l.get(&mut manager, i) {
            None => {
                panic!();
            }
            Some(x) => {
                assert_eq!(i + 1, x)
            }
        }
    }

    for i in 0u128..1000000u128 {
        let tmp = l.get(&mut manager, i);
        match tmp {
            Some(x) => {
                if x != i + 1 {
                    println!("Incorrect value get for key {:?}, got {}", i, x);
                    panic!();
                }
            }
            None => {
                panic!("got none for key {}", i);
            }
        }
    }

    l.merge(&mut manager);
    manager.flush();
    println!("done!");
}
