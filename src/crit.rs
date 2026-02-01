use crate::ft_pies::MemAlloc;
use std::mem;

pub fn critical_task() -> MemAlloc {
    println!("Starting critical task..");
    let mem_alloc: MemAlloc = MemAlloc::new(mem::size_of::<u32>()).expect("");
    let value: u32 = 0xAAAAAAAA;
    mem_alloc
        .write(&value.to_be_bytes())
        .expect("Failed write to allocated memory.");
    println!("Finished critical task!");
    mem_alloc
}
