use crate::mem_alloc;
use crate::run_ft_system;

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

fn main() {
    let pi_id: u8 = 0;
    let min_sys_size: u8 = 1;
    let sys_size: u8 = 2;
    let timeout_ms: u16 = 0;
    let port: String = "3841".to_string();
    run_ft_system::system_run(
        pi_id,
        port,
        sys_size,
        min_sys_size,
        timeout_ms,
        critical_task,
    );
}
