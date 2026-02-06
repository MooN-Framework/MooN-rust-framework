use std::mem;
use swb_fault_tolerance::mem_alloc::MemAlloc;
use swb_fault_tolerance::run_ft_system;

pub fn critical_task() -> MemAlloc {
    let mem_alloc: MemAlloc = MemAlloc::new(mem::size_of::<u32>()).expect("");
    let value: u32 = 0xAAAAAAAA;
    mem_alloc
        .write(&value.to_be_bytes())
        .expect("Failed write to allocated memory.");
    mem_alloc
}

fn main() {
    let pi_id: u8 = 0;
    let min_sys_size: u8 = 2;
    let sys_size: u8 = 3;
    let timeout_ms: u16 = 10000;
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
