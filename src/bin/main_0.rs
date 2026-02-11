use std::mem;
use swb_fault_tolerance::mem_alloc::MemAlloc;
use swb_fault_tolerance::{config_loader, run_ft_system};
use swb_fault_tolerance::config_loader::{SystemConfig, load_json_config};

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

    let sys_conf : SystemConfig = load_json_config("bin/config_0.json").expect("Failed to load system config.");

    run_ft_system::system_run(
        sys_conf.system_id,
        sys_conf.port,
        sys_conf.system_size,
        sys_conf.min_sys_size,
        sys_conf.timeout_ms,
        critical_task,
    );
}
