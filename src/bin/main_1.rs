use crate::mem_alloc;
use crate::run_ft_system;

fn main() {
    let pi_id: u8 = 1;
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
        mem_alloc::critical_task,
    );
}
