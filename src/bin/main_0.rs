use swbft::crit;
use swbft::ft_pies;
fn main() {
    let pi_id: u8 = 0;
    let min_sys_size: u8 = 1;
    let sys_size: u8 = 2;
    let timeout_ms: u16 = 0;
    let port: String = "3841".to_string();
    ft_pies::system_run(
        pi_id,
        port,
        sys_size,
        min_sys_size,
        timeout_ms,
        crit::critical_task,
    );
}
