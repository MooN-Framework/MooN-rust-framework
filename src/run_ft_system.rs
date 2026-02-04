use std::collections::HashMap;
use std::time::{Duration, Instant};
use crate::state_machine::StateMachine;
use crate::net::udp_com::{SystemUdpMessage};
use crate::sys_run_info::{SystemConfigData, SystemCycleData};
use crate::mem_alloc::MemAlloc;


const SEND_CYCLE_DURATION : Duration = Duration::from_secs(2);

fn try_add_id(received_ids: &mut Vec<u8>, new_id: &u8) -> bool {
    if !received_ids.contains(new_id) {
        received_ids.push(*new_id);
        println!("Added ID: {}", new_id);
        true
    } else {
        println!("Duplicate ID: {}", new_id);
        false
    }
}

fn startup() -> bool
{
    /* Do some dummy calculation to check if device is working correct */
    const HEALTH_CHECK_VAL : u8 = 0x10;
    const HEALTH_CHECK_CMP_VAL : u8 = 0x20;
    let mut test_variable : u8 = 0;
    test_variable += HEALTH_CHECK_VAL + HEALTH_CHECK_VAL;
    if test_variable == HEALTH_CHECK_CMP_VAL
    {
        true
    }else{
        false
    }
}

fn sys_send_receive_loop(sys_config : &SystemConfigData, sys_cycle : &mut SystemCycleData, sender_value : u32) -> bool
{
    let mut last_send = Instant::now();
    let mut connection_counter = 0;
    try_add_id(&mut sys_cycle.curr_system_ids, &sys_config.system_id);
    let timeout_start = Instant::now();
    loop {
        if sys_config.system_timeout != 0
            && timeout_start.elapsed() >= Duration::from_millis(sys_config.system_timeout.into())
        {
            return false;
        }

        if last_send.elapsed() >= SEND_CYCLE_DURATION {
            let send_msg: SystemUdpMessage =
                SystemUdpMessage::new(sys_config.system_id, sys_config.state.get(), sender_value);
            last_send = sys_config
                .udp_socket_info
                .send_udp_message(send_msg)
                .expect("Couldn't sent udp message.");
            if connection_counter >= (sys_config.sys_size.get() - 1) {
                return true;
            }
        }

        if let Some(rec_msg) = sys_config.udp_socket_info.receive_udp_message()
            && rec_msg.sender_state == sys_config.state.get()
            && try_add_id(&mut sys_cycle.curr_system_ids, &rec_msg.sender_id)
        {
            connection_counter += 1;
        }
    }
}

fn initial_synchronization(sys_config : &SystemConfigData, sys_cycle : &mut SystemCycleData) {
    let state_result = sys_send_receive_loop(sys_config, sys_cycle, 0);

}

fn voter_fetch(sys_info: &mut SystemInformation) -> bool {
    let mut last_send = Instant::now();
    let mut fetched_counter: u8 = 0;
    sys_info
        .curr_fetched_crcs
        .insert(sys_info.system_id, sys_info.curr_crc);
    let timeout_start = Instant::now();
    loop {
        if sys_info.system_timeout != 0
            && timeout_start.elapsed() >= Duration::from_millis(sys_info.system_timeout.into())
        {
            return true;
        }

        if last_send.elapsed() >= SEND_CYCLE_DURATION {
            let send_msg: SystemUdpMessage =
                SystemUdpMessage::new(sys_info.system_id, sys_info.state, sys_info.curr_crc);
            last_send = sys_info
                .udp_socket_info
                .send_udp_message(send_msg)
                .expect("Couldn't sent udp message.");
            if fetched_counter >= sys_info.curr_sys_size - 1 {
                return false;
            }
        }

        if let Some(rec_msg) = sys_info.udp_socket_info.receive_upd_message()
            && rec_msg.sender_state == StateMachine::VoterFetch
            && !sys_info.curr_fetched_crcs.contains_key(&rec_msg.sender_id)
        {
            sys_info
                .curr_fetched_crcs
                .insert(rec_msg.sender_id, rec_msg.sender_value);
            fetched_counter += 1;
        }
    }
}

fn vote_on_crc32(sys_info: &SystemInformation) -> Option<u32> {
    let total = sys_info.curr_fetched_crcs.len();
    if total == 0 {
        return None;
    }

    let mut counts: HashMap<u32, usize> = HashMap::new();

    for &crc in sys_info.curr_fetched_crcs.values() {
        *counts.entry(crc).or_insert(0) += 1;
    }

    counts
        .into_iter()
        .max_by_key(|&(_, count)| count)
        .and_then(|(crc, count)| {
            if count > total / 2 {
                println!("PI{}: Voted {:X}", sys_info.system_id, crc);
                Some(crc)
            } else {
                None
            }
        })
}

fn decide_on_vote_publisher(sys_info: &SystemInformation) -> u8 {
    let mut curr_main_cpu: u8 = u8::MAX;
    for &system_id in sys_info.curr_system_ids.iter() {
        if system_id < curr_main_cpu {
            println!(
                "CURRENT SYSTEMD ID: {}; CURRENT_MAIN_CPU{}; Current systemIdsLen{}",
                system_id,
                curr_main_cpu,
                sys_info.curr_system_ids.len()
            );
            if let Some(crc) = sys_info.curr_fetched_crcs.get(&system_id)
                && *crc == sys_info.curr_voted_crc
            {
                curr_main_cpu = system_id;
            }
        }
    }
    curr_main_cpu
}

fn publish_vote(sys_info: &SystemInformation) {
    if sys_info.system_id == sys_info.curr_publisher {
        println!(
            "PUBLISH_VOTE: IM PI:{}, publishing value.",
            sys_info.system_id
        );
    } else {
        println!(
            "Publisher is {}",
            sys_info.curr_publisher
        );
    }
}

fn cyclic_synchronization(sys_info: &mut SystemInformation) -> bool {
    let mut last_send = Instant::now();
    let mut fetched_counter: u8 = 0;
    sys_info
        .curr_fetched_crcs
        .insert(sys_info.system_id, sys_info.curr_crc);
    let timeout_start = Instant::now();
    loop {
        if sys_info.system_timeout != 0
            && timeout_start.elapsed() >= Duration::from_millis(sys_info.system_timeout.into())
        {
            return true;
        }

        if last_send.elapsed() >= SEND_CYCLE_DURATION {
            let send_msg: SystemUdpMessage =
                SystemUdpMessage::new(sys_info.system_id, sys_info.state, sys_info.curr_crc);
            last_send = sys_info
                .udp_socket_info
                .send_udp_message(send_msg)
                .expect("Couldn't sent udp message.");
            if fetched_counter >= sys_info.curr_sys_size - 1 {
                return false;
            }
        }

        if let Some(rec_msg) = sys_info.udp_socket_info.receive_upd_message()
            && rec_msg.sender_state == StateMachine::ExchangeCRC
            && !sys_info.curr_fetched_crcs.contains_key(&rec_msg.sender_id)
        {
            sys_info
                .curr_fetched_crcs
                .insert(rec_msg.sender_id, rec_msg.sender_value);
            fetched_counter += 1;
        }
    }
}

pub fn system_run(
    sys_id: u8,
    port: String,
    sys_size: u8,
    min_sys_size: u8,
    timeout_ms: u16,
    critical_fn: fn() -> MemAlloc,
) {
    let sys_config : SystemConfigData = SystemConfigData::new(sys_id, timeout_ms, sys_size, min_sys_size, port).expect("");
    let mut sys_cycle : SystemCycleData = SystemCycleData::new();
    let state_success : bool = true;

    loop {
        match sys_config.state.get() {
            StateMachine::Startup =>
            {
                state_success = startup();
                sys_config.state.set(StateMachine::InitialSync);
            }
            StateMachine::InitialSync => {
                initial_synchronization(&sys_config, &mut sys_cycle);
            }
            StateMachine::CycleSync => {
                if cyclic_synchronization(&mut sys_info) {
                    println!("Timeout occured in {}", sys_info.state);
                }
            }
            StateMachine::CalcCritical => {
                sys_info.curr_crit_mem_alloc = critical_fn();
                sys_info.curr_crc = sys_info.curr_crit_mem_alloc.calculate_crc();
            }
            StateMachine::ExchangeCRC => {
                if voter_fetch(&mut sys_info) {
                    println!("Timeout occured in {}", sys_info.state);
                }
            }
            StateMachine::Vote => {
                sys_info.curr_voted_crc = vote_on_crc32(&sys_info).expect("Failed to vote on the CRC32.");
            }
            StateMachine::PublishVote => {
                sys_info.curr_publisher = decide_on_vote_publisher(&sys_info);
                publish_vote(&sys_info);
            }
            StateMachine::ErrorHandling =>
            {
                continue;
            }
            StateMachine::Failsafe => loop {
                std::thread::sleep(Duration::from_secs(2));
                println!("{}: Currently in failsafe.", sys_config.system_id);
            },
        }
    }
}
