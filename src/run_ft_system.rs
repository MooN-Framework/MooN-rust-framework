use crate::mem_alloc::MemAlloc;
use crate::net::udp_com::SystemUdpMessage;
use crate::state_machine::StateMachine;
use crate::sys_run_info::{SystemHealthData, SystemRunnerData};
use log::{debug, error, info, warn};
use simple_logger;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const SEND_CYCLE_DURATION: Duration = Duration::from_secs(2);

fn startup() -> bool {
    /* Do some dummy calculation to check if device is working correct */
    const HEALTH_CHECK_VAL: u8 = 0x10;
    const HEALTH_CHECK_CMP_VAL: u8 = 0x20;
    let mut test_variable: u8 = 0;
    test_variable += HEALTH_CHECK_VAL + HEALTH_CHECK_VAL;
    test_variable == HEALTH_CHECK_CMP_VAL
}

fn sys_rec_gather_participants(
    sys_runner: &SystemRunnerData,
    sys_health: &mut SystemHealthData,
    connection_counter: &mut u8,
) -> u8 {
    if let Some(rec_msg) = sys_runner.udp_socket_info.receive_udp_message()
        && rec_msg.sender_state == sys_runner.state
        && sys_health.try_add_participant(rec_msg.sender_id)
    {
        *connection_counter += 1;
    }
    *connection_counter
}

fn sys_rec_sys_msg_participants(
    sys_runner: &SystemRunnerData,
    sys_health: &mut SystemHealthData,
    connection_counter: &mut u8,
) -> u8 {
    if let Some(rec_msg) = sys_runner.udp_socket_info.receive_udp_message()
        && rec_msg.sender_state == sys_runner.state
        && sys_health.is_id_participant(rec_msg.sender_id)
        && !sys_health.sys_checklist.contains_key(&rec_msg.sender_id)
    {
        sys_health
            .sys_checklist
            .insert(rec_msg.sender_id, rec_msg.sender_value);
        *connection_counter += 1;
    }
    *connection_counter
}

fn sys_send_receive_loop(
    sys_runner: &SystemRunnerData,
    sys_health: &mut SystemHealthData,
    sender_value: u32,
    receiv_fn: fn(&SystemRunnerData, &mut SystemHealthData, &mut u8) -> u8,
) -> bool {
    let mut last_send = Instant::now();
    let mut connection_counter = 0;
    let timeout_start = Instant::now();
    sys_health.reset_sys_checklist(sys_runner, sender_value);
    loop {
        if sys_runner.system_timeout != 0
            && timeout_start.elapsed() >= Duration::from_millis(sys_runner.system_timeout.into())
        {
            warn!("Timeout triggered in {},", sys_runner.state);
            for sys_id in sys_health.sys_participants.iter() {
                if !sys_health.sys_checklist.contains_key(sys_id) {
                    sys_health.sys_fault_set.insert(*sys_id);
                }
            }
            return false;
        }

        if last_send.elapsed() >= SEND_CYCLE_DURATION {
            let send_msg: SystemUdpMessage =
                SystemUdpMessage::new(sys_runner.system_id, sys_runner.state, sender_value);
            last_send = sys_runner
                .udp_socket_info
                .send_udp_message(send_msg)
                .expect("Couldn't sent udp message.");
            if connection_counter == (sys_health.curr_sys_size - 1) {
                return true;
            }
        }

        connection_counter = receiv_fn(sys_runner, sys_health, &mut connection_counter);
    }
}

fn initial_synchronization(
    sys_config: &SystemRunnerData,
    sys_health: &mut SystemHealthData,
) -> bool {
    sys_send_receive_loop(sys_config, sys_health, 0, sys_rec_gather_participants)
}

fn exchange_crc(sys_runner: &SystemRunnerData, sys_health: &mut SystemHealthData) -> bool {
    sys_send_receive_loop(
        sys_runner,
        sys_health,
        sys_runner.sys_cycle.crc,
        sys_rec_sys_msg_participants,
    )
}

fn vote_on_crc32(sys_health: &mut SystemHealthData) -> Option<u32> {
    let total = sys_health.sys_checklist.len();
    if total == 0 {
        return None;
    }

    let mut counts: HashMap<u32, usize> = HashMap::new();

    for &crc in sys_health.sys_checklist.values() {
        *counts.entry(crc).or_insert(0) += 1;
    }

    counts
        .into_iter()
        .max_by_key(|&(_, count)| count)
        .and_then(|(crc, count)| {
            if count > total / 2 {
                Some(crc)
            } else {
                None
            }
        })
}

fn decide_on_vote_publisher(sys_runner: &SystemRunnerData, sys_health: &SystemHealthData) -> u8 {
    let mut curr_main_cpu: u8 = u8::MAX;
    for &system_id in sys_health.sys_participants.iter() {
        if system_id < curr_main_cpu
            && let Some(crc) = sys_health.sys_checklist.get(&system_id)
            && *crc == sys_runner.sys_cycle.voted_crc
        {
            curr_main_cpu = system_id;
        }
    }
    curr_main_cpu
}

fn exchange_vote(sys_runner: &SystemRunnerData, sys_health: &mut SystemHealthData) -> bool
{
    let state_success = sys_send_receive_loop(
        sys_runner,
        sys_health,
        sys_runner.sys_cycle.publisher.into(),
        sys_rec_sys_msg_participants,
    );

    if !state_success {
        return false;
    }

    let mut counter: HashMap<u32, usize> = HashMap::new();
    for &publisher in sys_health.sys_checklist.values() {
        *counter.entry(publisher).or_insert(0) += 1;
    }

    let majority = counter
        .iter()
        .max_by_key(|(_k, v)| *v)
        .map(|(k, v)| (*k, *v));

    let (major_id, major_count) = match majority {
        None => 
        {
            sys_health.sys_fault_set.insert(sys_runner.system_id);
            return false;
        }
        Some(x) => x,
    };

    debug!("Node:{} MajorID:{} MajorCount:{}",sys_runner.system_id, major_id, major_count);

    if major_count < sys_health.min_sys_size.into() {
        for id in sys_health.sys_participants.iter()
        {
            sys_health.sys_fault_set.insert(*id);
        }
        return false;
    }

    for (&id, &value) in sys_health.sys_checklist.iter()
    {
        if value != major_id
        {
            sys_health.sys_fault_set.insert(id);
        }

        if !sys_health.sys_fault_set.is_empty()
        {
            return false;
        }
    }
    true
}

fn publish_vote(sys_runner: &SystemRunnerData) {
    if sys_runner.system_id == sys_runner.sys_cycle.publisher {
        info!("I am the publisher.");
    } else {
        info!("Publisher is node {}", sys_runner.sys_cycle.publisher);
    }
}

fn cyclic_synchronization(
    sys_runner: &SystemRunnerData,
    sys_health: &mut SystemHealthData,
) -> bool {
    sys_send_receive_loop(sys_runner, sys_health, 0, sys_rec_sys_msg_participants)
}

fn error_handling(sys_runner: &SystemRunnerData, sys_health: &mut SystemHealthData) -> bool {
    if sys_health.check_self_fault(sys_runner) {
        error!(
            "Fault was detected on device SYS_ID:{}",
            sys_runner.system_id
        );
        return false;
    }

    if !sys_health.sys_fault_set.is_empty() {
        for sys_id in sys_health.sys_fault_set.iter() {
            info!("Removing participant SYS_ID:{}", sys_id);
            sys_health.sys_participants.remove(sys_id);
            sys_health.curr_sys_size -= 1;
        }
    }

    if sys_health.sys_participants.len() < sys_health.min_sys_size.into() {
        error!("Current system size shrinked below minimal system size, moving to failsafe.");
        return false;
    }

    warn!(
        "Running system in degraded mode, current system size {}",
        sys_health.curr_sys_size
    );

    sys_health.reset_sys_fault_set();
    true
}

pub fn system_run(
    sys_id: u8,
    port: String,
    sys_size: u8,
    min_sys_size: u8,
    timeout_ms: u16,
    critical_fn: fn() -> MemAlloc,
) {
    let mut sys_runner: SystemRunnerData =
        SystemRunnerData::new(sys_id, timeout_ms, port).expect("Failed to initialize sys_config.");
    let mut sys_health: SystemHealthData = SystemHealthData::new(sys_size, sys_id, min_sys_size);
    let mut state_success: bool = true;
    let mut iteration_counter = 0;
    simple_logger::init_with_level(log::Level::Debug).unwrap();
    info!("Starting up system ID:{}", sys_runner.system_id);
    loop {
        match sys_runner.state {
            StateMachine::Startup => {
                state_success = startup();
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::InitialSync => {
                state_success = initial_synchronization(&sys_runner, &mut sys_health);
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::CalcCritical => {
                sys_runner.sys_cycle.crit_mem_alloc = critical_fn();
                sys_runner.sys_cycle.crc = sys_runner.sys_cycle.crit_mem_alloc.calculate_crc();
                debug!("Node:{} got CRC 0x{:X}", sys_runner.system_id ,sys_runner.sys_cycle.crc);
                state_success = true;
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::ExchangeCRC => {
                state_success = exchange_crc(&sys_runner, &mut sys_health);
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::Vote => {
                sys_runner.sys_cycle.voted_crc = vote_on_crc32(&mut sys_health)
                    .expect("Failed to vote on the CRC32.");
                state_success = true;
                debug!("Node{}: Voted CRC 0x{:X}", sys_runner.system_id, sys_runner.sys_cycle.voted_crc);
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::ExchangeVote => {
                sys_runner.sys_cycle.publisher = decide_on_vote_publisher(&sys_runner, &sys_health);
                state_success = exchange_vote(&sys_runner, &mut sys_health);
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::PublishVote => {
                publish_vote(&sys_runner);
                state_success = true;
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::Reset => {
                sys_runner.sys_cycle.reset();
                sys_runner.next_state_transition(state_success);
                info!("Finished Iteration Num {iteration_counter}");
                iteration_counter += 1
            }
            StateMachine::CycleSync => {
                state_success = cyclic_synchronization(&sys_runner, &mut sys_health);
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::ErrorHandling => {
                state_success = error_handling(&sys_runner, &mut sys_health);
                sys_runner.next_state_transition(state_success);
            }
            StateMachine::Failsafe => loop {
                std::thread::sleep(Duration::from_secs(2));
                error!("Node in failsafe")
            },
        }
    }
}
