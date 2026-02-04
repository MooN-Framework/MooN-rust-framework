use std::cell::Cell;
use std::collections::HashMap;
use crate::state_machine::StateMachine;
use crate::net::udp_com::UdpSocketInfo;
use crate::mem_alloc::MemAlloc;

pub struct SystemConfigData {
    pub state : Cell<StateMachine>,
    pub system_id: u8,
    pub system_timeout : u16,
    pub udp_socket_info: UdpSocketInfo,
    pub initial_sys_size : u8,
    pub sys_size : Cell<u8>,
    pub min_sys_size : u8,
}

impl SystemConfigData {
    pub fn new(
        system_id: u8,
        system_timeout: u16,
        initial_sys_size: u8,
        min_sys_size: u8,
        port: String,
    ) -> Option<Self> {
        Some(Self {
            state : Cell::new(StateMachine::Startup),
            system_id,
            system_timeout,
            udp_socket_info: UdpSocketInfo::new(port)
                .expect("Couldn't create and bind udp socket."),
            initial_sys_size,
            sys_size : Cell::new(initial_sys_size),
            min_sys_size,
        })
    }
}

pub struct SystemCycleData {
    pub crc: u32,
    pub voted_crc: u32,
    pub publisher: u8,
    pub curr_fetched_crcs: HashMap<u8, u32>,
    pub curr_system_ids: Vec<u8>,
    pub curr_crit_mem_alloc: MemAlloc,
}

impl SystemCycleData {
    pub fn new() -> Self {
        Self{crc : 0, voted_crc : 0, publisher : 0, curr_fetched_crcs : HashMap::new(), curr_system_ids : Vec::new(), curr_crit_mem_alloc : MemAlloc::new_null().unwrap()}
    }
}