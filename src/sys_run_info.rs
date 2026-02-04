use std::collections::HashMap;
use std::collections::HashSet;
use crate::state_machine::StateMachine;
use crate::net::udp_com::UdpSocketInfo;
use crate::mem_alloc::MemAlloc;

pub struct  SystemHealthData {
    sys_participants : HashSet<u8>,
    curr_sys_size : u8,
    initial_sys_size: u8,
    min_sys_size: u8,
    sys_checklist : HashMap<u8,u32>,
    sys_fault_set : HashSet<u8>
}

impl SystemHealthData {
    pub fn new(initial_sys_size : u8, curr_system_id :u8, min_sys_size : u8) -> Self
    {
        let mut sys_participants = HashSet::new();
        sys_participants.insert(curr_system_id);
        Self{sys_participants, initial_sys_size, curr_sys_size : initial_sys_size, min_sys_size, sys_checklist : HashMap::new(), sys_fault_set : HashSet::new()}
    }

    pub fn check_if_all_ids_received(&self) -> bool {
        self.sys_checklist.len() == self.curr_sys_size as usize
    }

    pub fn is_id_participant(&self, sys_id : u8) -> bool
    {
        self.sys_participants.contains(&sys_id)
    }

    pub fn try_add_participant(&mut self, sys_id : u8) -> bool
    {
        if !self.is_id_participant(sys_id) {
            self.sys_participants.insert(sys_id);
            true
        } else {
            false
        }
    }

    pub fn remove_participant(&mut self, sys_id : u8) 
    {
        self.sys_participants.remove(&sys_id);
    }

    pub fn try_add_node_message(&mut self, sys_id : u8, value : u32) -> bool
    {
        if self.is_id_participant(sys_id) && !self.sys_checklist.contains_key(&sys_id)
        {

            self.sys_checklist.insert(sys_id, value);
            return true;
        }
        false
    }

    pub fn reset_sys_checklist(&mut self)
    {
        self.sys_checklist.clear();
    }
}

pub struct SystemRunnerData {
    pub state : StateMachine,
    pub last_state : StateMachine,
    pub system_id: u8,
    pub system_timeout : u16,
    pub udp_socket_info: UdpSocketInfo,
}

impl SystemRunnerData {
    pub fn new(
        system_id: u8,
        system_timeout: u16,
        port: String,
    ) -> Option<Self> {
        Some(Self {
            state : StateMachine::Startup,
            last_state : StateMachine::Startup,
            system_id,
            system_timeout,
            udp_socket_info: UdpSocketInfo::new(port)
                .expect("Couldn't create and bind udp socket.")
        })
    }
}

pub struct SystemCycleData {
    pub curr_own_crc: u32,
    pub curr_voted_crc: u32,
    pub publisher: u8,
    pub curr_fetched_crcs: HashMap<u8, u32>,
    pub curr_crit_mem_alloc: MemAlloc,
}

impl SystemCycleData {
    pub fn new() -> Self {
        Self{curr_own_crc : 0, curr_voted_crc : 0, publisher : 0, curr_fetched_crcs : HashMap::new(), curr_crit_mem_alloc : MemAlloc::new_null().expect("MemAlloc null init failed.")}
    }
}