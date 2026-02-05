use std::net::{Ipv4Addr, UdpSocket};
use std::time::{Instant};
use std::fmt;
use log::{debug, error, trace};

use crate::state_machine::StateMachine;
use crate::net::network_helper;

pub struct UdpSocketInfo {
    socket: UdpSocket,
    ipv4_addr: Ipv4Addr,
    broadcast_addr: String,
}

impl UdpSocketInfo {
    pub fn new(port: String) -> Option<Self> {
        let (ipv4_addr, broadcast_ipv4addr): (Ipv4Addr, Ipv4Addr) = network_helper::get_eth0_ipv4_and_broadcast()
            .expect("Couldn't retrieve ipv4 addr of device and broadcast addr.");
        let broadcast_addr = format!("{}:{}", broadcast_ipv4addr, port);
        let socket = UdpSocket::bind(format!("{}:{}", "0.0.0.0", port))
            .expect("Couldn't bind to ip socket.");
        socket
            .set_broadcast(true)
            .expect("Couldn't allow broadcast message.");
        socket
            .set_nonblocking(true)
            .expect("Couldn't set socket to nonblocking.");

        debug!("Device IPv4: {}", ipv4_addr);
        debug!("Listening on 0.0.0.0:{}", port);
        debug!("Sending to broadcast: {}", broadcast_addr);
        Some(Self {
            socket,
            ipv4_addr,
            broadcast_addr,
        })
    }

    pub fn send_udp_message(&self, udp_message: SystemUdpMessage) -> Option<Instant> {
        let msg = udp_message.to_string();
        self.socket
            .send_to(msg.as_bytes(), &self.broadcast_addr)
            .expect("Couldn't send!");
        trace!("Sent: {}", msg);
        Some(Instant::now())
    }

    pub fn receive_udp_message(&self) -> Option<SystemUdpMessage> {
        let mut buf = [0u8; 1024];
        match self.socket.recv_from(&mut buf) {
            Ok((amt, src)) => {
                if src.ip() != std::net::IpAddr::V4(self.ipv4_addr) {
                        let parsed_udp_msg: SystemUdpMessage =
                            SystemUdpMessage::new_from_string(&String::from_utf8_lossy(&buf[..amt]))
                                .expect("Couldn't create SystemUdpMessage from string.");
                        trace!(
                            "Received Message{} STATE:{} VALUE:{}",
                            parsed_udp_msg.sender_id,
                            parsed_udp_msg.sender_state,
                            parsed_udp_msg.sender_value
                        );
                        return Some(parsed_udp_msg);
                    }
                }
                Err(_e) => {}
            }
            None
        }
}


pub struct SystemUdpMessage {
    pub sender_id: u8,
    pub sender_state: StateMachine,
    pub sender_value: u32,
}

impl SystemUdpMessage {
    pub fn new_from_string(msg: &str) -> Option<Self> {
        let msg_parts: Vec<&str> = msg.splitn(3, ':').collect();
        match msg_parts.as_slice() {
            [part1, part2, part3] => {
                let sender_id: u8 = part1
                    .parse::<u8>()
                    .expect("Error couldn't parse the sender_id from udp msg.");
                let sender_state: StateMachine = StateMachine::parse_state(part2)
                    .expect("Couldn't parse the sender_state from udp msg.");
                let sender_value: u32 = part3
                    .parse::<u32>()
                    .expect("Couldn't parse the sender_value from udp msg");
                Some(Self {
                    sender_id,
                    sender_state,
                    sender_value,
                })
            }
            [part1, part2] => {
                let sender_id: u8 = part1
                    .parse::<u8>()
                    .expect("Error couldn't parse the sender_id from udp msg.");
                let sender_state: StateMachine = StateMachine::parse_state(part2)
                    .expect("Couldn't parse the sender_state from udp msg.");
                let sender_value: u32 = 0;
                Some(Self {
                    sender_id,
                    sender_state,
                    sender_value,
                })
            }
            _ => {
                error!("Unexpected udp message format.");
                None
            }
        }
    }
    pub fn new(sender_id: u8, sender_state: StateMachine, sender_value: u32) -> Self {
        Self {
            sender_id,
            sender_state,
            sender_value,
        }
    }
}

impl fmt::Display for SystemUdpMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.sender_value != 0 {
            write!(
                f,
                "{}:{}:{}",
                self.sender_id, self.sender_state, self.sender_value
            )
        } else {
            write!(f, "{}:{}", self.sender_id, self.sender_state)
        }
    }
}