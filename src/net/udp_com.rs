use crate::net::network_helper;
use crate::state_machine::StateMachine;
use log::{debug, error, trace};
use std::fmt;
use std::net::{Ipv4Addr, UdpSocket};
use std::str::FromStr;
use std::time::Instant;

pub struct UdpSocketInfo {
    socket: UdpSocket,
    ipv4_addr: Ipv4Addr,
    broadcast_addr: String,
}

impl UdpSocketInfo {
    pub fn new(port: String) -> Option<Self> {
        let (ipv4_addr, broadcast_ipv4addr): (Ipv4Addr, Ipv4Addr) =
            network_helper::get_eth0_ipv4_and_broadcast()
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

#[derive(Debug, PartialEq)]
pub enum SystemMessageType {
    System,
    Master,
    Log
}

impl FromStr for SystemMessageType {
    type Err = ();
    fn from_str(input: &str) -> Result<SystemMessageType, Self::Err> {
        match input {
            "MASTER" => Ok(SystemMessageType::Master),
            "SYSTEM" => Ok(SystemMessageType::System),
            "LOG" => Ok(SystemMessageType::Log),
            _ => Err(()),
        }
    }
}

impl fmt::Display for SystemMessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemMessageType::Master => write!(f, "MASTER"),
            SystemMessageType::System => write!(f, "SYSTEM"),
            SystemMessageType::Log => write!(f, "LOG"),
        }
    }
}

pub struct SystemUdpMessage {
    pub message_type: SystemMessageType,
    pub sender_id: u8,
    pub sender_state: StateMachine,
    pub sender_value: u32,
}

impl SystemUdpMessage {
    pub fn new_from_string(msg: &str) -> Option<Self> {
        let msg_parts: Vec<&str> = msg.splitn(4, ':').collect();
        match msg_parts.as_slice() {
            [part1, part2, part3, part4] => {
                let message_type = SystemMessageType::from_str(part1).expect("");
                let sender_id: u8 = part2
                    .parse::<u8>()
                    .expect("Error couldn't parse the sender_id from udp msg.");
                let sender_state: StateMachine = StateMachine::parse_state(part3)
                    .expect("Couldn't parse the sender_state from udp msg.");
                let sender_value: u32 = part4
                    .parse::<u32>()
                    .expect("Couldn't parse the sender_value from udp msg");
                Some(Self {
                    message_type,
                    sender_id,
                    sender_state,
                    sender_value,
                })
            }
            [part1, part2, part3] => {
                let message_type = SystemMessageType::from_str(part1)
                    .expect("Error couldn't parse the message_type from udp msg.");
                let sender_id: u8 = part2
                    .parse::<u8>()
                    .expect("Error couldn't parse the sender_id from udp msg.");
                let sender_state: StateMachine = StateMachine::parse_state(part3)
                    .expect("Couldn't parse the sender_state from udp msg.");
                let sender_value: u32 = 0;
                Some(Self {
                    message_type,
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
    pub fn new(
        message_type: SystemMessageType,
        sender_id: u8,
        sender_state: StateMachine,
        sender_value: u32,
    ) -> Self {
        Self {
            message_type,
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
                "{}:{}:{}:{}",
                self.message_type, self.sender_id, self.sender_state, self.sender_value
            )
        } else {
            write!(
                f,
                "{}:{}:{}",
                self.message_type, self.sender_id, self.sender_state
            )
        }
    }
}
