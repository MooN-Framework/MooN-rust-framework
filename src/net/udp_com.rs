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
                    let msg_str = String::from_utf8_lossy(&buf[..amt]);

                    let parsed_udp_msg = SystemUdpMessage::new_from_string(&msg_str)?;

                    trace!(
                        "Received Message {} STATE:{}",
                        parsed_udp_msg.sender_id, parsed_udp_msg.sender_state
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
    Log,
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

#[derive(Debug)]
pub enum SystemMessagePayload {
    None,
    Value(u32),
    Log(String),
}

#[derive(Debug)]
pub struct SystemUdpMessage {
    pub message_type: SystemMessageType,
    pub sender_id: u8,
    pub sender_state: StateMachine,
    pub payload: SystemMessagePayload,
}

impl SystemUdpMessage {
    pub fn new_value(
        message_type: SystemMessageType,
        sender_id: u8,
        sender_state: StateMachine,
        value: u32,
    ) -> Self {
        Self {
            message_type,
            sender_id,
            sender_state,
            payload: SystemMessagePayload::Value(value),
        }
    }

    pub fn new_log(sender_id: u8, sender_state: StateMachine, log: String) -> Self {
        Self {
            message_type: SystemMessageType::Log,
            sender_id,
            sender_state,
            payload: SystemMessagePayload::Log(log),
        }
    }

    pub fn new_from_string(msg: &str) -> Option<Self> {
        let msg_parts: Vec<&str> = msg.splitn(4, ':').collect();

        match msg_parts.as_slice() {
            [part1, part2, part3, part4] => {
                let message_type = SystemMessageType::from_str(part1).ok()?;
                let sender_id = part2.parse::<u8>().ok()?;
                let sender_state = StateMachine::parse_state(part3)?;

                let payload = match message_type {
                    SystemMessageType::Log => SystemMessagePayload::Log(part4.to_string()),
                    _ => {
                        let value = part4.parse::<u32>().ok()?;
                        SystemMessagePayload::Value(value)
                    }
                };

                Some(Self {
                    message_type,
                    sender_id,
                    sender_state,
                    payload,
                })
            }

            [part1, part2, part3] => {
                let message_type = SystemMessageType::from_str(part1).ok()?;
                let sender_id = part2.parse::<u8>().ok()?;
                let sender_state = StateMachine::parse_state(part3)?;

                Some(Self {
                    message_type,
                    sender_id,
                    sender_state,
                    payload: SystemMessagePayload::None,
                })
            }

            _ => {
                error!("Unexpected udp message format.");
                None
            }
        }
    }
}

impl fmt::Display for SystemUdpMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.payload {
            SystemMessagePayload::None => {
                write!(
                    f,
                    "{}:{}:{}",
                    self.message_type, self.sender_id, self.sender_state
                )
            }
            SystemMessagePayload::Value(val) => {
                write!(
                    f,
                    "{}:{}:{}:{}",
                    self.message_type, self.sender_id, self.sender_state, val
                )
            }
            SystemMessagePayload::Log(text) => {
                write!(
                    f,
                    "{}:{}:{}:{}",
                    self.message_type, self.sender_id, self.sender_state, text
                )
            }
        }
    }
}
