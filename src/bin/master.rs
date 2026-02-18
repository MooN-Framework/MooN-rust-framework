use swb_fault_tolerance::net::udp_com::{SystemMessageType, SystemUdpMessage, UdpSocketInfo};
use swb_fault_tolerance::state_machine::StateMachine;

fn sys_send_master_msg_loop(udp_socket: &UdpSocketInfo) {
    let send_msg: SystemUdpMessage =
        SystemUdpMessage::new(SystemMessageType::Master, 1, StateMachine::Startup, 1);
    udp_socket.send_udp_message(send_msg).expect("Couldn't sent udp message.");
}

fn main() {
    let udp_socket =
        UdpSocketInfo::new("3841".to_string()).expect("Couldn't create and bind udp socket.");
    sys_send_master_msg_loop(&udp_socket);
}
