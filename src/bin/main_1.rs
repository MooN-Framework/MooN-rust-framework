use std::{net::Ipv4Addr, time::{Duration, Instant}};
use std::process::ExitCode;

use swb_fault_tolerance::{net::udp_transport::{RecvOutcome, TransportConfig, UdpTransport}, sys_state::state_machine::NodeState};

fn main() -> ExitCode {
    let config = TransportConfig {
        interface_name: "lo".into(),
        multicast_group: Ipv4Addr::new(239, 10, 0, 1),
        port: 3881,
        self_node_id: 0,
        self_session_id: 4,
        initial_sequenz_num: 0,
    };

    let mut tx = match UdpTransport::new(config) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Transport-Init fehlgeschlagen: {:?}", e);
            return ExitCode::FAILURE;
        }
    };

    let send_interval = Duration::from_secs(2);
    let recv_slice    = Duration::from_millis(100);
    let mut next_send = Instant::now();
    loop
    {
        if Instant::now() >= next_send {
            match tx.send_state(NodeState::Startup) {
                Ok(seq)  => println!("gesendet: state-frame seq={}", seq),
                Err(e)   => eprintln!("send fehlgeschlagen: {:?}", e),
            }
            next_send += send_interval;
        }

        // Bis zum nächsten Sende-Zeitpunkt (max. recv_slice) lauschen
        let deadline = std::cmp::min(next_send, Instant::now() + recv_slice);
        match tx.recv_before(deadline) {
            RecvOutcome::Valid(frame) => {
                println!(
                    "empfangen: node={} session={} seq={} state={:?}",
                    frame.node_id(),
                    frame.session_id(),
                    frame.seq_num(),
                    frame.node_state(),
                );
            }
            RecvOutcome::Timeout       => { /* okay, weiter */ }
            RecvOutcome::SelfLoopback  => { /* eigenen Frame ignorieren */ }
            RecvOutcome::CrcError      => eprintln!("CRC-Fehler"),
            RecvOutcome::Malformed(e)  => eprintln!("kaputter Frame: {:?}", e),
            RecvOutcome::Duplicate { peer_id, seen, last } =>
                eprintln!("Duplikat von {}: seen={} last={}", peer_id, seen, last),
            RecvOutcome::NewSession { peer_id, previous_session, new_session, frame } => {
                println!(
                    "Peer {} rebootete: {} → {}, akzeptiere neue Session",
                    peer_id, previous_session, new_session,
                );
                tx.accept(&frame);

                // Frame direkt wie einen Valid behandeln, damit der erste Frame der
                // neuen Session nicht verloren geht
                println!(
                    "empfangen: node={} session={} seq={} state={:?}",
                    frame.node_id(),
                    frame.session_id(),
                    frame.seq_num(),
                    frame.node_state(),
                );
            },
            RecvOutcome::SeqGap { peer_id, gap, .. } =>
                eprintln!("Lücke bei Peer {}: {} Frames verpasst", peer_id, gap),
        }

    }

    ExitCode::SUCCESS
}
