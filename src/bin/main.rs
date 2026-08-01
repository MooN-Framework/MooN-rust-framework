use std::net::Ipv4Addr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use swb_fault_tolerance::brake::braking_curve::BrakeInput;
use swb_fault_tolerance::brake::computation::BrakeComputation;
use swb_fault_tolerance::brake::sink::BrakeSink;
use swb_fault_tolerance::brake::voter::BrakeVoter;
use swb_fault_tolerance::framework::diagnostic::DiagnosticConfig;
use swb_fault_tolerance::framework::run_state::RunState;
use swb_fault_tolerance::framework::runner::{CycleTiming, Runner};
use swb_fault_tolerance::framework::udp_transport::{TransportConfig, UdpTransport};

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    // -------------------------------------------------------------
    // Konfiguration (spaeter aus Datei/CLI laden)
    // -------------------------------------------------------------

    // Kapazitaet des Peer-Arrays (compile-time Obergrenze).
    // Bei 2-oo-3 sind 2 Peers aktiv; N=7 laesst bis zu 8 Nodes zu.
    const NUM_PEERS: usize = 1;

    let own_id: u8 = 0;
    let session_id: u64 = fresh_session_id();
    let interface_name: String = "lo".into();

    // -------------------------------------------------------------
    // Framework-Komponenten
    // -------------------------------------------------------------

    let voter = BrakeVoter::new(
        /* required */ 1, /* distance_tolerance in Metern */ 0.5,
    );

    let state: RunState<BrakeVoter, 1> = RunState::new(own_id, session_id, voter);

    let transport = UdpTransport::<_>::new(TransportConfig {
        interface_name,
        multicast_group: Ipv4Addr::new(239, 10, 0, 1),
        port: 5555,
        self_node_id: own_id,
        self_session_id: session_id,
        initial_sequenz_num: 0,
    })
    .expect("transport init failed");

    let timing = CycleTiming {
        cycle_duration: Duration::from_millis(2000),
        init_sync_timeout: Duration::from_millis(10000),
        cycle_sync_timeout: Duration::from_millis(5),
        share_timeout: Duration::from_millis(5),
        ack_timeout: Duration::from_millis(5),
        peer_sync_request_interval: Duration::from_millis(2),
        peer_sync_timeout: Duration::from_millis(500),
        stale_threshold: Duration::from_millis(50),
        resync_interval_cycles: 30,
    };

    // -------------------------------------------------------------
    // Anwendungskomponenten
    // -------------------------------------------------------------

    let computation = BrakeComputation;
    let sink = BrakeSink::new();

    // Fester Start-Input. Ueber set_input im Runner spaeter durch das
    // Diagnose-Tool per UDP ueberschreibbar.
    let initial_input = BrakeInput::new(
        /* current_speed */ 1.0, /* target_speed */ 0.0,
        /* available_distance */ 100.0,
    );

    let diag_cfg = DiagnosticConfig {
        enabled: true,
        interface_name: "lo".to_string(),
        multicast_group: Ipv4Addr::new(239, 10, 0, 2),
        port: 6666,
    };

    // -------------------------------------------------------------
    // Loop
    // -------------------------------------------------------------

    let mut runner = Runner::new(
        state,
        transport,
        computation,
        initial_input,
        sink,
        timing,
        diag_cfg,
    );
    runner.run();

    // Wenn run() zurueckkehrt, ist der Node in Failsafe.
    eprintln!("Node {} in Failsafe. Exiting.", own_id);
    std::process::exit(1);
}

fn fresh_session_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as u64
}

fn derive_stale_threshold(cycle_duration: Duration, sync_epsilon_ns: i64) -> Duration {
    let sync_based = Duration::from_nanos((2 * sync_epsilon_ns.max(0)) as u64);
    let cycle_based = cycle_duration / 2;
    sync_based.max(cycle_based)
}
