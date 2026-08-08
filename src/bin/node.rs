use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};
use swb_fault_tolerance::brake::braking_curve::BrakeInput;
use swb_fault_tolerance::brake::computation::{BrakeComputation, BrakeInputTolerance};
use swb_fault_tolerance::brake::selftest::BrakeSelfTest;
use swb_fault_tolerance::brake::sink::BrakeSink;
use swb_fault_tolerance::brake::voter::BrakeVoter;
use swb_fault_tolerance::framework::config::NodeConfig;
use swb_fault_tolerance::framework::runner::Runner;
use swb_fault_tolerance::framework::state::RunState;
use swb_fault_tolerance::framework::transport::UdpTransport;

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = match parse_config_path() {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("usage: node --config <path/to/node.toml>");
            return ExitCode::from(2);
        }
    };

    let cfg = match load_config(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to load config {}: {e}", config_path.display());
            return ExitCode::from(2);
        }
    };

    let session_id = fresh_session_id();
    let own_id = cfg.own_id;
    let participants = cfg.participants();

    let voter = BrakeVoter::new(participants.min_participants, 0.5);
    let state = RunState::<BrakeVoter, BrakeInput>::new(own_id, session_id, voter, participants);

    let transport = match UdpTransport::<BrakeInput, _>::new(cfg.transport(session_id)) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("transport init failed: {e:?}");
            return ExitCode::from(1);
        }
    };

    // TODO: read tolerances from config. For now hardcoded — speed within
    // 0.1 m/s, distance within 0.5 m of the majority.
    let computation = BrakeComputation::new(BrakeInputTolerance::new(0.1, 0.5));
    let sink = BrakeSink::new();
    let self_test = BrakeSelfTest::default_vectors();
    let initial_input = BrakeInput::new(1.0, 0.0, 100.0);

    let mut runner = Runner::new(
        state,
        transport,
        computation,
        initial_input,
        sink,
        self_test,
        cfg.timing(),
        cfg.diagnostic(),
    );
    runner.run();

    eprintln!("node {own_id} in failsafe, exiting");
    ExitCode::from(1)
}

/// Parse `--config <path>` from argv. `-c` and `--config=<path>` are also
/// accepted.
fn parse_config_path() -> Result<PathBuf, String> {
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" || arg == "-c" {
            return args
                .next()
                .map(PathBuf::from)
                .ok_or_else(|| "missing value for --config".into());
        }
        if let Some(v) = arg.strip_prefix("--config=") {
            return Ok(PathBuf::from(v));
        }
    }
    Err("missing --config argument".into())
}

/// Read the TOML file and parse it into a `NodeConfig`.
fn load_config(path: &PathBuf) -> Result<NodeConfig, Box<dyn std::error::Error>> {
    let bytes = fs::read_to_string(path)?;
    Ok(toml::from_str(&bytes)?)
}

/// Fresh session id from the wall clock (nanoseconds since epoch).
fn fresh_session_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as u64
}
