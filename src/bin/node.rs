//! # Software-Based Fault Tolerance Framework
//!
//! This main binary is a concrete example for the brake example implementation of the framework.
//! It is a node that participates in a distributed system and performs computations, voting, and
//! decision-making based on the brake input data. The node communicates with other nodes using UDP transport and
//! follows the protocols defined in the framework to ensure fault tolerance and correct operation even in the presence of failures.
//!
//! ## Responsibilities
//!
//! - Load the configuration from a TOML file specified by the `--config` command-line argument.
//! - Initialize the runner with the loaded configuration, including the computation,
//! - voter, sink, and self-test components (that have to be implemented by you for your use case ).
//!
//! ## Logging
//!
//! Log lines are always written to stdout. When `--log-dir <path>` is passed,
//! an additional per-session file sink is enabled:
//!
//!   `<log-dir>/node_<own_id>_session_<session_id>.log`
//!
//! A stable symlink `<log-dir>/node_<own_id>_current.log` is (re)pointed at
//! the freshly opened session file on every start so external tooling has a
//! deterministic path to the current log.
//!
//! Without `--log-dir` the file sink is off (stdout only). The operator is
//! responsible for only setting the flag on real hardware.
//!
//! Both sinks are written by worker threads, so a slow reader on the
//! other end of the node's stdout cannot stall a cycle. Neither worker
//! drops lines; see `init_logging`.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use swb_fault_tolerance::brake::braking_curve::BrakeInput;
use swb_fault_tolerance::brake::computation::{BrakeComputation, BrakeInputTolerance};
use swb_fault_tolerance::brake::selftest::BrakeSelfTest;
use swb_fault_tolerance::brake::sink::BrakeSink;
use swb_fault_tolerance::brake::voter::BrakeVoter;
use swb_fault_tolerance::framework::config::NodeConfig;
use swb_fault_tolerance::framework::runner::Runner;
use swb_fault_tolerance::framework::state::RunState;
use swb_fault_tolerance::framework::traits::{LatchedInput, NoAppState};
use swb_fault_tolerance::framework::transport::UdpTransport;

struct CliArgs {
    config: PathBuf,
    log_dir: Option<PathBuf>,
}

fn main() -> ExitCode {
    // Parse CLI first (no logging yet — logging init needs the config).
    let args = match parse_args() {
        Ok(a) => a,
        Err(msg) => {
            eprintln!("{msg}");
            eprintln!("usage: node --config <path/to/node.toml> [--log-dir <dir>]");
            return ExitCode::from(2);
        }
    };

    // Load the configuration from the specified TOML file. If loading fails, print an error message and exit.
    let cfg = match load_config(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to load config {}: {e}", args.config.display());
            return ExitCode::from(2);
        }
    };

    let session_id = fresh_session_id();
    let own_id = cfg.own_id;
    let participants = cfg.participants();

    // Initialize the tracing subscriber. `_log_guards` MUST stay alive for
    // the whole program lifetime — dropping them flushes and closes the
    // sinks. Never call `std::process::exit` past this point, that skips
    // the drop and loses whatever is still buffered.
    let _log_guards = match init_logging(args.log_dir.as_deref(), own_id, session_id) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("logging init failed: {e}");
            return ExitCode::from(1);
        }
    };

    let voter = BrakeVoter::new(participants.min_participants, 0.5);
    let state = RunState::<BrakeVoter, BrakeInput>::new(own_id, session_id, voter, participants);

    // Initialize the UDP transport for communication with other nodes. The transport is configured based on the loaded configuration.
    let transport = match UdpTransport::<BrakeInput, _>::new(cfg.transport(session_id)) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("transport init failed: {e:?}");
            return ExitCode::from(1);
        }
    };

    // Hardcoded tolerance here for simplicity since the BrakeInputTolerance is not part of the config.
    // In a real application, you might want to make this configurable.
    let computation = BrakeComputation::new(BrakeInputTolerance::new(0.1, 0.5));
    let sink = BrakeSink::new();
    let self_test = BrakeSelfTest::default_vectors();
    let initial_input = BrakeInput::new(1.0, 0.0, 100.0);

    // Create the runner with the initialized components and configuration, and start the main loop of the node.
    // `LatchedInput` gives the runner the same "hold + return" input behaviour as before —
    // swap it for a custom `InputSource` impl to drive inputs from hardware, a shared buffer, etc.
    // `NoAppState` is the zero-cost `ApplicationStateProvider` for use cases that have no
    // per-node domain state to fold into the system-state CRC / state sync.
    let mut runner = Runner::new(
        state,
        transport,
        computation,
        LatchedInput::new(initial_input),
        NoAppState,
        sink,
        self_test,
        cfg.timing(),
        #[cfg(feature = "diagnostic")]
        cfg.diagnostic(),
    );
    runner.run();

    eprintln!("node {own_id} in failsafe, exiting program context.");
    ExitCode::from(1)
}

/// How many lines the stdout worker may buffer before a write blocks.
/// At the ten or so lines a cycle this node emits, that is roughly a
/// minute of output at a 20 ms cycle, and about five megabytes worst
/// case. Anything beyond that is not a stalled reader any more.
const STDOUT_BUFFERED_LINES: usize = 32_768;

/// Set up the tracing subscriber.
///
/// stdout is always active. A file sink is added iff `log_dir` is `Some`.
/// When the file sink is active, log lines go to
/// `<log_dir>/node_<own_id>_session_<session_id>.log` and the symlink
/// `<log_dir>/node_<own_id>_current.log` is repointed at that file.
///
/// Both sinks are written by worker threads. stdout used to be written
/// synchronously from the node thread, which is fine against a file or a
/// terminal but not against a pipe: a harness or GUI that reads the
/// node's output stalls for a moment, the 64 KB pipe buffer fills, and
/// the next `write` blocks inside whatever phase happened to be logging.
/// That surfaced as a phase deadline missed for no visible reason, with
/// the milliseconds unaccounted for between the cycle anchor and the
/// first send.
///
/// The stdout worker is explicitly NOT lossy. `tracing_appender`
/// defaults to dropping lines once its buffer is full, which would make
/// `wait_for_log` in the scenario harness miss lines at random. Blocking
/// after `STDOUT_BUFFERED_LINES` is the better failure mode here: the
/// node keeps every line, and a reader that far behind is a problem in
/// its own right.
fn init_logging(
    log_dir: Option<&Path>,
    own_id: u8,
    session_id: u64,
) -> Result<Vec<WorkerGuard>, Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let mut guards: Vec<WorkerGuard> = Vec::new();

    let (stdout_writer, stdout_guard) = NonBlockingBuilder::default()
        .lossy(false)
        .buffered_lines_limit(STDOUT_BUFFERED_LINES)
        .thread_name("log-stdout")
        .finish(std::io::stdout());
    guards.push(stdout_guard);
    let stdout_layer = fmt::layer().with_writer(stdout_writer);

    if let Some(dir) = log_dir {
        fs::create_dir_all(dir)?;

        let filename = format!("node_{own_id}_session_{session_id}.log");
        let appender = tracing_appender::rolling::never(dir, &filename);
        let (nb_writer, file_guard) = NonBlockingBuilder::default()
            .lossy(false)
            .buffered_lines_limit(STDOUT_BUFFERED_LINES)
            .thread_name("log-file")
            .finish(appender);
        guards.push(file_guard);

        let file_layer = fmt::layer()
            .with_writer(nb_writer)
            .with_ansi(false)
            .with_target(true)
            .with_thread_ids(true)
            .with_level(true);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer)
            .with(file_layer)
            .init();

        update_current_symlink(dir, own_id, &filename)?;

        tracing::info!(
            own_id,
            session_id,
            log_file = %dir.join(&filename).display(),
            "file logging enabled"
        );
        Ok(guards)
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer)
            .init();
        Ok(guards)
    }
}

/// (Re)create `<dir>/node_<own_id>_current.log` as a symlink to
/// `session_filename`. Relative target so the link stays valid if the log
/// directory is moved. Existing symlink/file at the link path is removed
/// first.
#[cfg(unix)]
fn update_current_symlink(
    dir: &Path,
    own_id: u8,
    session_filename: &str,
) -> std::io::Result<()> {
    use std::os::unix::fs as unix_fs;
    let link = dir.join(format!("node_{own_id}_current.log"));
    match fs::remove_file(&link) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    unix_fs::symlink(session_filename, &link)
}

#[cfg(not(unix))]
fn update_current_symlink(
    _dir: &Path,
    _own_id: u8,
    _session_filename: &str,
) -> std::io::Result<()> {
    Ok(())
}

/// Parse `--config <path>` (required) and `--log-dir <path>` (optional).
/// Both accept `--key=value` and `-c` (config only).
fn parse_args() -> Result<CliArgs, String> {
    let mut config: Option<PathBuf> = None;
    let mut log_dir: Option<PathBuf> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                let v = args
                    .next()
                    .ok_or_else(|| "missing value for --config".to_string())?;
                config = Some(PathBuf::from(v));
            }
            "--log-dir" => {
                let v = args
                    .next()
                    .ok_or_else(|| "missing value for --log-dir".to_string())?;
                log_dir = Some(PathBuf::from(v));
            }
            s if s.starts_with("--config=") => {
                config = Some(PathBuf::from(&s["--config=".len()..]));
            }
            s if s.starts_with("--log-dir=") => {
                log_dir = Some(PathBuf::from(&s["--log-dir=".len()..]));
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    Ok(CliArgs {
        config: config.ok_or_else(|| "missing --config argument".to_string())?,
        log_dir,
    })
}

/// Read the TOML file and parse it into a `NodeConfig`.
fn load_config(path: &PathBuf) -> Result<NodeConfig, Box<dyn std::error::Error>> {
    let bytes = fs::read_to_string(path)?;
    NodeConfig::verify_and_parse(&bytes).map_err(|e| Box::<dyn std::error::Error>::from(e.to_string()))
}

/// Fresh session id from the wall clock (nanoseconds since epoch).
fn fresh_session_id() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as u64
}