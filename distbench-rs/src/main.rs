use clap::{Parser, ValueEnum};
use distbench::community::{Community, PeerId};
use distbench::transport::channel::{ChannelTransport, ChannelTransportBuilder};
use distbench::transport::tcp::{TcpAddress, TcpTransport};
use distbench::transport::{ConnectionManager, ThinConnectionManager};
use distbench::{BincodeFormat, Format, JsonFormat};
use log::{error, info};
use runner::config::{load_config, ConfigFile};
use runner::logging::init_logger;
use runner::transport_setup::setup_offline_transport;
use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Command-line arguments for the distributed algorithms benchmark.
#[derive(Parser, Debug)]
#[command(author, version, about = "Distributed Systems Workbench")]
struct CliArgs {
    /// Path to the YAML configuration file
    #[arg(short, long, value_name = "FILE", required = true)]
    config: PathBuf,

    /// ID of this node (must exist in the config)
    #[arg(long)]
    id: Option<String>,

    /// The algorithm to run
    #[arg(short, long, required = true)]
    algorithm: String,

    /// Operating mode
    #[arg(short, long, value_enum, default_value_t = Mode::Offline)]
    mode: Mode,

    /// Serialization format
    #[arg(short, long, value_enum, default_value_t = FormatType::Bincode)]
    format: FormatType,

    /// Timeout for the algorithm to run in seconds
    #[arg(short, long, default_value_t = 10)]
    timeout: u64,

    /// Verbosity level (can be repeated: -v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count, default_value_t = 0)]
    verbose: u8,

    /// Report folder path
    #[arg(short, long)]
    report_folder: Option<PathBuf>,

    /// Base port for local instances
    #[arg(short, long, default_value_t = 10000)]
    port_base: u16,
}

/// Execution mode for the distributed system.
#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum Mode {
    /// Network mode (spawns a process listening on specific IP/port)
    Network,
    /// Local mode (spawns multiple instances listening on different ports)
    Local,
    /// Offline mode (spawns threads, uses tokio channels)
    Offline,
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum FormatType {
    Json,
    Bincode,
}

include!(concat!(env!("OUT_DIR"), "/registry.rs"));

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();

    init_logger(args.verbose);

    let config = match load_config(&args.config) {
        Ok(config) => config,
        Err(e) => {
            error!("{}", e);
            process::exit(1);
        }
    };

    if let Some(folder) = &args.report_folder {
        if let Err(e) = create_dir_all(folder) {
            error!(
                "Failed to create report directory {:?}: {}. Reports will be printed to console.",
                folder, e
            );
        } else {
            info!("Reports will be saved to {:?}", folder);
        }
    }

    match (&args.mode, &args.format) {
        (Mode::Offline, FormatType::Json) => {
            run_offline_mode(&args, &config, Arc::new(JsonFormat {})).await;
        }
        (Mode::Offline, FormatType::Bincode) => {
            run_offline_mode(&args, &config, Arc::new(BincodeFormat {})).await;
        }
        (Mode::Local, FormatType::Json) => {
            run_local_mode(&args, &config, Arc::new(JsonFormat {})).await;
        }
        (Mode::Local, FormatType::Bincode) => {
            run_local_mode(&args, &config, Arc::new(BincodeFormat {})).await;
        }
        (Mode::Network, FormatType::Json) => {
            run_network_mode(&args, &config, Arc::new(JsonFormat {})).await;
        }
        (Mode::Network, FormatType::Bincode) => {
            run_network_mode(&args, &config, Arc::new(BincodeFormat {})).await;
        }
    };
}

// --- Mode Implementations ---

/// Runs the distributed system in offline mode.
///
/// Spawns all nodes in the same process using in-memory channels for communication.
async fn run_offline_mode<F: Format + 'static>(
    args: &CliArgs,
    config: &runner::config::ConfigFile,
    format: Arc<F>,
) {
    let stop_signal = Arc::new(Notify::new());
    let builder = ChannelTransportBuilder::new();
    let mut handles = Vec::new();

    for (node_id_str, node_def) in config.iter() {
        let node_id = PeerId::new(node_id_str.clone());
        let (neighbours_str, _) = get_neighbours(config, node_id_str, &node_id);

        let community = setup_offline_transport::<ThinConnectionManager<ChannelTransport>>(
            config,
            &builder,
            &node_id,
            &neighbours_str,
        );

        let serve_handle = start_node!(
            args.algorithm,
            node_def.alg_config,
            node_id,
            community,
            stop_signal.clone(),
            format
        );
        handles.push(serve_handle);
    }

    info!(
        "Started {} nodes in offline mode. Waiting for {} seconds",
        handles.len(),
        args.timeout
    );

    let join_all_fut = futures::future::join_all(handles);
    tokio::pin!(join_all_fut);

    let results = tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(args.timeout)) => {
            info!("Timeout reached. Sending stop signal");
            stop_signal.notify_waiters();
            join_all_fut.await
        },
        results = &mut join_all_fut => {
            tokio::time::sleep(Duration::from_millis(10)).await;
            info!("All nodes finished");
            results
        }
    };

    handle_reports(results, &args.report_folder);
}

/// Runs the distributed system in local mode (all nodes on localhost).
async fn run_local_mode<F: Format + 'static>(
    args: &CliArgs,
    config: &runner::config::ConfigFile,
    format: Arc<F>,
) {
    let stop_signal = Arc::new(Notify::new());
    let mut handles = Vec::new();

    let (all_peer_sockets, all_peer_ids, peer_id_to_numeric_addr) =
        build_tcp_mappings(config, Some(args.port_base));

    let all_peer_sockets = Arc::new(all_peer_sockets);

    info!("Local mode peer map:");
    for (numeric_id, socket) in all_peer_sockets.iter() {
        let peer_id = all_peer_ids.get(numeric_id).unwrap();
        info!("  - {} (Node {}): {}", peer_id, numeric_id, socket);
    }

    for (node_id_str, node_def) in config.iter() {
        let node_id = PeerId::new(node_id_str.clone());
        let (_, neighbour_ids) = get_neighbours(config, node_id_str, &node_id);

        let community = setup_tcp_transport::<ThinConnectionManager<TcpTransport>>(
            &all_peer_ids,
            all_peer_sockets.clone(),
            &peer_id_to_numeric_addr,
            &node_id,
            &neighbour_ids,
        );

        let serve_handle = start_node!(
            args.algorithm,
            node_def.alg_config,
            node_id,
            community,
            stop_signal.clone(),
            format
        );
        handles.push(serve_handle);
    }

    info!(
        "Started {} nodes in local mode. Waiting for {} seconds",
        handles.len(),
        args.timeout
    );

    let join_all_fut = futures::future::join_all(handles);
    tokio::pin!(join_all_fut);

    let results = tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(args.timeout)) => {
            info!("Timeout reached. Sending stop signal");
            stop_signal.notify_waiters();
            join_all_fut.await
        },
        results = &mut join_all_fut => {
            tokio::time::sleep(Duration::from_millis(10)).await;
            info!("All nodes finished");
            results
        }
    };

    handle_reports(results, &args.report_folder);
}

/// Runs one node in network mode.
async fn run_network_mode<F: Format + 'static>(
    args: &CliArgs,
    config: &runner::config::ConfigFile,
    format: Arc<F>,
) {
    let stop_signal = Arc::new(Notify::new());

    // --- 1. Get Node ID ---
    let node_id_str = match &args.id {
        Some(id) => id,
        None => {
            error!("Network mode requires an --id <NODE_ID> argument.");
            process::exit(1);
        }
    };
    let node_id = PeerId::new(node_id_str.clone());
    let node_def = match config.get(node_id_str) {
        Some(def) => def,
        None => {
            error!("Node ID '{}' not found in config file.", node_id_str);
            process::exit(1);
        }
    };

    // --- 2. Generate mappings from config ---
    info!("Network mode: Parsing host/port from config...");
    // Pass `None` for port_base to indicate network mode
    let (all_peer_sockets, all_peer_ids, peer_id_to_numeric_addr) =
        build_tcp_mappings(config, None);

    let all_peer_sockets = Arc::new(all_peer_sockets);

    // --- 3. Spawn the single node ---
    let (_, neighbour_ids) = get_neighbours(config, node_id_str, &node_id);

    let community = setup_tcp_transport::<ThinConnectionManager<TcpTransport>>(
        &all_peer_ids,
        all_peer_sockets.clone(),
        &peer_id_to_numeric_addr,
        &node_id,
        &neighbour_ids,
    );

    let local_numeric_id = peer_id_to_numeric_addr.get(&node_id).unwrap();
    let local_bind_addr = all_peer_sockets.get(local_numeric_id).unwrap();
    info!(
        "Starting node {} (Node {}) on {}...",
        node_id, local_numeric_id, local_bind_addr
    );

    let serve_handle = start_node!(
        args.algorithm,
        node_def.alg_config,
        node_id,
        community,
        stop_signal.clone(),
        format
    );
    let handles = vec![serve_handle]; // Use the same report handler

    // --- 4. Wait for completion ---
    let join_all_fut = futures::future::join_all(handles);
    tokio::pin!(join_all_fut);

    let results = tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(args.timeout)) => {
            info!("Timeout reached. Sending stop signal");
            stop_signal.notify_waiters();
            join_all_fut.await
        },
        results = &mut join_all_fut => {
            tokio::time::sleep(Duration::from_millis(10)).await;
            info!("Node {} finished", node_id_str);
            results
        }
    };

    handle_reports(results, &args.report_folder);
}

// --- Helper Functions ---

/// Builds all necessary mappings for TCP transport.
///
/// # Arguments
/// * `config` - The loaded config file.
/// * `port_base` - `Some(port)` for local mode, `None` for network mode.
///
/// # Returns
/// Tuple containing:
/// 1. `HashMap<TcpAddress(u16), SocketAddr>` - Map of numeric IDs to physical sockets.
/// 2. `HashMap<TcpAddress(u16), PeerId>` - Map of numeric IDs to logical PeerIds.
/// 3. `HashMap<PeerId, TcpAddress(u16)>` - Map of logical PeerIds to numeric IDs.
fn build_tcp_mappings(
    config: &ConfigFile,
    port_base: Option<u16>,
) -> (
    HashMap<TcpAddress, SocketAddr>,
    HashMap<TcpAddress, PeerId>,
    HashMap<PeerId, TcpAddress>,
) {
    // Sort IDs to get a stable numeric index
    let mut sorted_node_ids: Vec<_> = config.keys().cloned().collect();
    sorted_node_ids.sort();

    let mut all_peer_sockets = HashMap::new();
    let mut all_peer_ids = HashMap::new();
    let mut peer_id_to_numeric_addr = HashMap::new();

    for (i, node_id_str) in sorted_node_ids.iter().enumerate() {
        let numeric_id = i as u16;
        let tcp_addr = TcpAddress(numeric_id);
        let peer_id = PeerId::new(node_id_str.clone());
        let node_def = config.get(node_id_str).unwrap();

        let socket_addr = if let Some(port_base) = port_base {
            // Local Mode: Assign 127.0.0.1:port_base + i
            let port = port_base + numeric_id;
            let addr_str = format!("127.0.0.1:{}", port);
            addr_str
                .parse()
                .unwrap_or_else(|_| panic!("Failed to parse local address: {}", addr_str))
        } else {
            // Network Mode: Read from config
            let (host, port) = match (&node_def.host, node_def.port) {
                (Some(h), Some(p)) => (h, p),
                _ => {
                    error!(
                        "Network mode requires 'host' and 'port' fields for all nodes (missing for '{}').",
                        node_id_str
                    );
                    process::exit(1);
                }
            };
            let addr_str = format!("{}:{}", host, port);
            match addr_str.parse::<SocketAddr>() {
                Ok(addr) => addr,
                Err(e) => {
                    error!(
                        "Failed to parse address '{}' for node {}: {}",
                        addr_str, node_id_str, e
                    );
                    process::exit(1);
                }
            }
        };

        all_peer_sockets.insert(tcp_addr, socket_addr);
        all_peer_ids.insert(tcp_addr, peer_id.clone());
        peer_id_to_numeric_addr.insert(peer_id, tcp_addr);
    }

    (all_peer_sockets, all_peer_ids, peer_id_to_numeric_addr)
}

/// Sets up a TCP (network) transport for a node.
fn setup_tcp_transport<CM: ConnectionManager<TcpTransport> + 'static>(
    all_peer_ids: &HashMap<TcpAddress, PeerId>,
    all_peer_sockets: Arc<HashMap<TcpAddress, SocketAddr>>,
    peer_id_to_numeric_addr: &HashMap<PeerId, TcpAddress>,
    node_id: &PeerId,
    neighbours: &HashSet<PeerId>,
) -> Arc<Community<TcpTransport, CM>> {
    // Find the numeric ID this node should use
    let local_numeric_id = peer_id_to_numeric_addr
        .get(node_id)
        .unwrap_or_else(|| panic!("Failed to find local numeric ID for node {}", node_id));

    let transport = Arc::new(TcpTransport::new(*local_numeric_id, all_peer_sockets));

    // Create map of *remote* peers (numeric ID -> PeerId)
    let remote_peer_ids: HashMap<PeerId, TcpAddress> = all_peer_ids
        .iter()
        .filter(|(addr, _)| *addr != local_numeric_id)
        .map(|(addr, id)| (id.clone(), *addr))
        .collect();

    let community = Community::new(neighbours.clone(), remote_peer_ids, transport);

    Arc::new(community)
}

/// Helper to resolve neighbour list (handles fully-connected case).
fn get_neighbours(
    config: &ConfigFile,
    node_id_str: &str,
    node_id: &PeerId,
) -> (Vec<String>, HashSet<PeerId>) {
    let node_def = config.get(node_id_str).unwrap();

    // If neighbours list is empty, assume fully connected to all *other* nodes
    let neighbours_str = if node_def
        .neighbours
        .as_deref()
        .map_or(true, |n| n.is_empty())
    {
        config
            .keys()
            .filter(|k| *k != node_id_str)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        node_def.neighbours.as_ref().unwrap().to_vec()
    };

    let neighbour_ids: HashSet<PeerId> = neighbours_str
        .iter()
        .map(|id| PeerId::new(id.clone()))
        .filter(|id| id != node_id) // Ensure node is not its own neighbour
        .collect();

    (neighbours_str, neighbour_ids)
}

/// Helper to process node results and write reports.
fn handle_reports(
    results: Vec<(
        PeerId,
        Result<distbench::transport::Result<String>, tokio::task::JoinError>,
    )>,
    report_folder: &Option<PathBuf>,
) {
    for (peer_id, join_result) in results {
        let report = match join_result {
            Err(e) => {
                error!("Task join error: {}", e);
                continue;
            }
            Ok(Err(e)) => {
                error!("Node {} returned error: {}", peer_id, e);
                continue;
            }
            Ok(Ok(report)) => report,
        };

        if let Some(folder) = report_folder {
            let file_path = folder.join(format!("{}.json", peer_id));
            let line = format!("{}\n", report);

            match OpenOptions::new()
                .create(true)
                .write(true) // Overwrite file instead of appending
                .truncate(true)
                .open(&file_path)
            {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(line.as_bytes()) {
                        error!("Failed to write report for {}: {}", peer_id, e);
                    }
                }
                Err(e) => {
                    error!("Failed to open report file for {}: {}", peer_id, e);
                }
            }
        } else {
            // No report folder, just print to info log
            info!("Report for {}: {}", peer_id, report);
        }
    }
}
