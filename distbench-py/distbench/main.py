"""Main entry point and CLI for the distributed algorithms framework."""

import asyncio
import logging
import os
import sys
from typing import Any

import click

# Add TRACE log level (below DEBUG)
TRACE = 5
logging.addLevelName(TRACE, "TRACE")


def trace(self: logging.Logger, message: str, *args, **kwargs) -> None:  # type: ignore
    """Log a message with severity 'TRACE'."""
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kwargs)


# Add trace method to Logger class
logging.Logger.trace = trace  # type: ignore


# ANSI color codes for log levels
class Colors:
    """ANSI color codes"""

    RESET = "\033[0m"
    GREY = "\033[90m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    PURPLE = "\033[95m"
    CYAN = "\033[96m"


LOG_LEVEL_COLORS = {
    logging.DEBUG: Colors.BLUE,
    logging.INFO: Colors.GREEN,
    logging.WARNING: Colors.YELLOW,
    logging.ERROR: Colors.RED,
    logging.CRITICAL: Colors.RED,
    TRACE: Colors.GREY,
}

# Import context variable
from distbench.context import current_node_id


class NodeContextFormatter(logging.Formatter):
    """Custom formatter that includes node ID in log messages."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with node ID and colors."""
        level_color = LOG_LEVEL_COLORS.get(record.levelno, Colors.RESET)
        record.levelname_color = f"{level_color}{record.levelname:5}{Colors.RESET}"  # type: ignore

        node_id = current_node_id.get()
        if node_id:
            node_colors = [Colors.CYAN, Colors.PURPLE, Colors.BLUE, Colors.YELLOW, Colors.GREEN]
            color_index = hash(node_id) % len(node_colors)
            node_color = node_colors[color_index]
            record.node_id_color = f"{node_color}[{node_id}]{Colors.RESET} "  # type: ignore
        else:
            record.node_id_color = ""  # type: ignore

        if not hasattr(record, "asctime"):
            record.asctime = self.formatTime(record, self.datefmt)
        record.asctime_color = f"{Colors.GREY}{record.asctime}{Colors.RESET}"  # type: ignore

        return super().format(record)


from distbench.algorithm import Algorithm, Peer
from distbench.community import Community, PeerId
from distbench.config import extract_algorithm_config, extract_neighbours, load_config
from distbench.encoding.format import Format
from distbench.encoding.json_format import JsonFormat
from distbench.encoding.msgpack_format import MsgpackFormat
from distbench.node import Node
from distbench.transport import Address, LocalAddress, LocalTransport
from distbench.transport.tcp import SocketAddress, TcpAddress, TcpTransport
from distbench.algorithms import ALGORITHM_REGISTRY

ALGORITHMS = ALGORITHM_REGISTRY


def register_algorithm(name: str, cls: type[Algorithm]) -> None:
    """Register an algorithm for use in the CLI.

    Args:
        name: Name to use for CLI selection
        cls: Algorithm class
    """
    ALGORITHMS[name] = cls


def _get_format(format_name: str) -> Format:
    """Get serialization format implementation."""
    if format_name == "json":
        return JsonFormat()
    elif format_name == "msgpack":
        return MsgpackFormat()
    else:
        raise ValueError(f"Unknown format: {format_name}")


async def _run_nodes(
    nodes: list[Node[Any, Any]],
    timeout: float,
    stop_event: asyncio.Event,
    startup_delay: int = 0,
) -> None:
    """Start and wait for a list of nodes to complete.

    Args:
        nodes: List of nodes to run
        timeout: Maximum runtime in seconds
        stop_event: Event to signal shutdown
        startup_delay: Delay in milliseconds before starting nodes
    """
    logger = logging.getLogger(__name__)
    tasks = [asyncio.create_task(node.start(stop_event, startup_delay)) for node in nodes]

    # Wait with timeout
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=timeout
        )
        # Check for exceptions in node tasks
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Node task failed: {res}", exc_info=res)
        logger.info("All nodes completed")
    except asyncio.TimeoutError:
        logger.warning(f"Timeout after {timeout} seconds, stopping nodes")
        stop_event.set()
        # Give nodes a chance to stop gracefully
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run_offline(
    config_path: str,
    algorithm_name: str,
    format_name: str,
    timeout: float,
    report_dir: str | None,
    latency: tuple[int, int] = (0, 0),
    startup_delay: int = 0,
) -> None:
    """Run in offline mode with all nodes in one process (in-memory transport).

    Args:
        config_path: Path to configuration file
        algorithm_name: Name of algorithm to run
        format_name: Serialization format ("json" or "msgpack")
        timeout: Maximum runtime in seconds
        report_dir: Optional directory for storing reports
        latency: Tuple of (min_ms, max_ms) for network latency simulation
        startup_delay: Delay in milliseconds before starting nodes
    """
    logger = logging.getLogger(__name__)
    config_data = load_config(config_path)
    all_node_ids = set(config_data.keys())
    format_impl = _get_format(format_name)
    server_registry: dict[str, Node[LocalTransport, LocalAddress]] = {}
    stop_event = asyncio.Event()

    # First pass: create all nodes and register them
    nodes: list[Node[Any, Any]] = []
    for node_id_str, node_config in config_data.items():
        node_id = PeerId(node_id_str)
        base_transport = LocalTransport()
        base_transport.set_current_node(node_id_str)

        # Wrap with DelayedTransport if latency is configured
        if latency[0] > 0 or latency[1] > 0:
            from distbench.transport.delayed import DelayedTransport

            transport = DelayedTransport(base_transport, latency)
        else:
            transport = base_transport

        neighbours = extract_neighbours(node_config, all_node_ids)
        neighbours.discard(node_id)  # Remove self from neighbors

        peer_addresses = {
            PeerId(pid): LocalAddress(pid) for pid in all_node_ids if pid != node_id_str
        }

        community = Community(
            neighbours=neighbours,
            peer_addresses=peer_addresses,
            transport=transport,
        )

        peers = {
            pid: Peer(pid, community.connection(pid), format_impl, community)  # type: ignore
            for pid, conn in community.connections.items()
        }

        algo_config = extract_algorithm_config(node_config)
        if algorithm_name not in ALGORITHMS:
            raise ValueError(f"Unknown algorithm: {algorithm_name}")
        algorithm = ALGORITHMS[algorithm_name](algo_config, peers)

        node = Node(
            node_id=node_id,
            community=community,
            algorithm=algorithm,
            format=format_impl,
            report_dir=report_dir,
        )
        nodes.append(node)
        server_registry[node_id_str] = node

    # Second pass: register all servers with all transports
    for node in nodes:
        transport = node.community.transport
        for node_id_str, server_node in server_registry.items():
            transport.register_server(node_id_str, server_node)

    logger.info(f"Starting {len(nodes)} nodes in offline mode")
    await _run_nodes(nodes, timeout, stop_event, startup_delay)


def build_tcp_mappings(
    config_data: dict[str, Any], port_base: int, is_local: bool
) -> tuple[
    dict[TcpAddress, SocketAddress],
    dict[TcpAddress, PeerId],
    dict[PeerId, TcpAddress],
]:
    """Builds all necessary mappings for TCP transport, mirroring Rust logic.

    Args:
        config_data: The loaded configuration
        port_base: Base port number
        is_local: If True, use 127.0.0.1:port_base+i. If False, use node_id:port_base.

    Returns:
        A tuple of (all_peer_sockets, all_peer_ids, peer_id_to_numeric_addr)
    """
    all_peer_sockets: dict[TcpAddress, SocketAddress] = {}
    all_peer_ids: dict[TcpAddress, PeerId] = {}
    peer_id_to_numeric_addr: dict[PeerId, TcpAddress] = {}

    # Sort IDs to get a stable numeric index
    sorted_node_ids = sorted(config_data.keys())

    for i, node_id_str in enumerate(sorted_node_ids):
        # Use u16 (0-65535) for the numeric ID
        numeric_id = i
        if numeric_id > 65535:
            raise ValueError("Too many nodes for u16 numeric ID")

        tcp_addr = TcpAddress(numeric_id)
        peer_id = PeerId(node_id_str)

        if is_local:
            # Local mode: assign 127.0.0.1:port_base + i
            socket_addr = SocketAddress("127.0.0.1", port_base + i)
        else:
            # Network mode: use node ID as hostname with port_base
            socket_addr = SocketAddress(node_id_str, port_base)

        all_peer_sockets[tcp_addr] = socket_addr
        all_peer_ids[tcp_addr] = peer_id
        peer_id_to_numeric_addr[peer_id] = tcp_addr

    return all_peer_sockets, all_peer_ids, peer_id_to_numeric_addr


async def run_local(
    config_path: str,
    algorithm_name: str,
    format_name: str,
    timeout: float,
    port_base: int,
    report_dir: str | None,
    latency: tuple[int, int] = (0, 0),
    startup_delay: int = 0,
) -> None:
    """Run in local mode with all nodes on localhost (TCP transport).

    Args:
        config_path: Path to configuration file
        algorithm_name: Name of algorithm to run
        format_name: Serialization format ("json" or "msgpack")
        timeout: Maximum runtime in seconds
        port_base: Base port for local nodes
        report_dir: Optional directory for storing reports
        latency: Tuple of (min_ms, max_ms) for network latency simulation
        startup_delay: Delay in milliseconds before starting nodes
    """
    logger = logging.getLogger(__name__)
    config_data = load_config(config_path)
    all_node_ids = set(config_data.keys())
    format_impl = _get_format(format_name)
    stop_event = asyncio.Event()

    # Build all mappings
    (
        all_peer_sockets,
        all_peer_ids,
        peer_id_to_numeric_addr,
    ) = build_tcp_mappings(config_data, port_base, True)

    logger.trace("Local mode node mappings:")
    for numeric_id, peer_id in all_peer_ids.items():
        sock_addr = all_peer_sockets[numeric_id]
        logger.trace(f"  - {peer_id} ({numeric_id}): {sock_addr}")

    nodes: list[Node[Any, Any]] = []
    for node_id_str, node_config in config_data.items():
        node_id = PeerId(node_id_str)
        local_numeric_id = peer_id_to_numeric_addr[node_id]

        # Each transport knows its own ID and the map to all physical sockets
        base_transport = TcpTransport(local_numeric_id, all_peer_sockets)

        # Wrap with DelayedTransport if latency is configured
        if latency[0] > 0 or latency[1] > 0:
            from distbench.transport.delayed import DelayedTransport

            transport = DelayedTransport(base_transport, latency)
        else:
            transport = base_transport

        neighbours = extract_neighbours(node_config, all_node_ids)
        neighbours.discard(node_id)

        # The community maps PeerIDs to *logical* numeric addresses
        peer_addresses: dict[PeerId, Address] = {
            pid: addr for pid, addr in peer_id_to_numeric_addr.items() if pid != node_id
        }

        community = Community(
            neighbours=neighbours,
            peer_addresses=peer_addresses,  # type: ignore
            transport=transport,
        )

        peers = {
            pid: Peer(pid, community.connection(pid), format_impl, community)  # type: ignore
            for pid, conn in community.connections.items()
        }

        algo_config = extract_algorithm_config(node_config)
        if algorithm_name not in ALGORITHMS:
            raise ValueError(f"Unknown algorithm: {algorithm_name}")
        algorithm = ALGORITHMS[algorithm_name](algo_config, peers)

        node = Node(
            node_id=node_id,
            community=community,
            algorithm=algorithm,
            format=format_impl,
            report_dir=report_dir,
        )
        nodes.append(node)

    logger.info(f"Starting {len(nodes)} nodes in local mode")
    await _run_nodes(nodes, timeout, stop_event, startup_delay)


async def run_network(
    config_path: str,
    algorithm_name: str,
    format_name: str,
    timeout: float,
    node_id_str: str,
    port_base: int,
    report_dir: str | None,
    latency: tuple[int, int] = (0, 0),
    startup_delay: int = 0,
) -> None:
    """Run in network mode with this node connecting to others.

    Args:
        config_path: Path to configuration file
        algorithm_name: Name of algorithm to run
        format_name: Serialization format ("json" or "msgpack")
        timeout: Maximum runtime in seconds
        node_id_str: ID of this node
        report_dir: Optional directory for storing reports
        latency: Tuple of (min_ms, max_ms) for network latency simulation
        startup_delay: Delay in milliseconds before starting nodes
    """
    logger = logging.getLogger(__name__)
    config_data = load_config(config_path)

    if node_id_str not in config_data:
        raise ValueError(f"Node ID '{node_id_str}' not found in configuration")

    all_node_ids = set(config_data.keys())
    node_config = config_data[node_id_str]
    node_id = PeerId(node_id_str)
    format_impl = _get_format(format_name)
    stop_event = asyncio.Event()

    # Build all mappings from config
    (
        all_peer_sockets,
        all_peer_ids,
        peer_id_to_numeric_addr,
    ) = build_tcp_mappings(config_data, port_base, False)

    logger.trace("Network mode node mappings:")
    for numeric_id, peer_id in all_peer_ids.items():
        sock_addr = all_peer_sockets[numeric_id]
        logger.trace(f"  - {peer_id} ({numeric_id}): {sock_addr}")

    local_numeric_id = peer_id_to_numeric_addr[node_id]
    bind_addr = all_peer_sockets[local_numeric_id]

    base_transport = TcpTransport(local_numeric_id, all_peer_sockets)

    # Wrap with DelayedTransport if latency is configured
    if latency[0] > 0 or latency[1] > 0:
        from distbench.transport.delayed import DelayedTransport

        transport = DelayedTransport(base_transport, latency)
    else:
        transport = base_transport

    neighbours = extract_neighbours(node_config, all_node_ids)
    neighbours.discard(node_id)

    # The community maps PeerIDs to *logical* numeric addresses
    peer_addresses: dict[PeerId, Address] = {
        pid: addr for pid, addr in peer_id_to_numeric_addr.items() if pid != node_id
    }

    community = Community(
        neighbours=neighbours,
        peer_addresses=peer_addresses,  # type: ignore
        transport=transport,
    )

    peers = {
        pid: Peer(pid, community.connection(pid), format_impl, community)  # type: ignore
        for pid, conn in community.connections.items()
    }

    algo_config = extract_algorithm_config(node_config)
    if algorithm_name not in ALGORITHMS:
        raise ValueError(f"Unknown algorithm: {algorithm_name}")
    algorithm = ALGORITHMS[algorithm_name](algo_config, peers)

    node = Node(
        node_id=node_id,
        community=community,
        algorithm=algorithm,
        format=format_impl,
        report_dir=report_dir,
    )

    logger.info(f"Starting node {node_id_str} in network mode on {bind_addr}")
    await _run_nodes([node], timeout, stop_event, startup_delay)


@click.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to configuration YAML file",
)
@click.option(
    "--algorithm",
    "-a",
    required=True,
    help="Name of algorithm to run",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(["offline", "local", "network"], case_sensitive=False),
    default="offline",
    show_default=True,
    help="Execution mode: "
    "offline (in-memory channels), "
    "local (TCP on localhost), "
    "network (TCP over network)",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["json", "msgpack"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Serialization format",
)
@click.option(
    "--timeout",
    "-t",
    default=30.0,
    type=float,
    show_default=True,
    help="Timeout in seconds",
)
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Increase verbosity (-v: DEBUG, -vv: TRACE)",
)
@click.option(
    "--id",
    "node_id",
    help="Node ID (required for --mode network)",
)
@click.option(
    "--port-base",
    "-p",
    default=8000,
    type=int,
    show_default=True,
    help="Base port for local/network modes",
)
@click.option(
    "--report-dir",
    type=click.Path(file_okay=False, dir_okay=True, writable=True, resolve_path=True),
    default=None,
    help="Directory to append node reports (as .jsonl files)",
)
@click.option(
    "--latency",
    "-l",
    default="0-0",
    type=str,
    show_default=True,
    help="Network latency simulation in milliseconds (e.g. '10-50' for 10-50ms range, '20-20' for fixed 20ms)",
)
@click.option(
    "--startup-delay",
    "-s",
    default=500,
    type=int,
    show_default=True,
    help="Startup delay in milliseconds before nodes begin",
)
def main(
    config: str,
    algorithm: str,
    mode: str,
    format: str,
    timeout: float,
    verbose: int,
    node_id: str | None,
    port_base: int,
    report_dir: str | None,
    latency: str,
    startup_delay: int,
) -> None:
    """Distributed Algorithms Framework.

    Run distributed algorithms in various modes.

    Example usage:

        # Run echo algorithm in offline (in-memory) mode
        distbench -c configs/echo.yaml -a echo --mode offline

        # Run echo algorithm in local (TCP on localhost) mode
        distbench -c configs/echo.yaml -a echo --mode local --port-base 10000

        # Run in network mode as node n1 (reads host/port from config)
        distbench -c configs/echo.yaml -a echo --mode network --id n1
    """
    # Setup logging
    # -v 0 (default): INFO
    # -v:             DEBUG
    # -vv:            TRACE
    log_levels = [logging.INFO, logging.DEBUG, TRACE]
    log_level = log_levels[min(verbose, 2)]

    formatter = NodeContextFormatter(
        "%(asctime_color)s %(node_id_color)s%(levelname_color)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Validate node_id for network mode
    if mode == "network" and not node_id:
        click.echo("Error: --id is required for --mode network", err=True)
        sys.exit(1)

    if report_dir:
        os.makedirs(report_dir, exist_ok=True)

    # Parse latency range
    try:
        if "-" in latency:
            parts = latency.split("-")
            if len(parts) != 2:
                click.echo(
                    f"Error: Invalid latency format '{latency}'. Expected 'min-max'", err=True
                )
                sys.exit(1)
            latency_range = (int(parts[0]), int(parts[1]))
        else:
            # Single value, use as both min and max
            val = int(latency)
            latency_range = (val, val)
    except ValueError as e:
        click.echo(f"Error: Invalid latency format '{latency}': {e}", err=True)
        sys.exit(1)

    # Run the appropriate mode
    try:
        if mode == "offline":
            asyncio.run(
                run_offline(
                    config, algorithm, format, timeout, report_dir, latency_range, startup_delay
                )
            )
        elif mode == "local":
            asyncio.run(
                run_local(
                    config,
                    algorithm,
                    format,
                    timeout,
                    port_base,
                    report_dir,
                    latency_range,
                    startup_delay,
                )
            )
        else:
            # mode == "network"
            if node_id is None:  # Should be caught by click, but check again
                click.echo("Error: --id is required for --mode network", err=True)
                sys.exit(1)
            asyncio.run(
                run_network(
                    config,
                    algorithm,
                    format,
                    timeout,
                    node_id,
                    port_base,
                    report_dir,
                    latency_range,
                    startup_delay,
                )
            )
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Interrupted by user")
    except Exception as e:
        logging.getLogger(__name__).error(f"Error: {e}", exc_info=verbose > 0)
        sys.exit(1)


if __name__ == "__main__":
    main()
