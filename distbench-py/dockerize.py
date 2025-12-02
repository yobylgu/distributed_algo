#!/usr/bin/env python3
"""
Generate docker-compose.yaml for running Python distbench nodes.

This script reads a distbench configuration file and generates a docker-compose.yaml
that runs each node as a separate container in network mode.
"""

import argparse
import sys
from pathlib import Path
from typing import Any, Optional

import yaml


def load_config(config_path: Path) -> dict[str, Any]:
    """Load the YAML configuration file.

    Args:
        config_path: Path to the configuration file

    Returns:
        Dictionary containing the configuration
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Filter out template keys (starting with _)
    return {k: v for k, v in config.items() if not k.startswith("_")}


def generate_compose_config(
    config: dict[str, Any],
    config_path: Path,
    algorithm: str,
    format_type: str = "json",
    timeout: int = 30,
    verbosity: int = 0,
    latency: str = "0-0",
    startup_delay: int = 0,
    report_dir: Optional[str] = None,
    port: int = 8000,
) -> dict[str, Any]:
    """Generate docker-compose configuration for Python nodes.

    Args:
        config: The loaded distbench configuration
        config_path: Path to the config file (for mounting)
        algorithm: Name of the algorithm to run
        format_type: Serialization format ('json' or 'msgpack')
        timeout: Timeout in seconds
        verbosity: Verbosity level (0-2)
        latency: Latency range in milliseconds (e.g., "0-0" or "10-50")
        startup_delay: Startup delay in milliseconds
        report_dir: Optional directory for reports
        port: Port number to use for all nodes (default: 8000)

    Returns:
        Dictionary representing the docker-compose configuration
    """
    if format_type not in ["json", "msgpack"]:
        raise ValueError(f"Python supports 'json' or 'msgpack' format, got '{format_type}'")

    compose_config: dict[str, Any] = {
        "services": {},
        "networks": {"distbench-net": {"driver": "bridge"}},
    }

    image_name = "distbench-python"
    dockerfile = "Dockerfile"

    # Build verbosity flag
    verbose_flag = ""
    if verbosity > 0:
        verbose_flag = "-" + "v" * min(verbosity, 3)

    # Get absolute path to config file
    config_abs_path = config_path.resolve()

    # Create a service for each node
    for node_id in config.keys():
        cmd = [
            "--config",
            "/config/config.yaml",
            "--algorithm",
            algorithm,
            "--mode",
            "network",
            "--id",
            node_id,
            "--format",
            format_type,
            "--timeout",
            str(timeout),
            "--port-base",
            str(port),
            "--latency",
            latency,
            "--startup-delay",
            str(startup_delay),
        ]
        if verbose_flag:
            cmd.append(verbose_flag)
        if report_dir:
            cmd.extend(["--report-dir", "/reports"])

        # Create service configuration
        service_config: dict[str, Any] = {
            "image": f"{image_name}:latest",
            "container_name": f"distbench-{node_id}",
            "networks": ["distbench-net"],
            "command": cmd,
            "volumes": [
                f"{config_abs_path}:/config/config.yaml:ro",
            ],
            "build": {
                "context": ".",
                "dockerfile": dockerfile,
            },
        }

        # Add report directory volume if specified
        if report_dir:
            report_abs_path = Path(report_dir).resolve()
            service_config["volumes"].append(f"{report_abs_path}:/reports")

        # Add environment variable for the node's info (informational)
        service_config["environment"] = [
            f"NODE_ID={node_id}",
            f"PORT={port}",
        ]

        compose_config["services"][node_id] = service_config

    return compose_config


def main() -> None:
    """Main entry point for the compose generator."""
    parser = argparse.ArgumentParser(
        description="Generate docker-compose.yaml for Python distbench",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate compose file
  python py/dockerize.py -c configs/echo.yaml -a echo -o docker-compose.yaml

  # Generate with verbose output
  python py/dockerize.py -c configs/echo.yaml -a echo -v

  # Generate with custom timeout and latency
  python py/dockerize.py -c configs/echo.yaml -a echo --timeout 60 --latency 10-50
        """,
    )

    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        required=True,
        help="Path to the distbench configuration YAML file",
    )

    parser.add_argument(
        "-a",
        "--algorithm",
        type=str,
        required=True,
        help="Name of the algorithm to run",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("docker-compose.yaml"),
        help="Output path for the generated docker-compose file (default: docker-compose.yaml)",
    )

    parser.add_argument(
        "-f",
        "--format",
        type=str,
        default="json",
        help="Serialization format (json or msgpack, default: json)",
    )

    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=30,
        help="Timeout in seconds (default: 30)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (-v, -vv, -vvv)",
    )

    parser.add_argument(
        "--latency",
        type=str,
        default="0-0",
        help="Network latency simulation in milliseconds (e.g., '10-50' for range, default: '0-0')",
    )

    parser.add_argument(
        "--startup-delay",
        type=int,
        default=600,
        help="Startup delay in milliseconds (default: 0)",
    )

    parser.add_argument(
        "--report-dir",
        type=str,
        help="Directory for storing node reports (will be mounted as volume)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port number for all nodes (default: 8000)",
    )

    args = parser.parse_args()

    # Validate config file exists
    if not args.config.exists():
        print(f"Error: Config file '{args.config}' not found", file=sys.stderr)
        sys.exit(1)

    try:
        # Load configuration
        config = load_config(args.config)

        # Generate docker-compose configuration
        compose_config = generate_compose_config(
            config=config,
            config_path=args.config,
            algorithm=args.algorithm,
            format_type=args.format,
            timeout=args.timeout,
            verbosity=args.verbose,
            latency=args.latency,
            startup_delay=args.startup_delay,
            report_dir=args.report_dir,
            port=args.port,
        )

        # Write to output file
        with open(args.output, "w") as f:
            yaml.dump(compose_config, f, default_flow_style=False, sort_keys=False)

        print(f"✓ Generated docker-compose configuration: {args.output}")
        print(f"  - Algorithm: {args.algorithm}")
        print(f"  - Nodes: {len(config)}")
        print(f"  - Port: {args.port}")
        print(f"  - Format: {args.format}")
        print(f"  - Timeout: {args.timeout}s")
        if args.latency != "0-0":
            print(f"  - Latency: {args.latency}ms")
        if args.startup_delay > 0:
            print(f"  - Startup delay: {args.startup_delay}ms")
        if args.report_dir:
            print(f"  - Reports: {args.report_dir}")
        print()
        print("To start the distributed system, run:")
        print(f"  sudo docker-compose -f {args.output} up --build")
        print()
        print("To stop and clean up:")
        print(f"  sudo docker-compose -f {args.output} down")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose > 0:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
