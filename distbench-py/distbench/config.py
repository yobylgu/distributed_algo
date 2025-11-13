"""Configuration loading and parsing.

This module handles loading and parsing YAML configuration files
for distributed algorithm execution.
"""

import logging
from typing import Any

import yaml

from distbench.community import PeerId

logger = logging.getLogger(__name__)


def load_config(path: str) -> dict[str, dict[str, Any]]:
    """Load configuration from a YAML file.

    The configuration file should have the format:
    ```yaml
    node_id:
      neighbours: [list of neighbor node IDs]
      host: "hostname or IP"
      port: port_number
      algorithm_specific_field: value
      ...
    ```

    Args:
        path: Path to the YAML configuration file

    Returns:
        Dictionary mapping node IDs to their configuration dictionaries

    Raises:
        FileNotFoundError: If the config file doesn't exist
        yaml.YAMLError: If the config file is malformed
    """
    with open(path) as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError("Config file must contain a dictionary")

    logger.trace(f"Loaded configuration for {len(config)} nodes")
    return config


def extract_neighbours(node_config: dict[str, Any], all_node_ids: set[str]) -> set[PeerId]:
    """Extract and validate neighbor list from node configuration.

    If the neighbours list is empty, returns all other nodes (fully connected).

    Args:
        node_config: Configuration dictionary for a single node
        all_node_ids: Set of all node IDs in the system

    Returns:
        Set of peer IDs that are neighbors of this node
    """
    neighbours = node_config.get("neighbours", [])

    if not neighbours:
        # Empty list means fully connected topology
        # Neighbors are all nodes except self (which will be excluded by caller)
        return {PeerId(nid) for nid in all_node_ids}

    return {PeerId(nid) for nid in neighbours}


def extract_algorithm_config(node_config: dict[str, Any]) -> dict[str, Any]:
    """Extract algorithm-specific configuration from node config.

    This removes standard framework fields (neighbours, host, port) and
    returns the remaining fields for the algorithm.

    Args:
        node_config: Configuration dictionary for a single node

    Returns:
        Dictionary of algorithm-specific configuration values
    """
    # Standard fields that are not algorithm config
    standard_fields = {"neighbours", "host", "port"}

    algorithm_config = {k: v for k, v in node_config.items() if k not in standard_fields}

    return algorithm_config
