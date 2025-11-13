"""Context management for node-specific logging.

This module provides a context variable for storing the current node ID,
which is used by the logging formatter to include node IDs in log messages.
"""

from contextvars import ContextVar

# Context variable to store current node ID
current_node_id: ContextVar[str | None] = ContextVar("current_node_id", default=None)
