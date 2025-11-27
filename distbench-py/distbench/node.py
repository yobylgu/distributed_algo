"""Node implementation for managing distributed algorithm execution.

This module implements the Node class which coordinates:
- Node lifecycle state machine
- Startup and shutdown synchronization with peers
- Message routing to algorithm handlers
- Cryptographic key exchange
"""

import asyncio
import json
import logging
import time
from typing import Generic, TypeVar

from distbench.algorithm import Algorithm
from distbench.community import Community, NodeStatus, PeerId
from distbench.context import current_node_id
from distbench.encoding.format import Format
from distbench.messages import MessageType, NodeMessage
from distbench.signing import KeyPair
from distbench.transport.base import Address, Server, Transport

logger = logging.getLogger(__name__)

A = TypeVar("A", bound=Address)
T = TypeVar("T", bound=Transport)


class Node(Server, Generic[T, A]):
    """A node in the distributed system.

    The Node class manages:
    - Lifecycle state transitions
    - Synchronization with other nodes
    - Message handling and routing
    - Algorithm execution
    """

    def __init__(
        self,
        node_id: PeerId,
        community: Community[T, A],
        algorithm: Algorithm,
        format: Format,
        report_dir: str | None = None,
    ) -> None:
        """Initialize a node.

        Args:
            node_id: This node's unique identifier
            community: Community managing peer relationships
            algorithm: Algorithm instance to execute
            format: Serialization format for messages
            report_dir: Optional directory for storing report
        """
        self.id = node_id
        self.community = community
        self.algorithm = algorithm
        self.format = format
        self.report_dir = report_dir

        self.status = NodeStatus.NOT_STARTED
        self._status_lock = asyncio.Lock()
        self._status_changed = asyncio.Event()

        # Cryptographic key pair for this node
        self.keypair = KeyPair(node_id)

        # Metrics tracking
        self._start_time: float | None = None
        self._end_time: float | None = None
        self._messages_received = 0
        self._bytes_received = 0

        # Initialize algorithm with node info
        self.algorithm._set_node_id(node_id)
        self.algorithm._set_total_nodes(len(community.peers) + 1)  # +1 for self
        self.algorithm._set_keypair(self.keypair)
        self.algorithm._set_format(format)
        self.algorithm.community = community

        logger.trace(f"Node {node_id} initialized")

    async def _set_status(self, status: NodeStatus) -> None:
        """Update node status and notify waiters.

        Args:
            status: New status to set
        """
        async with self._status_lock:
            old_status = self.status
            self.status = status
            if old_status != status:
                logger.trace(f"Node {self.id} status: {old_status.value} -> {status.value}")
            self._status_changed.set()
            self._status_changed = asyncio.Event()  # Reset for next wait

    async def _monitor(self) -> None:
        """Monitor node lifecycle and coordinate state transitions.

        This coroutine handles the state machine for node startup and shutdown,
        coordinating with other nodes through status messages.
        """
        while True:
            # Wait for status change or just check current status
            if self.status == NodeStatus.NOT_STARTED:
                await self._status_changed.wait()
                continue

            if self.status == NodeStatus.KEY_SHARING:
                # Broadcast our public key to all peers
                pubkey_msg = NodeMessage.pubkey(self.keypair.public_key_bytes())
                tasks = []
                for conn in self.community.connections.values():
                    tasks.append(conn.cast(pubkey_msg))

                await asyncio.gather(*tasks, return_exceptions=True)
                logger.trace(f"Node {self.id} broadcasted public key")

                # Wait for all peer keys
                while not await self.community.has_all_keys():
                    await asyncio.sleep(0.1)

                logger.trace(f"Node {self.id} received all public keys")
                await self._set_status(NodeStatus.STARTING)

            elif self.status == NodeStatus.STARTING:
                # Broadcast Started message to all peers
                started_msg = NodeMessage.started()
                tasks = []
                for conn in self.community.connections.values():
                    tasks.append(conn.cast(started_msg))

                await asyncio.gather(*tasks, return_exceptions=True)
                logger.trace(f"Node {self.id} broadcasted Started")

                # Wait for all peers to be running
                while True:
                    await asyncio.sleep(0.01)  # Yield to allow message handling
                    statuses = await self.community.statuses()
                    if all(s == NodeStatus.RUNNING for s in statuses.values()):
                        await self._set_status(NodeStatus.RUNNING)
                        break

            elif self.status == NodeStatus.RUNNING:
                # Call algorithm's on_start hook
                logger.trace(f"Node {self.id} running algorithm")
                # Start timing
                if self._start_time is None:
                    self._start_time = time.time()
                try:
                    await self.algorithm.on_start()
                except Exception as e:
                    logger.error(f"Error in algorithm on_start: {e}", exc_info=True)

                await self._status_changed.wait()

            elif self.status == NodeStatus.STOPPING:
                # Stop timing
                if self._end_time is None:
                    self._end_time = time.time()

                # Broadcast Finished message to all peers
                finished_msg = NodeMessage.finished()
                tasks = []
                for conn in self.community.connections.values():
                    tasks.append(conn.cast(finished_msg))

                await asyncio.gather(*tasks, return_exceptions=True)
                logger.trace(f"Node {self.id} broadcasted Finished")

                # Wait for all peers to terminate
                while True:
                    statuses = await self.community.statuses()
                    if all(s == NodeStatus.TERMINATED for s in statuses.values()):
                        await self._set_status(NodeStatus.TERMINATED)
                        break
                    await asyncio.sleep(0.1)

            elif self.status == NodeStatus.TERMINATED:
                # Call algorithm's on_exit hook
                logger.trace(f"Node {self.id} terminated")
                try:
                    await self.algorithm.on_exit()
                except Exception as e:
                    logger.error(f"Error in algorithm on_exit: {e}", exc_info=True)
                break

    async def handle(self, addr: A, msg: bytes) -> bytes | None:
        """Handle an incoming message from a peer.

        This implements the Server protocol for the transport layer.

        Args:
            addr: Source address of the message
            msg: Serialized message bytes

        Returns:
            Response bytes, or None for cast messages
        """
        peer_id = self.community.id_of(addr)
        if peer_id is None:
            logger.warning(f"Received message from unknown peer: {addr}")
            return None

        try:
            msg_type, type_id, payload = NodeMessage.deserialize(msg)
        except Exception as e:
            logger.error(f"Failed to deserialize message: {e}")
            return None

        if msg_type == MessageType.PUBKEY:
            # Store peer's public key
            if payload:
                await self.community.add_key(peer_id, payload)
                logger.trace(f"Received public key from {peer_id}")
            return None

        elif msg_type == MessageType.STARTED:
            # Mark peer as running
            await self.community.set_status(peer_id, NodeStatus.RUNNING)

            # If we're also starting, check if all peers are ready
            if self.status == NodeStatus.STARTING:
                statuses = await self.community.statuses()
                if all(s == NodeStatus.RUNNING for s in statuses.values()):
                    await self._set_status(NodeStatus.RUNNING)

            return None

        elif msg_type == MessageType.FINISHED:
            # Mark peer as terminated
            await self.community.set_status(peer_id, NodeStatus.TERMINATED)

            # If we're also stopping, check if all peers are done
            if self.status == NodeStatus.STOPPING:
                statuses = await self.community.statuses()
                if all(s == NodeStatus.TERMINATED for s in statuses.values()):
                    await self._set_status(NodeStatus.TERMINATED)

            return None

        elif msg_type == MessageType.ALGORITHM:
            # Route to algorithm handler
            if type_id is None or payload is None:
                logger.error("Algorithm message missing type_id or payload")
                return None

            # Track metrics for algorithm messages
            self._messages_received += 1
            self._bytes_received += len(payload)

            try:
                return await self.algorithm.handle(peer_id, type_id, payload, self.format)
            except Exception as e:
                logger.error(f"Error handling algorithm message: {e}", exc_info=True)
                return None

        return None

    async def start(self, stop_event: asyncio.Event, startup_delay: int = 0) -> None:
        """Start the node and run until completion.

        This method:
        1. Starts the monitor task for lifecycle management
        2. Starts the transport server for incoming connections
        3. Applies startup delay if configured
        4. Initiates the key sharing phase
        5. Waits for algorithm completion or external stop signal

        Args:
            stop_event: Event that signals external shutdown request
            startup_delay: Delay in milliseconds before starting (default: 0)
        """
        # Set the node ID in context for logging
        current_node_id.set(str(self.id))
        await self.community.add_key(self.id, self.keypair.public_key_bytes())

        logger.trace(f"Node {self.id} starting...")

        # Start monitor task
        monitor_task = asyncio.create_task(self._monitor())

        # Start serving incoming connections
        serve_task = asyncio.create_task(self.community.transport.serve(self, stop_event))

        # Give servers time to start
        await asyncio.sleep(0.5)

        # Apply startup delay if configured
        if startup_delay > 0:
            logger.trace(f"Node {self.id} delaying startup by {startup_delay}ms")
            await asyncio.sleep(startup_delay / 1000.0)

        # Transition to key sharing phase
        await self._set_status(NodeStatus.KEY_SHARING)

        # Create task for algorithm termination
        async def wait_for_algorithm() -> None:
            await self.algorithm.terminated()

        algorithm_task = asyncio.create_task(wait_for_algorithm())

        # Wait for completion
        done, pending = await asyncio.wait(
            [monitor_task, serve_task, algorithm_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        logger.trace(f"Node {self.id} algorithm completed, initiating shutdown")
        await self._set_status(NodeStatus.STOPPING)

        # Cancel serve task and close connections
        if serve_task not in done:
            serve_task.cancel()
            from contextlib import suppress

            with suppress(asyncio.CancelledError):
                await serve_task

        await self.community.close_all()

        logger.trace(f"Node {self.id} stopped")

        # Generate and display combined metrics report
        try:
            # Calculate elapsed time
            elapsed_time = 0
            if self._start_time is not None and self._end_time is not None:
                elapsed_time = int(self._end_time - self._start_time)

            # Get algorithm report
            algorithm_report = await self.algorithm.report()

            # Get algorithm name from class
            algorithm_name = self.algorithm.__class__.__name__

            # Build combined metrics
            metrics = {
                "elapsed_time": elapsed_time,
                "messages_received": self._messages_received,
                "bytes_received": self._bytes_received,
                "algorithm": algorithm_name,
                "details": algorithm_report or {},
            }

            metrics_json = json.dumps(metrics, separators=(",", ":"))
            if self.report_dir:
                report_path = f"{self.report_dir}/{self.id}.json"

                with open(report_path, "a") as f:
                    f.write(metrics_json + "\n")
            else:
                logger.info(f"Algorithm report for {self.id}: {metrics_json}")
        except Exception as e:
            logger.warning(f"Failed to generate algorithm report: {e}")
