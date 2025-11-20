import asyncio
import json
import logging
import random
import time
import uuid
from typing import Dict, Any, List, Set

from distbench import Algorithm, PeerId
from distbench.decorators import message, handler, config_field, distbench

logger = logging.getLogger(__name__)


# most basic struct i could think of
@message
class DMsg:
    msg_id: str
    source: str
    payload: str
    path: List[str]


@distbench
class DolevNaive(Algorithm):
    f: int = config_field(required=True)
    is_sender: bool = config_field(default=False)
    payload: str = config_field(default="hello")
    delay_min: float = config_field(default=0.0)
    delay_max: float = config_field(default=0.0)
    behavior_mode: str = config_field(default="HONEST")  # HONEST, BYZANTINE_SILENT, BYZANTINE_SPOOF
    num_messages: int = config_field(default=1)  # Number of messages to send (for volume testing)

    def __init__(self, config: Dict[str, Any], peers: Dict[PeerId, Any]):
        super().__init__()
        self.peers = peers
        self.paths: Dict[str, List[List[str]]] = {}
        self.delivered: Set[str] = set()
        self.neighbour_ids: Set[str] = set()
        self.expected_messages: int = 0
        self.messages_sent: int = 0

        # METRICS: Performance tracking
        self.start_time = time.time()
        self.total_messages_sent = 0          # All point-to-point sends
        self.messages_forwarded = 0           # Relay/forward count
        self.empty_paths_sent = 0             # MD.2 optimization messages -> in naive dolev this remains 0
        self.message_metrics: Dict[str, Dict] = {}  # msg_id -> {broadcast_time, delivery_time, ...}

    async def on_start(self) -> None:
        logger.info(f"[{self.id()}] starting with f={self.f}, behavior={self.behavior_mode} (NAIVE VERSION)")
        if not self.neighbour_ids:
            self.neighbour_ids = {str(pid) for pid in self.community.neighbours}
            print(f"✔ DOLEV NAIVE LOADED NEIGHBOURS: {self.neighbour_ids}")

        # BYZANTINE_SPOOF: Send fake message claiming another node is the source
        if self.behavior_mode == "BYZANTINE_SPOOF":
            # Choose a random peer to spoof as the source
            peer_ids = [str(p.peer_id) for p in self.peers.values()]
            if peer_ids:
                fake_source = peer_ids[0]  # Spoof first peer
                fake_msg_id = str(uuid.uuid4())
                logger.info(f"[{self.id()}] BYZANTINE SPOOF: Sending fake message claiming source={fake_source}")

                fake_msg = DMsg(
                    msg_id=fake_msg_id,
                    source=fake_source,  # Lying about the source!
                    payload="SPOOFED MESSAGE",
                    path=[],
                )

                for peer in self.peers.values():
                    if str(peer.peer_id) in self.neighbour_ids:
                        await self._apply_delay()
                        await peer.dolev_naive(fake_msg)

        if self.is_sender:
            # Send multiple messages for volume testing
            for i in range(self.num_messages):
                await self.broadcast_message(suffix=f"_{i}" if self.num_messages > 1 else "")
                self.messages_sent += 1
                # Add small delay between messages
                if i < self.num_messages - 1:
                    await asyncio.sleep(0.1)

        # Wait a bit for messages to propagate, then terminate
        await asyncio.sleep(2.0)
        logger.info(f"[{self.id()}] Terminating. Delivered {len(self.delivered)} messages, sent {self.messages_sent}")
        await self.terminate()

    async def on_exit(self) -> None:
        # Calculate latencies for delivered messages
        latencies = []
        for msg_id, metrics in self.message_metrics.items():
            if metrics.get("delivery_time") and metrics.get("broadcast_time"):
                latency_ms = (metrics["delivery_time"] - metrics["broadcast_time"]) * 1000
                latencies.append(latency_ms)

        # Compile comprehensive metrics
        metrics_output = {
            "node_id": str(self.id()),
            "total_messages_sent": self.total_messages_sent,
            "messages_forwarded": self.messages_forwarded,
            "empty_paths_sent": self.empty_paths_sent,
            "delivered_count": len(self.delivered),
            "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
            "min_latency_ms": min(latencies) if latencies else 0,
            "max_latency_ms": max(latencies) if latencies else 0,
            "is_sender": self.is_sender,
            "behavior_mode": self.behavior_mode,
            "f": self.f,
            "version": "naive"
        }

        # Output JSON for benchmark parsing
        logger.info(f"METRICS_JSON: {json.dumps(metrics_output)}")
        logger.info(f"[{self.id()}] finished")

    async def report(self) -> Dict[str, str]:
        return {"delivered_count": str(len(self.delivered))}

    async def _apply_delay(self) -> None:
        """Apply random network delay if configured."""
        if self.delay_max > 0:
            delay = random.uniform(self.delay_min, self.delay_max)
            await asyncio.sleep(delay)

    async def broadcast_message(self, suffix: str = "") -> None:
        msg_id = str(uuid.uuid4())
        self_id = str(self.id())

        msg = DMsg(
            msg_id=msg_id,
            source=self_id,
            payload=self.payload + suffix,
            path=[],
        )

        # METRICS: Record broadcast timestamp
        self.message_metrics[msg_id] = {
            "broadcast_time": time.time(),
            "delivery_time": None,
            "source": self_id,
            "is_sender": True
        }

        logger.info(f"[{self.id()}] BROADCAST START FOR -----> {msg_id} payload={msg.payload}")

        for peer in self.peers.values():
            if str(peer.peer_id) in self.neighbour_ids:
                await self._apply_delay()
                await peer.dolev_naive(msg)
                self.total_messages_sent += 1

        self.delivered.add(msg_id)

    # NAIVE VERSION: No optimizations - simple forwarding and path tracking
    @handler
    async def dolev_naive(self, src: PeerId, msg: DMsg) -> None:
        # BYZANTINE_SILENT: Drop all messages
        if self.behavior_mode == "BYZANTINE_SILENT":
            logger.info(f"[{self.id()}] BYZANTINE SILENT: Dropping message from {src}")
            return

        msg_id = msg.msg_id
        src_id = str(src)

        # Skip non-neighbor
        if src_id not in self.neighbour_ids:
            return

        # Add source to path
        new_path = msg.path + [src_id]

        logger.info(f"[{self.id()}] recv {msg_id} via {new_path}")

        # Initialize paths for this message if needed
        if msg_id not in self.paths:
            self.paths[msg_id] = []

        # Store path if new
        if new_path not in self.paths[msg_id]:
            self.paths[msg_id].append(new_path)

        # Forward to all neighbors not in path (basic flooding)
        for peer in self.peers.values():
            peer_str = str(peer.peer_id)
            if peer_str not in new_path:
                await self._apply_delay()
                await peer.dolev_naive(
                    DMsg(
                        msg_id=msg_id,
                        source=msg.source,
                        payload=msg.payload,
                        path=new_path,
                    )
                )
                self.total_messages_sent += 1
                self.messages_forwarded += 1

        # Check if we can deliver
        await self.deliver(msg)

    async def deliver(self, msg: DMsg) -> None:
        msg_id = msg.msg_id

        if msg_id in self.delivered:
            return

        paths = self.paths.get(msg_id, [])

        if self.has_f_plus_one_disjoint(paths, msg.source):
            logger.info(f"[{self.id()}] DELIVER {msg_id} payload={msg.payload}")
            self.delivered.add(msg_id)

            # METRICS: Record delivery time
            if msg_id not in self.message_metrics:
                self.message_metrics[msg_id] = {"broadcast_time": None, "source": msg.source, "is_sender": False}
            self.message_metrics[msg_id]["delivery_time"] = time.time()

            # NAIVE: No empty path optimization - messages continue to flood

    def has_f_plus_one_disjoint(self, paths: List[List[str]], source: str) -> bool:
        needed = self.f + 1
        chosen: List[Set[str]] = []

        for path in paths:
            internal = set(path)
            # sender and receiver cant be byzantine (assumed)
            internal.discard(source)
            internal.discard(str(self.id()))

            if any(internal & used for used in chosen):
                continue

            chosen.append(internal)

            if len(chosen) >= needed:
                return True

        return False
