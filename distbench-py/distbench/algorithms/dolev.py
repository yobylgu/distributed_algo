import logging
import uuid
from typing import Dict, Any, List, Set
import random
import asyncio
import time
import json
import networkx as nx

from distbench import Algorithm, PeerId
from distbench.decorators import message, handler, config_field, distbench


@message
class DMsg:
    msg_id: str
    source: str
    payload: str
    path: List[str]


@distbench
class Dolev(Algorithm):
    is_sender: bool = config_field(default=False)
    payload: str = config_field(default="hello")
    max_delay: float = config_field(default=0.5)
    behavior_mode: str = config_field(default="HONEST")
    num_messages: int = config_field(default=1)
    f: int = config_field(default=1)

    def __init__(self, config: Dict[str, Any], peers: Dict[PeerId, Any]):
        super().__init__()
        self.peers = peers
        self.neighbour_ids: Set[str] = set()
        # self.f is set by config_field, no need to reassign
        self.paths: Dict[str, List[List[str]]] = {}
        self.delivered: Set[str] = set()
        self.forwarded_empty: Set[str] = set()
        self.neighbour_delivered: Dict[str, Set[str]] = {}
        self.message_metrics: Dict[str, Dict] = {}
        self.total_messages_sent = 0
        self.messages_forwarded = 0
        self.logger = logging.getLogger("PLACEHOLDER")

    def _safe_id(self) -> str:
        """Get node ID safely, returning 'unknown' if not set yet."""
        """tries to get parent id if parent exist, preserves logging ownerhip"""
        try:
            if self._parent:
                if hasattr(self._parent, "_safe_id"):
                    return str(self._parent._safe_id())
                return str(self._parent.id())
            return str(self.id())
        except RuntimeError:
            return "unknown"

    async def on_start(self):
        self.logger = logging.getLogger(f"dolev-{self._safe_id()}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if not self.logger.handlers:
            fh = logging.FileHandler(f"{self._safe_id()}.txt", mode="w")
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s : %(message)s")
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

        # When used as child algorithm, community might not be set - parent will set neighbour_ids manually
        if self.community:
            self.neighbour_ids = {str(pid) for pid in self.community.neighbours}
            print(f" DOLEV LOADED WITH NEIGHBOURS: {self.neighbour_ids}")

        # BYZANTINE_SPOOF: Send fake message claiming another node is the source
        # Only run here if standalone (not child algorithm) - otherwise parent calls trigger_byzantine_spoof()
        if self.behavior_mode == "BYZANTINE_SPOOF" and self._parent is None:
            await self.trigger_byzantine_spoof()

    async def trigger_byzantine_spoof(self):
        """Trigger Byzantine spoof behavior - can be called by parent after neighbor_ids are set."""
        if self.behavior_mode != "BYZANTINE_SPOOF":
            return

        peer_ids = [str(p.peer_id) for p in self.peers.values()]
        if peer_ids and self.neighbour_ids:
            fake_source = peer_ids[0]  # Spoof first peer
            fake_msg_id = str(uuid.uuid4())
            self.logger.info(f"[{self._safe_id()}] BYZANTINE SPOOF: Sending fake message claiming source={fake_source}")

            fake_msg = DMsg(fake_msg_id, fake_source, "SPOOFED MESSAGE", [])

            for peer in self.peers.values():
                if str(peer.peer_id) in self.neighbour_ids:
                    await self.delay()
                    await peer.dolev(fake_msg)

        # Only run standalone Dolev logic if not used as a child algorithm
        if self._parent is None:
            if self.is_sender:
                for i in range(self.num_messages):
                    await self.broadcast_message()
                    if i < self.num_messages - 1:
                        await asyncio.sleep(0.1)  # Small delay between sequential broadcasts

            # Wait longer for sequential broadcasts to propagate (1.5s per message + 2s buffer)
            wait_time = 2.0 + (self.num_messages * 1.5) if self.is_sender else 10.0
            await asyncio.sleep(wait_time)

            self.logger.info(f"[{self._safe_id()}] Terminating. Delivered {len(self.delivered)} messages.")

            latencies = []
            for msg_id, data in self.message_metrics.items():
                if data.get("delivery_time") and data.get("broadcast_time"):
                    latencies.append((data["delivery_time"] - data["broadcast_time"]) * 1000)

            metrics_output = {
                "node_id": str(self._safe_id()),
                "neighbours_per_node": len(self.neighbour_ids),
                "f": self.f,
                "is_sender": self.is_sender,
                "total_messages_sent": self.total_messages_sent,
                "messages_forwarded": self.messages_forwarded,
                "delivered_count": len(self.delivered),
                "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
                "min_latency_ms": min(latencies) if latencies else 0,
                "max_latency_ms": max(latencies) if latencies else 0,
            }

            self.logger.info(f"METRICS_JSON: {json.dumps(metrics_output)}")
            await self.terminate()
        else:
            # When used as a child, just log that we're ready
            self.logger.info(f"[{self._safe_id()}] Dolev child ready, controlled by parent algorithm")

    async def delay(self):
        await asyncio.sleep(random.uniform(0, 0))

    def _ensure_logger(self):
        # If parent says split logs
        if getattr(self._parent, "split_logs", False):
            if not self.logger or not self.logger.handlers:
                self.logger = logging.getLogger(f"dolev-{self._safe_id()}")
                self.logger.setLevel(logging.INFO)
                self.logger.propagate = False

                self.logger = logging.getLogger(f"dolev-{self._safe_id()}")
                self.logger.setLevel(logging.INFO)
                self.logger.propagate = False
                self.logger.handlers.clear()

                fh = logging.FileHandler(f"dolev-{self._safe_id()}.txt", mode="w")
                fh.setLevel(logging.INFO)
                formatter = logging.Formatter("%(asctime)s [%(name)s] %(message)s")
                fh.setFormatter(formatter)

                self.logger.addHandler(fh)


        else:
            # Use parent's logger (unified log)
            self.logger = self._parent.logger if self._parent else self.logger

    async def yes_daddy_bracha(self, bracha_msg):
        """Broadcast a Bracha message via Dolev protocol.
        
        Each Dolev broadcast needs a unique msg_id that identifies both:
        1. The original Bracha message (bracha_msg.msg_id)
        2. The node broadcasting this specific Dolev message (sender_id)
        
        This is because multiple nodes may broadcast ECHOs/READYs for the same
        Bracha message, and Dolev tracks paths per msg_id.
        """
        #parent logger setup on entry
        self._ensure_logger()

        # Get neighbors from parent's community when used as child algorithm
        if not self.neighbour_ids:
            if self._parent and self._parent.community:
                self.neighbour_ids = {str(pid) for pid in self._parent.community.neighbours}
            elif self.community:
                self.neighbour_ids = {str(pid) for pid in self.community.neighbours}
        sender_id = self._safe_id()

        # Create unique Dolev msg_id: combines Bracha msg_id + phase + sender
        # This ensures each node's broadcast of ECHO/READY is tracked separately
        dolev_msg_id = f"{bracha_msg.msg_id}:{bracha_msg.phase}:{sender_id}"
        
        dmsg = DMsg(dolev_msg_id, sender_id, bracha_msg, [])
        self.logger.info(f"[{self._safe_id()}] Dolev broadcast: {bracha_msg.phase} for Bracha msg {bracha_msg.msg_id}")
        
        for peer in self.peers.values():
            if str(peer.peer_id) in self.neighbour_ids:
                await self.delay()
                await peer.dolev(dmsg)
                if self._parent:
                    self._parent.messages_sent += 1

    async def broadcast_message(self):
        msg_id = str(uuid.uuid4())
        self_id = str(self._safe_id())
        self.message_metrics[msg_id] = {
            "broadcast_time": time.time(),
            "delivery_time": None,
        }

        msg = DMsg(msg_id, self_id, self.payload, [])
        self.logger.info(f"[{self._safe_id()}] BROADCAST ----> {msg_id}")

        # async func here messes up (race condition?), origin can deliver to itself offline (maybe)
        if msg_id not in self.delivered:
            self.delivered.add(msg_id)
            self.message_metrics[msg_id]["delivery_time"] = time.time()
            self.logger.info(f"[{self._safe_id()}] DELIVER (sender local) {msg_id} payload='{self.payload}'")

        # BYZANTINE_SELECTIVE: Only send to subset of neighbors (breaks totality)
        if self.behavior_mode == "BYZANTINE_SELECTIVE":
            # Only send to first half of neighbors (rounded down)
            target_neighbors = sorted(list(self.neighbour_ids))[:len(self.neighbour_ids) // 2]
            excluded = sorted(list(self.neighbour_ids))[len(self.neighbour_ids) // 2:]
            self.logger.info(f"[{self._safe_id()}] BYZANTINE SELECTIVE: Sending to {target_neighbors}, excluding {excluded}")

            for peer in self.peers.values():
                if str(peer.peer_id) in target_neighbors:
                    await peer.dolev(msg)
                    self.total_messages_sent += 1
                    if self._parent:
                        self._parent.messages_sent += 1
        else:
            # Normal broadcast to all neighbors
            for peer in self.peers.values():
                if str(peer.peer_id) in self.neighbour_ids:
                    await peer.dolev(msg)
                    self.total_messages_sent += 1
                    if self._parent:
                        self._parent.messages_sent += 1

    @handler
    async def dolev(self, src: PeerId, msg: DMsg):
        #parent logger setup in case of receive from other children
        self._ensure_logger()

        # BYZANTINE_SILENT: Drop all messages
        if self.behavior_mode == "BYZANTINE_SILENT":
            self.logger.info(f"[{self._safe_id()}] BYZANTINE SILENT: Dropping message from {src}")
            return

        # Ensure we have neighbor IDs (get from parent if child algorithm)
        if not self.neighbour_ids:
            if self._parent and self._parent.community:
                self.neighbour_ids = {str(pid) for pid in self._parent.community.neighbours}
            elif self.community:
                self.neighbour_ids = {str(pid) for pid in self.community.neighbours}

        src_id = str(src)
        if src_id not in self.neighbour_ids:
            return

        msg_id = msg.msg_id

        # NOTE: RC-INTEGRITY check disabled because it conflicts with MD2 empty forward optimization.
        # MD2 sends empty-path messages claiming original source to inform neighbors of delivery.
        # This is legitimate behavior from honest nodes, not spoofing.
        # The path-based delivery verification (distPaths with f+1 disjoint paths) provides
        # the actual Byzantine fault tolerance.
        
        # Original RC-INTEGRITY check (kept for reference):
        # if not msg.path:  # Empty path = direct from source claim
        #     if src_id != msg.source:
        #         # This could be MD2 empty forward from a neighbor who delivered
        #         # OR it could be Byzantine spoofing - we can't tell without tracking
        #         # which neighbors have delivered. For now, allow it and rely on
        #         # path-based verification.
        #         pass

        #MD5 stop all activity for msg, we done
        if msg.msg_id in self.delivered and msg.msg_id in self.forwarded_empty:
            #self.logger.info(f"[{self._safe_id()}] MD5: already delivered and forwarded empty, ignore {msg_id}") --> debug only
            return

        if msg_id not in self.message_metrics:
            self.message_metrics[msg_id] = {
                "broadcast_time": time.time(),
                "delivery_time": None,
            }

        # MD1 call
        if src_id == msg.source and msg_id not in self.delivered:
            await self.my_deliver(msg, True)

        #ND3: handle empty from non-source
        if not msg.path and src_id != msg.source:
            self.logger.info(f"[{self._safe_id()}] recv EMPTY from {src_id} for {msg_id}")
            self.neighbour_delivered.setdefault(msg_id, set()).add(src_id)
            return

        new_path = msg.path + [src_id]
        self.logger.info(f"[{self._safe_id()}] recv {msg_id} from {src_id} path={msg.path}")
        # MD4
        if src_id in self.neighbour_delivered.get(msg_id, set()) and src_id in msg.path:
            self.logger.info(f"[{self._safe_id()}] MD4: drop second hop from delivered neighbor {src_id} for {msg_id}")
            return

        self.paths.setdefault(msg_id, [])
        if new_path not in self.paths[msg_id]:
            self.paths[msg_id].append(new_path)

        delivered_neighbours = self.neighbour_delivered.get(msg_id, set())

        for peer in self.peers.values():
            pid = str(peer.peer_id)
            if pid in self.neighbour_ids and pid not in new_path and pid not in delivered_neighbours:
                await self.delay()
                await peer.dolev(DMsg(msg_id, msg.source, msg.payload, new_path))
                self.messages_forwarded += 1
                self.total_messages_sent += 1
                if self._parent:
                    self._parent.messages_sent += 1

        await self.my_deliver(msg)

    async def my_deliver(self, msg: DMsg, direct: bool = False):
        msg_id = msg.msg_id
        # ignore alr delivered + MD2 ignore forwarded empty so no repeats
        if msg_id in self.delivered and msg_id in self.forwarded_empty:
            return

        # MD1 deliver directly
        if direct and msg_id not in self.delivered:
            self.delivered.add(msg_id)
            self.message_metrics.setdefault(msg_id, {})["delivery_time"] = time.time()
            self.logger.info(f"[{self._safe_id()}] DELIVER (MD1 direct) {msg_id} payload='{msg.payload}' source={msg.source}")

            # up to daddy bracha (directly)
            if self._parent:
                await self._parent.dolev_deliver(src=msg.source, msg=msg)
                self.total_messages_sent += 1
                self.messages_forwarded += 1
                self._parent.messages_sent += 1

        # deliver needs if to guard empty forward for one exec
        elif msg_id not in self.delivered:
            paths = self.paths.get(msg_id, [])
            if self.distPaths(paths, msg.source):
                self.delivered.add(msg_id)
                self.neighbour_delivered.setdefault(msg_id, set()).add(str(self._safe_id()))
                self.message_metrics[msg_id]["delivery_time"] = time.time()
                self.logger.info(f"[{self._safe_id()}] DELIVER {msg_id} payload='{msg.payload}' source={msg.source}")
                # up to daddy bracha (standard)
                if self._parent:
                    await self._parent.dolev_deliver(src=msg.source, msg=msg)

        else:
            return

        # MD2 empty forward
        if msg_id not in self.forwarded_empty:
            self.neighbour_delivered.setdefault(msg_id, set()).add(str(self._safe_id()))
            empty = DMsg(msg_id, msg.source, msg.payload, [])

            for peer in self.peers.values():
                if str(peer.peer_id) in self.neighbour_ids:
                    await self.delay()
                    await peer.dolev(empty)
                    self.messages_forwarded += 1
                    self.total_messages_sent += 1
                    if self._parent:
                        self._parent.messages_sent += 1

            self.forwarded_empty.add(msg_id)

    def distPaths(self, paths: List[List[str]], source: str) -> bool:
        """
        Check if there are f+1 node-disjoint paths from source to this node.
        Uses NetworkX's node_connectivity which implements max-flow (Menger's theorem).
        This is mathematically correct, unlike the previous greedy approach.
        """
        needed = self.f + 1

        if not paths:
            return False

        # Build graph from received paths
        G = nx.Graph()  # Undirected for node-disjoint paths

        # Add all edges from paths
        for path in paths:
            full_path = [source] + path + [str(self._safe_id())]
            for i in range(len(full_path) - 1):
                G.add_edge(full_path[i], full_path[i+1])

        # Check if source and destination are connected
        if not nx.has_path(G, source, str(self._safe_id())):
            return False

        # Use Menger's theorem: node_connectivity gives max node-disjoint paths
        try:
            disjoint_count = nx.node_connectivity(G, source, str(self._safe_id()))
            self.logger.debug(f"[{self._safe_id()}] Found {disjoint_count} disjoint paths from {source}, need {needed}")
            return disjoint_count >= needed
        except nx.NetworkXError:
            return False
