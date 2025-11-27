GLOBAL_F = 1
import logging
import uuid
from typing import Dict, Any, List, Set
import random
import asyncio
import time
import json

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

    def __init__(self, config: Dict[str, Any], peers: Dict[PeerId, Any]):
        super().__init__()
        self.peers = peers
        self.neighbour_ids: Set[str] = set()
        self.f = GLOBAL_F
        self.paths: Dict[str, List[List[str]]] = {}
        self.delivered: Set[str] = set()
        self.forwarded_empty: Set[str] = set()
        self.neighbour_delivered: Dict[str, Set[str]] = {}
        self.message_metrics: Dict[str, Dict] = {}
        self.total_messages_sent = 0
        self.messages_forwarded = 0

    async def on_start(self):
        self.logger = logging.getLogger(f"dolev-{self.id()}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if not self.logger.handlers:
            fh = logging.FileHandler(f"{self.id()}.txt", mode="w")
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s : %(message)s")
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

        self.neighbour_ids = {str(pid) for pid in self.community.neighbours}
        print(f" DOLEV LOADED WITH NEIGHBOURS: {self.neighbour_ids}")

        # BYZANTINE_SPOOF: Send fake message claiming another node is the source
        if self.behavior_mode == "BYZANTINE_SPOOF":
            peer_ids = [str(p.peer_id) for p in self.peers.values()]
            if peer_ids:
                fake_source = peer_ids[0]  # Spoof first peer
                fake_msg_id = str(uuid.uuid4())
                self.logger.info(f"[{self.id()}] BYZANTINE SPOOF: Sending fake message claiming source={fake_source}")

                fake_msg = DMsg(fake_msg_id, fake_source,"SPOOFED MESSAGE",[],)

                for peer in self.peers.values():
                    if str(peer.peer_id) in self.neighbour_ids:
                        await self.delay()
                        await peer.dolev(fake_msg)

        if self.is_sender:
            await self.broadcast_message()

        await asyncio.sleep(5.0)

        self.logger.info(f"[{self.id()}] Terminating. Delivered {len(self.delivered)} messages.")

        latencies = []
        for msg_id, data in self.message_metrics.items():
            if data.get("delivery_time") and data.get("broadcast_time"):
                latencies.append((data["delivery_time"] - data["broadcast_time"]) * 1000)

        metrics_output = {
            "node_id": str(self.id()),
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

    async def delay(self):
        await asyncio.sleep(random.uniform(0, self.max_delay))

    async def broadcast_message(self):
        msg_id = str(uuid.uuid4())
        self_id = str(self.id())
        self.message_metrics[msg_id] = {
            "broadcast_time": time.time(),
            "delivery_time": None,
        }

        msg = DMsg(msg_id, self_id, self.payload, [])
        self.logger.info(f"[{self.id()}] BROADCAST ----> {msg_id}")

        # async func here messes up (race condition?), origin can deliver to itself offline (maybe)
        if msg_id not in self.delivered:
            self.delivered.add(msg_id)
            self.message_metrics[msg_id]["delivery_time"] = time.time()
            self.logger.info(f"[{self.id()}] DELIVER (sender local) {msg_id}")

        for peer in self.peers.values():
            if str(peer.peer_id) in self.neighbour_ids:
                await peer.dolev(msg)
                self.total_messages_sent += 1

    @handler
    async def dolev(self, src: PeerId, msg: DMsg):
        # BYZANTINE_SILENT: Drop all messages
        if self.behavior_mode == "BYZANTINE_SILENT":
            self.logger.info(f"[{self.id()}] BYZANTINE SILENT: Dropping message from {src}")
            return

        src_id = str(src)
        if src_id not in self.neighbour_ids:
            return

        msg_id = msg.msg_id

        #MD5 stop all activity for msg, we done
        if msg.msg_id in self.delivered and msg.msg_id in self.forwarded_empty:
            #self.logger.info(f"[{self.id()}] MD5: already delivered and forwarded empty, ignore {msg_id}") --> debug only
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
            self.logger.info(f"[{self.id()}] recv EMPTY from {src_id} for {msg_id}")
            self.neighbour_delivered.setdefault(msg_id, set()).add(src_id)
            return

        new_path = msg.path + [src_id]
        self.logger.info(f"[{self.id()}] recv {msg_id} from {src_id} path={msg.path}")
        # MD4
        if src_id in self.neighbour_delivered.get(msg_id, set()) and src_id in msg.path:
            self.logger.info(f"[{self.id()}] MD4: drop second hop from delivered neighbor {src_id} for {msg_id}")
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
            self.logger.info(f"[{self.id()}] DELIVER (MD1 direct) {msg_id}")

        # deliver needs if to guard empty forward for one exec
        elif msg_id not in self.delivered:
            paths = self.paths.get(msg_id, [])
            if self.distPaths(paths, msg.source):
                self.delivered.add(msg_id)
                self.neighbour_delivered.setdefault(msg_id, set()).add(str(self.id()))
                self.message_metrics[msg_id]["delivery_time"] = time.time()
                self.logger.info(f"[{self.id()}] DELIVER {msg_id}")
        else:
            return

        # MD2 empty forward
        if msg_id not in self.forwarded_empty:
            self.neighbour_delivered.setdefault(msg_id, set()).add(str(self.id()))
            empty = DMsg(msg_id, msg.source, msg.payload, [])

            for peer in self.peers.values():
                if str(peer.peer_id) in self.neighbour_ids:
                    await self.delay()
                    await peer.dolev(empty)
                    self.messages_forwarded += 1
                    self.total_messages_sent += 1

            self.forwarded_empty.add(msg_id)
            self.paths[msg_id] = []

    def distPaths(self, paths: List[List[str]], source: str) -> bool:
        needed = self.f + 1
        chosen: List[Set[str]] = []

        for path in paths:
            internal = set(path)
            internal.discard(source)
            internal.discard(str(self.id()))

            if any(internal & used for used in chosen):
                continue
            chosen.append(internal)

            if len(chosen) >= needed:
                return True

        return False
