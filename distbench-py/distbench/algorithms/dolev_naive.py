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
class DolevNaive(Algorithm):
    is_sender: bool = config_field(default=False)
    payload: str = config_field(default="hello")
    max_delay: float = config_field(default=0.5)

    def __init__(self, config: Dict[str, Any], peers: Dict[PeerId, Any]):
        super().__init__()
        self.peers = peers
        self.neighbour_ids: Set[str] = set()
        self.f = GLOBAL_F

        self.paths: Dict[str, List[List[str]]] = {}
        self.delivered: Set[str] = set()

        self.message_metrics: Dict[str, Dict] = {}
        self.total_messages_sent = 0
        self.messages_forwarded = 0

    async def on_start(self):
        self.logger = logging.getLogger(f"dolev-naive-{self.id()}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if not self.logger.handlers:
            fh = logging.FileHandler(f"{self.id()}.txt", mode="w")
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter(
                "%(asctime)s [%(name)s] %(levelname)s : %(message)s"
            )
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

        self.neighbour_ids = {str(pid) for pid in self.community.neighbours}
        print(f" DOLEV LOADED WITH NEIGHBOURS: {self.neighbour_ids}")

        if self.is_sender:
            await self.broadcast_message()

        await asyncio.sleep(15.0)

        self.logger.info(f"[{self.id()}] Terminating. Delivered {len(self.delivered)} messages.")

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

        for peer in self.peers.values():
            if str(peer.peer_id) in self.neighbour_ids:
                await peer.dolev(msg)
                self.total_messages_sent += 1

    @handler
    async def dolev(self, src: PeerId, msg: DMsg):
        src_id = str(src)
        if src_id not in self.neighbour_ids:
            return

        msg_id = msg.msg_id

        if msg_id not in self.message_metrics:
            self.message_metrics[msg_id] = {
                "broadcast_time": time.time(),
                "delivery_time": None,
            }

        new_path = msg.path + [src_id]
        self.logger.info(f"[{self.id()}] recv {msg_id} from {src_id} path={msg.path}")

        self.paths.setdefault(msg_id, [])
        if new_path not in self.paths[msg_id]:
            self.paths[msg_id].append(new_path)

        for peer in self.peers.values():
            pid = str(peer.peer_id)
            if pid in self.neighbour_ids and pid not in new_path:
                await self.delay()
                await peer.dolev(DMsg(msg_id, msg.source, msg.payload, new_path))
                self.messages_forwarded += 1
                self.total_messages_sent += 1

        await self.my_deliver(msg)

    async def my_deliver(self, msg: DMsg):
        msg_id = msg.msg_id
        if msg_id in self.delivered:
            return

        paths = self.paths.get(msg_id, [])
        if self.distPaths(paths, msg.source):
            self.delivered.add(msg_id)
            self.message_metrics[msg_id]["delivery_time"] = time.time()
            self.logger.info(f"[{self.id()}] DELIVER {msg_id}")

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
