GLOBAL_F = 1
import logging
import uuid
from typing import Dict, Any, List, Set
import random
import asyncio
from distbench import Algorithm, PeerId
from distbench.decorators import message, handler, config_field, distbench

logger = logging.getLogger(__name__)


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

    def __init__(self, config: Dict[str, Any], peers: Dict[PeerId, Any]):
        super().__init__()
        self.peers = peers
        self.neighbour_ids: Set[str] = set()
        self.f = GLOBAL_F
        self.paths: Dict[str, List[List[str]]] = {}
        self.delivered: Set[str] = set()
        self.forwarded_empty: Set[str] = set()
        self.neighbour_delivered: Dict[str, Set[str]] = {}

    async def on_start(self):
        if not self.neighbour_ids:
            self.neighbour_ids = {str(pid) for pid in self.community.neighbours}
            print("✔ DOLEV LOADED WITH NEIGHBOURS:", self.neighbour_ids)

        logger.info(f"[{self.id()}] start neighbours={self.neighbour_ids}")

        if self.is_sender:
            await self.broadcast_message()
        await asyncio.sleep(4.0)

        logger.info(f"[{self.id()}] Terminating. Delivered {len(self.delivered)} messages.")
        await self.terminate()

    async def on_exit(self) -> None:
        logger.info(f"[{self.id()}] finished")

    async def report(self) -> Dict[str, str]:
        return {"delivered_count": str(len(self.delivered))}

    async def broadcast_message(self):
        msg_id = str(uuid.uuid4())
        self_id = str(self.id())

        msg = DMsg(
            msg_id=msg_id,
            source=self_id,
            payload=self.payload,
            path=[]
        )

        logger.info(f"[{self.id()}] BROADCAST START -----> {msg_id}")

        for peer in self.peers.values():
            if str(peer.peer_id) in self.neighbour_ids:
                await peer.dolev(msg)

        self.delivered.add(msg_id)

    async def delay(self):
        await asyncio.sleep(random.uniform(0, self.max_delay))

    async def _immediate_deliver(self, msg: DMsg):
        if msg.msg_id not in self.delivered:
            self.delivered.add(msg.msg_id)
            logger.info(f"[{self.id()}] DELIVER (direct) {msg.msg_id}")

    @handler
    async def dolev(self, src: PeerId, msg: DMsg):
        src_id = str(src)
        msg_id = msg.msg_id

        if src_id not in self.neighbour_ids:
            return

        if msg_id in self.delivered and msg_id in self.forwarded_empty:
            return

        if src_id == msg.source and msg_id not in self.delivered:
            await self._immediate_deliver(msg)

        if not msg.path and src_id != msg.source:
            self.neighbour_delivered.setdefault(msg_id, set()).add(src_id)
            logger.info(f"[{self.id()}] recv empty from {src_id}")
            return

        new_path = msg.path + [src_id]
        logger.info(f"[{self.id()}] recv {msg_id} via {new_path}")

        self.paths.setdefault(msg_id, [])
        delivered_neighbours = self.neighbour_delivered.get(msg_id, set())

        if any(n in delivered_neighbours for n in new_path):
            return

        if new_path not in self.paths[msg_id]:
            self.paths[msg_id].append(new_path)

        for peer in self.peers.values():
            pid = str(peer.peer_id)
            if pid not in self.neighbour_ids:
                continue
            if pid in delivered_neighbours:
                continue
            if pid not in new_path:
                await self.delay()
                await peer.dolev(DMsg(msg_id, msg.source, msg.payload, new_path))

        await self.deliver(msg)

    async def deliver(self, msg: DMsg):
        msg_id = msg.msg_id
        paths = self.paths.get(msg_id, [])

        if msg_id not in self.delivered:
            if not self.disPaths(paths, msg.source):
                return
            logger.info(f"[{self.id()}] DELIVER {msg_id}")
            self.delivered.add(msg_id)

        if msg_id in self.forwarded_empty:
            return

        empty = DMsg(msg_id, msg.source, msg.payload, [])

        for peer in self.peers.values():
            if str(peer.peer_id) in self.neighbour_ids:
                await self.delay()
                await peer.dolev(empty)

        self.forwarded_empty.add(msg_id)
        self.paths.pop(msg_id, None)

    def disPaths(self, paths: List[List[str]], source: str) -> bool:
        needed = self.f + 1
        chosen: List[Set[str]] = []
        for p in paths:
            internal = set(p)
            internal.discard(source)
            internal.discard(str(self.id()))
            if any(internal & used for used in chosen):
                continue
            chosen.append(internal)
            if len(chosen) >= needed:
                return True
        return False