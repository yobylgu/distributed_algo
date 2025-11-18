import logging
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
class Dolev(Algorithm):
    f: int = config_field(required=True)
    is_sender: bool = config_field(default=False)
    payload: str = config_field(default="hello")

    def __init__(self, config: Dict[str, Any], peers: Dict[PeerId, Any]):
        super().__init__()
        self.peers = peers
        self.paths: Dict[str, List[List[str]]] = {}
        self.delivered: Set[str] = set()
        # MD.4: Track which neighbors have delivered each message
        self.neighbor_delivered: Dict[str, Set[str]] = {}
        # MD.2/MD.5: Track messages for which we've forwarded empty paths
        self.forwarded_empty: Set[str] = set()

    async def on_start(self) -> None:
        logger.info(f"[{self.id()}] starting with f={self.f}")
        if self.is_sender:
            await self.broadcast_message()

    async def on_exit(self) -> None:
        logger.info(f"[{self.id()}] finished")

    async def report(self) -> Dict[str, str]:
        return {"delivered_count": str(len(self.delivered))}

    async def broadcast_message(self) -> None:
        msg_id = str(uuid.uuid4())
        self_id = str(self.id())

        msg = DMsg(
            msg_id=msg_id,
            source=self_id,
            payload=self.payload,
            path=[],
        )

        logger.info(f"[{self.id()}] BROADCAST START FOR -----> {msg_id}")

        for peer in self.peers.values():
            await peer.dolev(msg)

        self.delivered.add(msg_id)

    # do not change above, it's msg builder and loop to send stuff (optims come below)
    # ============== OPTIMIZATIONS ==============
        # all 5 optims are abt forwarding and optims so change below

    # MD.1 & MD.2: Immediate delivery helper - delivers message and sends empty paths
    async def _immediate_deliver(self, msg: DMsg) -> None:
        """
        MD.1: Deliver message immediately when received directly from source.
        MD.2: After delivery, send empty path to all neighbors to signal delivery.
        """
        msg_id = msg.msg_id

        # Mark as delivered
        self.delivered.add(msg_id)
        logger.info(f"[{self.id()}] DELIVER (direct from source) {msg_id} payload={msg.payload}")

        # MD.2: Send empty path to ALL neighbors to signal delivery
        empty_msg = DMsg(
            msg_id=msg_id,
            source=msg.source,
            payload=msg.payload,
            path=[]
        )

        for peer in self.peers.values():
            await peer.dolev(empty_msg)

        # Mark that we've forwarded empty path
        self.forwarded_empty.add(msg_id)

        # Clear stored paths for this message
        if msg_id in self.paths:
            del self.paths[msg_id]

    #loop and paths tracking, calls deliver to check if can deliver

    @handler
    async def dolev(self, src: PeerId, msg: DMsg) -> None:
        msg_id = msg.msg_id
        src_id = str(src)

        # MD.5: Stop processing if already delivered AND forwarded empty
        if msg_id in self.delivered and msg_id in self.forwarded_empty:
            return

        # MD.1: Direct delivery from source
        if src_id == msg.source and msg_id not in self.delivered:
            await self._immediate_deliver(msg)
            return

        # MD.4: Handle empty path (delivery signal from neighbor)
        if not msg.path:
            # Track that src_id has delivered this message
            if msg_id not in self.neighbor_delivered:
                self.neighbor_delivered[msg_id] = set()
            self.neighbor_delivered[msg_id].add(src_id)
            logger.info(f"[{self.id()}] recv empty path from {src_id} for {msg_id} (neighbor delivered)")
            return

        new_path = msg.path + [src_id]

        logger.info(f"[{self.id()}] recv {msg_id} via {new_path}")

        if msg_id not in self.paths:
            self.paths[msg_id] = []

        # MD.4 extension: Skip paths containing nodes that have already delivered
        delivered_neighbors = self.neighbor_delivered.get(msg_id, set())
        path_contains_delivered = any(node in delivered_neighbors for node in new_path)

        if not path_contains_delivered and new_path not in self.paths[msg_id]:
            self.paths[msg_id].append(new_path)

        for peer in self.peers.values():
            peer_str = str(peer.peer_id)
            if peer_str not in new_path:
                await peer.dolev(
                    DMsg(
                        msg_id=msg_id,
                        source=msg.source,
                        payload=msg.payload,
                        path=new_path,
                    )
                )
        await self.deliver(msg)

    # check if can deliver
    # has help func to check disjoint paths
    async def deliver(self, msg: DMsg) -> None:
        msg_id = msg.msg_id

        if msg_id in self.delivered:
            return

        paths = self.paths.get(msg_id, [])

        if self.has_f_plus_one_disjoint(paths, msg.source):
            logger.info(f"[{self.id()}] DELIVER {msg_id} payload={msg.payload}")
            self.delivered.add(msg_id)

            # MD.2: Send empty path to all neighbors after delivery
            empty_msg = DMsg(
                msg_id=msg_id,
                source=msg.source,
                payload=msg.payload,
                path=[]
            )

            for peer in self.peers.values():
                await peer.dolev(empty_msg)

            # Mark that we've forwarded empty path
            self.forwarded_empty.add(msg_id)

            # Clear stored paths for this message
            if msg_id in self.paths:
                del self.paths[msg_id]

            await self.terminate()


    # helper to check f+1 disjoint paths
    # as brute force as it gets
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
