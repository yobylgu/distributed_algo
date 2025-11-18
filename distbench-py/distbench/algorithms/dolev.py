import logging
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

    async def on_start(self) -> None:
        logger.info(f"[{self.id()}] starting with f={self.f}")
        if self.is_sender:
            await self.broadcast_message()

    async def on_exit(self) -> None:
        logger.info(f"[{self.id()}] finished")

    async def report(self) -> Dict[str, str]:
        return {"delivered_count": str(len(self.delivered))}

    async def broadcast_message(self) -> None:
        msg_id = "test_bby"
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


    #loop and paths tracking, calls deliver to check if can deliver

    @handler
    async def dolev(self, src: PeerId, msg: DMsg) -> None:
        msg_id = msg.msg_id
        src_id = str(src)

        new_path = msg.path + [src_id]

        logger.info(f"[{self.id()}] recv {msg_id} via {new_path}")

        if msg_id not in self.paths:
            self.paths[msg_id] = []

        if new_path not in self.paths[msg_id]:
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
