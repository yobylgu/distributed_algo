import asyncio
import logging
import time
from typing import Any, Dict, List, Tuple

from distbench import Algorithm, PeerId
from distbench.decorators import child_algorithm, config_field, distbench, handler, message

from .bracha_optimized import BrachaOptimized, BrachaMessage
from .dolev import DMsg

@message
class RCBMessage:
    origin: str
    vc: Dict[str, int]
    msg: Any

@distbench
class RCB(Algorithm):
    is_sender: bool = config_field(required=True)
    num_messages: int = config_field(default=1)
    delay_min: float = config_field(default=0.0)
    delay_max: float = config_field(default=0.2)
    f: int = config_field(default=1)
    behavior_mode: str = config_field(default="HONEST")
    split_logs: bool = config_field(default=True)

    bracha: BrachaOptimized = child_algorithm(BrachaOptimized)

    def __init__(self, config: dict, peers: dict):
        super().__init__()
        self.peers = peers

        self.VC: Dict[str, int] = {}
        self.pending: List[Tuple[str, Dict[str, int], Any]] = []

        self.start_time: float = 0.0
        self.rcb_broadcast_count: int = 0
        self.rcb_deliver_count: int = 0

        self.logger = logging.getLogger("PLACEHOLDER")


    async def on_start(self):
        self.logger = logging.getLogger(f"causal-{self.id()}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if not self.logger.handlers:
            fh = logging.FileHandler(f"{self.id()}.txt", mode="w")
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s : %(message)s")
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

        self.start_time = time.time()
        self.logger.info(f"[{self.id()}] RCB starting")
        self.bracha.is_sender = False
        self.bracha.num_messages = 0
        self.bracha.f = self.f
        self.bracha.behavior_mode = self.behavior_mode

        curr = str(self.id())
        ids = sorted({curr} | {str(p.peer_id) for p in self.peers.values()})
        self.VC = {pid: 0 for pid in ids}
        self.pending = []
        self.logger.info(f"[{curr}] init VC={self.VC}")

        if self.is_sender:
            await asyncio.sleep(1.0) # wait startup nodes
            for i in range(self.num_messages):
                await self.rcb(i, f"{i} --- {curr}")


    async def rcb(self, i:int, m: Any):
        curr = str(self.id())
        self.rcb_broadcast_count += 1

        # local delivers asap
        self._rcb_deliver(curr, m)
        vc = dict(self.VC)

        ## THIS IS FOR REVERSE ORDER (TO SHOW JUST ~HOW~ CAUSAL WE ARE)
        ##vc[lsn] = 14 - i
        ##

        msg = RCBMessage(origin=curr, vc=vc, msg=m)
        payload = {"origin": msg.origin, "vc": msg.vc, "msg": msg.msg}

        msg_id = f"{curr}:{time.time_ns()}"
        bmsg = BrachaMessage("send", curr, msg_id, payload)

        await self.bracha.bracha("send", bmsg)
        self.VC[curr] += 1
        self.logger.info(f"[{curr}] rcbBroadcast '{m}' VC={self.VC}")

    @handler(from_child="bracha")
    async def bracha_deliver(self, src: PeerId, payload: Any):
        got_msg = RCBMessage(**payload)
        origin = got_msg.origin
        vc = got_msg.vc
        msg = got_msg.msg

        curr = str(self.id())
        if origin == curr: # alr deliv itself
            return

        self.pending.append((origin, vc, msg))
        self.logger.info(f"[{curr}] deliver from bracha: origin={origin} msg={msg} VCx={vc}")
        self._deliver_pending()

    def _deliver_pending(self):
        while True:
            deliv_msg = None
            for i, (sender, vc, msg) in enumerate(self.pending):
                for pj, needed in vc.items():  #find smallest

                    if self.VC.get(pj, 0) < int(needed):
                        break
                else:
                    # newest is smallest
                    deliv_msg = i
                    break

            if deliv_msg is None:
                break

            sender, vc, msg = self.pending.pop(deliv_msg) # deliver one go again
            self._rcb_deliver(sender, msg)
            self.VC[sender] = self.VC.get(sender, 0) + 1

    def _rcb_deliver(self, sender: str, m: Any):
        self.rcb_deliver_count += 1
        self.logger.info(f"[{self.id()}] >>> RCB DELIVER from {sender}: {m} <<<")


    # child handlers
    @handler
    async def dolev(self, src: PeerId, msg: DMsg):
        await self.bracha.dolev(src, msg)

    @handler
    async def send(self, src: PeerId, msg: BrachaMessage):
        await self.bracha.send(src, msg)

    @handler
    async def echo(self, src: PeerId, msg: BrachaMessage):
        await self.bracha.echo(src, msg)

    @handler
    async def ready(self, src: PeerId, msg: BrachaMessage):
        await self.bracha.ready(src, msg)