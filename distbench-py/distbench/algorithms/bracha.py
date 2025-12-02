import logging
import uuid
import math
from distbench import Algorithm, PeerId
from distbench.decorators import message, handler, config_field, distbench, child_algorithm
from .dolev import DMsg, Dolev
import asyncio
logger = logging.getLogger(__name__)


@message
class BrachaMessage:
    phase: str
    sender: PeerId
    msg_id: str
    payload: str


@distbench
class Bracha(Algorithm):
    is_sender: bool = config_field(required=True)
    f: int = config_field(default=1)

    dolev: Dolev = child_algorithm(Dolev)

    def __init__(self, config: dict, peers: dict):
        super().__init__()
        self.peers = peers
        self.N = len(peers) + 1
        self.seen_messages = set()
        self.echos = {}
        self.readys = {}
        self.sent_echo = {}
        self.sent_ready = {}
        self.delivered = {}

        self.ready_threshold = math.ceil((self.N + self.f + 1) / 2)
        self.deliver_threshold = 2 * self.f + 1

    # dicts for messages otherwise it explodes
    def init_state(self, msg_id):
        if msg_id not in self.echos:
            self.echos[msg_id] = set()
            self.readys[msg_id] = set()
            self.sent_echo[msg_id] = False
            self.sent_ready[msg_id] = False
            self.delivered[msg_id] = False

    async def on_start(self):
        if self.is_sender:
            msg_id = str(uuid.uuid4())
            logger.info(f"[{self.id()}] SENDER broadcasting SEND: {msg_id}")
            self.init_state(msg_id)

            msg = BrachaMessage("send", str(self.id()), msg_id, "hello - " + str(self.id()))

            await self.bracha("send", msg)
            self.seen_messages.add(msg_id)
            self.sent_echo[msg_id] = True
            await self.bracha("echo", BrachaMessage("echo", msg.sender, msg_id, msg.payload))

    async def bracha(self, method: str, msg: BrachaMessage):
        await asyncio.sleep(1)
        logger.info(f"[{self.id()}] BRACHA {method} {msg}")
        await self.dolev.yes_daddy_bracha(msg)

    @handler(from_child="dolev")
    async def dolev_deliver(self, src: PeerId, msg: DMsg):
        b_msg = msg.payload
        if b_msg.phase == "send":
            await self.send(src, b_msg)
        elif b_msg.phase == "echo":
            await self.echo(src, b_msg)
        elif b_msg.phase == "ready":
            await self.ready(src, b_msg)

    #basically the same, but different so i dont if statement my way through it
    @handler
    async def send(self, src: PeerId, msg: BrachaMessage):
        msg_id = msg.msg_id
        logger.info(f"[{self.id()}] RECV SEND from {src} msg_id={msg_id}")
        self.init_state(msg_id)

        if msg_id not in self.seen_messages:
            self.seen_messages.add(msg_id)
            self.sent_echo[msg_id] = True
            await self.bracha("echo", BrachaMessage("echo", msg.sender, msg_id, msg.payload))

    @handler
    async def echo(self, src: PeerId, msg: BrachaMessage):
        msg_id = msg.msg_id
        logger.info(f"[{self.id()}] RECV ECHO from {src} msg_id={msg_id}")
        self.init_state(msg_id)
        self.echos[msg_id].add(str(src))

        if not self.sent_ready[msg_id] and len(self.echos[msg_id]) >= self.ready_threshold:
            self.sent_ready[msg_id] = True
            await self.bracha("ready", BrachaMessage("ready", msg.sender, msg_id, msg.payload))

    @handler
    async def ready(self, src: PeerId, msg: BrachaMessage):
        msg_id = msg.msg_id
        logger.info(f"[{self.id()}] RECV READY from {src} msg_id={msg_id}")
        self.init_state(msg_id)
        self.readys[msg_id].add(str(src))

        if not self.sent_ready[msg_id] and len(self.readys[msg_id]) >= self.f + 1:

            self.sent_ready[msg_id] = True
            await self.bracha("ready", msg)

        if not self.delivered[msg_id] and len(self.readys[msg_id]) >= self.deliver_threshold:
            self.delivered[msg_id] = True
            logger.info(f"[{self.id()}] >>> BRACHA DELIVERED: {msg.payload} <<<")
