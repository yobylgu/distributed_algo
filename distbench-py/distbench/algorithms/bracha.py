import asyncio
import logging
import math
import time
import uuid

from distbench import Algorithm, PeerId
from distbench.decorators import child_algorithm, config_field, distbench, handler, message

from .dolev import DMsg, Dolev

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

        # Metrics tracking
        self.start_time: float = 0.0
        self.delivery_times: dict[str, float] = {}  # msg_id -> delivery timestamp
        self.messages_sent: int = 0
        self.messages_received: int = 0
        self.expected_senders: set[str] = set()  # Track unique senders for dynamic completion

    # dicts for messages otherwise it explodes
    def init_state(self, msg_id):
        if msg_id not in self.echos:
            self.echos[msg_id] = set()
            self.readys[msg_id] = set()
            self.sent_echo[msg_id] = False
            self.sent_ready[msg_id] = False
            self.delivered[msg_id] = False

    async def on_start(self):
        self.start_time = time.time()
        logger.info(f"[{self.id()}] Bracha starting (N={self.N}, f={self.f}, thresholds: echo={self.ready_threshold}, ready={self.deliver_threshold})")

        if self.is_sender:
            msg_id = str(uuid.uuid4())
            logger.info(f"[{self.id()}] SENDER broadcasting SEND: {msg_id}")
            self.init_state(msg_id)
            self.expected_senders.add(str(self.id()))  # Track self as sender

            msg = BrachaMessage("send", str(self.id()), msg_id, "hello - " + str(self.id()))

            await self.bracha("send", msg)
            self.seen_messages.add(msg_id)
            self.sent_echo[msg_id] = True
            await self.bracha("echo", BrachaMessage("echo", msg.sender, msg_id, msg.payload))

        # Fallback timeout - wait for algorithm to complete or timeout
        await asyncio.sleep(30.0)  # 30 second timeout
        if not self.is_terminated():
            logger.warning(f"[{self.id()}] Timeout reached, terminating")
            await self.terminate()

    async def bracha(self, method: str, msg: BrachaMessage):
        logger.info(f"[{self.id()}] BRACHA {method} {msg}")
        self.messages_sent += 1
        await self.dolev.yes_daddy_bracha(msg)

    @handler(from_child="dolev")
    async def dolev_deliver(self, src: PeerId, msg: DMsg):
        self.messages_received += 1
        b_msg = msg.payload
        if b_msg.phase == "send":
            # Track sender for dynamic completion detection
            self.expected_senders.add(str(b_msg.sender))
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
            self.delivery_times[msg_id] = time.time()
            logger.info(f"[{self.id()}] >>> BRACHA DELIVERED: {msg.payload} <<<")
            await self.check_completion()

    async def check_completion(self):
        """Check if all expected messages have been delivered."""
        # Count how many unique senders we've seen
        expected_count = len(self.expected_senders)
        delivered_count = sum(1 for d in self.delivered.values() if d)

        logger.debug(f"[{self.id()}] Completion check: delivered {delivered_count}/{expected_count}")

        # If we've delivered all messages from known senders, we're done
        if expected_count > 0 and delivered_count >= expected_count:
            logger.info(f"[{self.id()}] All {delivered_count} messages delivered, terminating")
            await self.terminate()

    async def report(self) -> dict[str, str]:
        """Return algorithm metrics."""
        delivered_count = sum(1 for d in self.delivered.values() if d)

        # Calculate latencies
        latencies = []
        for _msg_id, delivery_time in self.delivery_times.items():
            latency_ms = (delivery_time - self.start_time) * 1000
            latencies.append(latency_ms)

        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        min_latency = min(latencies) if latencies else 0.0
        max_latency = max(latencies) if latencies else 0.0

        return {
            "node_id": str(self.id()),
            "is_sender": str(self.is_sender),
            "N": str(self.N),
            "f": str(self.f),
            "delivered_count": str(delivered_count),
            "messages_sent": str(self.messages_sent),
            "messages_received": str(self.messages_received),
            "avg_latency_ms": f"{avg_latency:.2f}",
            "min_latency_ms": f"{min_latency:.2f}",
            "max_latency_ms": f"{max_latency:.2f}",
            "echo_threshold": str(self.ready_threshold),
            "ready_threshold": str(self.deliver_threshold),
        }
