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
class BrachaOptimized(Algorithm):
    is_sender: bool = config_field(required=True)
    f: int = config_field(default=1)

    # Optimization flags (all enabled by default)
    enable_echo_amplification: bool = config_field(default=True)
    enable_single_hop_send: bool = config_field(default=True)
    enable_reduced_messages: bool = config_field(default=True)

    dolev_alg: Dolev = child_algorithm(Dolev)

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

        # Validate dependencies
        if self.enable_single_hop_send and not self.enable_echo_amplification:
            raise ValueError(
                "Single-hop Send (MBD.2) requires Echo Amplifications to be enabled. "
                "Set enable_echo_amplification=True or disable enable_single_hop_send."
            )

    # dicts for messages otherwise it explodes
    def init_state(self, msg_id):
        if msg_id not in self.echos:
            self.echos[msg_id] = set()
            self.readys[msg_id] = set()
            self.sent_echo[msg_id] = False
            self.sent_ready[msg_id] = False
            self.delivered[msg_id] = False

    def calculate_eligible_nodes(self, sender_id: str, count: int) -> set[str]:
        """Calculate the 'count' smallest node IDs after sender in circular order."""
        try:
            my_id = str(self.id())
        except RuntimeError:
            # Node ID not set yet - return empty set
            return set()

        all_ids = sorted([my_id] + [str(p.peer_id) for p in self.peers.values()])
        n = len(all_ids)

        try:
            sender_idx = all_ids.index(sender_id)
        except ValueError:
            logger.warning(f"[{my_id}] Sender {sender_id} not found in node list")
            return set()

        eligible = set()
        for i in range(1, count + 1):
            idx = (sender_idx + i) % n
            eligible.add(all_ids[idx])
        return eligible

    def should_generate_echo(self, sender_id: str) -> bool:
        """Check if this node should generate ECHO (MBD.11 optimization)."""
        try:
            my_id = str(self.id())
        except RuntimeError:
            # Node ID not set yet - allow generation (safe default)
            return True

        count = math.ceil((self.N + self.f + 1) / 2) + self.f
        eligible = self.calculate_eligible_nodes(sender_id, count)
        return my_id in eligible

    def should_generate_ready(self, sender_id: str) -> bool:
        """Check if this node should generate READY (MBD.11 optimization)."""
        try:
            my_id = str(self.id())
        except RuntimeError:
            # Node ID not set yet - allow generation (safe default)
            return True

        count = (2 * self.f + 1) + self.f
        eligible = self.calculate_eligible_nodes(sender_id, count)
        return my_id in eligible

    async def on_start(self):
        self.start_time = time.time()

        # Manually initialize Dolev child's neighbour_ids to avoid community access issues
        # This is needed for single-hop send where we call peer.send() before their Dolev is ready
        if hasattr(self.dolev_alg, 'neighbour_ids') and self.community:
            self.dolev_alg.neighbour_ids = {str(pid) for pid in self.community.neighbours}

        logger.info(
            f"[{self.id()}] Bracha Optimized starting "
            f"(N={self.N}, f={self.f}, "
            f"thresholds: echo={self.ready_threshold}, ready={self.deliver_threshold})"
        )
        logger.info(
            f"[{self.id()}] Optimizations - "
            f"Echo Amp: {self.enable_echo_amplification}, "
            f"Single-hop: {self.enable_single_hop_send}, "
            f"Reduced Msgs: {self.enable_reduced_messages}"
        )

        if self.is_sender:
            msg_id = str(uuid.uuid4())
            logger.info(f"[{self.id()}] SENDER broadcasting SEND: {msg_id}")
            self.init_state(msg_id)
            self.expected_senders.add(str(self.id()))  # Track self as sender

            msg = BrachaMessage("send", str(self.id()), msg_id, "hello - " + str(self.id()))

            if self.enable_single_hop_send:
                # MBD.2: Send SEND only to direct neighbors
                logger.info(f"[{self.id()}] Single-hop Send: Sending SEND to neighbors only")
                neighbour_ids = {str(pid) for pid in self.community.neighbours}

                # Delay to ensure all peer nodes have completed initialization
                # This ensures child Dolev algorithms have self.community set
                await asyncio.sleep(1.0)

                # Send directly to neighbor Bracha instances (bypass Dolev broadcast)
                for peer in self.peers.values():
                    if str(peer.peer_id) in neighbour_ids:
                        await peer.send(msg)
                        self.messages_sent += 1
            else:
                # Standard: Broadcast SEND via full Dolev
                await self.bracha("send", msg)

            # Sender always sends own ECHO (if eligible in reduced messages mode)
            self.seen_messages.add(msg_id)
            if self.should_generate_echo(str(self.id())):
                self.sent_echo[msg_id] = True
                self.echos[msg_id].add(str(self.id()))  # Count own ECHO
                await self.bracha("echo", BrachaMessage("echo", msg.sender, msg_id, msg.payload))

        # Fallback timeout - wait for algorithm to complete or timeout
        await asyncio.sleep(30.0)  # 30 second timeout
        if not self.is_terminated():
            logger.warning(f"[{self.id()}] Timeout reached, terminating")
            await self.terminate()

    async def bracha(self, method: str, msg: BrachaMessage):
        logger.info(f"[{self.id()}] BRACHA {method} {msg}")
        self.messages_sent += 1
        await self.dolev_alg.yes_daddy_bracha(msg)

    @handler
    async def dolev(self, src: PeerId, msg: DMsg):
        """
        Route incoming Dolev network messages to the Dolev child algorithm.

        CRITICAL: When Dolev broadcasts DMsg packets over the network (for ECHO/READY),
        they arrive at peer BrachaOptimized instances. Without this handler, they get
        dropped with "Unhandled message type: dolev" warnings.

        This forwards the DMsg to the child Dolev for processing.
        """
        await self.dolev_alg.dolev(src, msg)

    @handler(from_child="dolev_alg")
    async def dolev_deliver(self, src: PeerId, msg: DMsg):
        self.messages_received += 1
        # msg.payload is a dict when deserialized - convert to BrachaMessage
        payload = msg.payload
        if isinstance(payload, dict):
            b_msg = BrachaMessage(**payload)
        else:
            b_msg = payload

        if b_msg.phase == "send":
            # Track sender for dynamic completion detection
            self.expected_senders.add(str(b_msg.sender))
            await self.send(src, b_msg)
        elif b_msg.phase == "echo":
            await self.echo(src, b_msg)
        elif b_msg.phase == "ready":
            await self.ready(src, b_msg)

    @handler
    async def send(self, src: PeerId, msg: BrachaMessage):
        msg_id = msg.msg_id
        logger.info(f"[{self.id()}] RECV SEND from {src} msg_id={msg_id}")
        self.init_state(msg_id)

        if msg_id not in self.seen_messages:
            self.seen_messages.add(msg_id)

            # Check if eligible to generate ECHO (MBD.11 optimization)
            if not self.enable_reduced_messages or self.should_generate_echo(msg.sender):
                self.sent_echo[msg_id] = True
                self.echos[msg_id].add(str(self.id()))  # Count own ECHO
                await self.bracha("echo", BrachaMessage("echo", msg.sender, msg_id, msg.payload))
            else:
                logger.info(f"[{self.id()}] Reduced Messages: Not eligible to generate ECHO")

    @handler
    async def echo(self, src: PeerId, msg: BrachaMessage):
        msg_id = msg.msg_id
        logger.info(f"[{self.id()}] RECV ECHO from {src} msg_id={msg_id}")
        self.init_state(msg_id)
        self.echos[msg_id].add(str(src))

        # Echo Amplification: Send ECHO early if we have f+1 ECHOs
        # FIX: Must check committee eligibility even for amplification
        if self.enable_echo_amplification:
            if not self.sent_echo[msg_id] and len(self.echos[msg_id]) >= self.f + 1:
                # Check if eligible to generate ECHO (MBD.11 compatibility)
                if self.should_generate_echo(msg.sender):
                    logger.info(f"[{self.id()}] Echo Amplification: Sending ECHO at f+1={self.f+1} threshold")
                    self.sent_echo[msg_id] = True
                    self.echos[msg_id].add(str(self.id()))  # Count own ECHO
                    await self.bracha("echo", BrachaMessage("echo", msg.sender, msg_id, msg.payload))
                else:
                    logger.debug(f"[{self.id()}] Echo Amplification: Not eligible (MBD.11)")

        # Standard READY trigger: ⌈(N+f+1)/2⌉ ECHOs
        if not self.sent_ready[msg_id] and len(self.echos[msg_id]) >= self.ready_threshold:
            # Check if eligible to generate READY (MBD.11 optimization)
            if self.should_generate_ready(msg.sender):
                self.sent_ready[msg_id] = True
                await self.bracha("ready", BrachaMessage("ready", msg.sender, msg_id, msg.payload))
            else:
                logger.info(f"[{self.id()}] Reduced Messages: Not eligible to generate READY")

    @handler
    async def ready(self, src: PeerId, msg: BrachaMessage):
        msg_id = msg.msg_id
        logger.info(f"[{self.id()}] RECV READY from {src} msg_id={msg_id}")
        self.init_state(msg_id)
        self.readys[msg_id].add(str(src))

        # Determine if we will send READY now (due to f+1 READYs)
        # This is needed for "Silent Echo" optimization (PDF source 44)
        will_send_ready_now = False
        if not self.sent_ready[msg_id] and len(self.readys[msg_id]) >= self.f + 1:
            if self.should_generate_ready(msg.sender):
                will_send_ready_now = True

        # Echo Amplification: Generate ECHO when receiving READY
        # FIX: "If process generates both ECHO and READY, only send READY" (PDF source 44)
        if self.enable_echo_amplification and not self.sent_echo[msg_id]:
            # Mark that we implicitly have the ECHO
            self.sent_echo[msg_id] = True
            self.echos[msg_id].add(str(self.id()))

            # Only send physical ECHO message if we are NOT about to send READY
            if not will_send_ready_now:
                # Check if eligible to generate ECHO (MBD.11 compatibility)
                if self.should_generate_echo(msg.sender):
                    logger.info(f"[{self.id()}] Echo Amplification: Generating ECHO from READY")
                    await self.bracha("echo", BrachaMessage("echo", msg.sender, msg_id, msg.payload))
                else:
                    logger.debug(f"[{self.id()}] Echo Amplification: Not eligible (MBD.11)")
            else:
                logger.debug(f"[{self.id()}] Silent Echo: Suppressing ECHO because READY is being sent")

        # Early READY trigger: f+1 READYs (amplification)
        if will_send_ready_now:
            self.sent_ready[msg_id] = True
            await self.bracha("ready", msg)

        # Delivery threshold: 2f+1 READYs
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
            # Add optimization status to metrics
            "echo_amplification": str(self.enable_echo_amplification),
            "single_hop_send": str(self.enable_single_hop_send),
            "reduced_messages": str(self.enable_reduced_messages),
        }
