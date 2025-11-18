import logging
from typing import Dict, Any

from distbench import Algorithm, PeerId
from distbench.decorators import message, handler, config_field, distbench

logger = logging.getLogger(__name__)


@message
class Ping:
    sequence: int


@message
class Pong:
    sequence: int

@distbench
class PingPong(Algorithm):
    initiator: bool = config_field(required=True)
    max_rounds: int = config_field(default=5)

    def __init__(self, config: Dict[str, Any], peers: Dict[PeerId, Any]):
        super().__init__()
        self.config = config
        self.peers = peers
        self.pings_received = 0
        self.pong_peer = None

    async def on_start(self) -> None:
        logger.info(f"PingPong starting (N={self.N()})")
        if self.initiator:
            if self.peers:
                peer_id, peer = next(iter(self.peers.items()))
                self.pong_peer = peer
                logger.info(f"I am initiator, sending Ping 0 to {peer_id}")
                try:
                    response = await peer.ping(Ping(sequence=0))
                    logger.info(f"Got response to first ping: {response}")
                except Exception as e:
                    logger.error(f"Error sending ping: {e}")
            else:
                logger.warning("Initiator has no peers, terminating.")
                await self.terminate()

    async def on_exit(self) -> None:
        logger.info("PingPong finished")

    async def report(self) -> Dict[str, str]:
        return {
            "pings_received": str(self.pings_received),
            "am_initiator": str(self.initiator),
        }

    @handler
    async def ping(self, src: PeerId, msg: Ping) -> str:
        logger.info(f"Received Ping {msg.sequence} from {src}")
        self.pings_received += 1

        peer = self.peers[src]
        await peer.pong(Pong(sequence=msg.sequence))

        return f"ACK_Ping_{msg.sequence}"

    @handler
    async def pong(self, src: PeerId, msg: Pong) -> None:
        logger.info(f"Received Pong {msg.sequence} from {src}")

        if msg.sequence >= self.max_rounds:
            logger.info("Max rounds reached, terminating.")
            await self.terminate()
            return

        peer = self.peers[src]
        new_seq = msg.sequence + 1
        logger.info(f"Sending Ping {new_seq} to {src}")
        await peer.ping(Ping(sequence=new_seq))
