import logging
from distbench.algorithm import Algorithm
from distbench.community import PeerId
from distbench.decorators import config_field, distbench, handler, message

logger = logging.getLogger(__name__)

@message
class Ping:
    sequence: int

@message
class Pong:
    sequence: int


@distbench
class PingPong(Algorithm):
    # Configuration fields (loaded from YAML)
    initiator: bool = config_field(required=True)
    max_rounds: int = config_field(default=5)


    def __init__(self, config: dict, peers: dict):
        """
        Initialize the algorithm's internal state.
        This is called by the framework.
        """
        super().__init__()
        self.pings_received = 0
        self.pong_peer = None # To store who to play with


    async def on_start(self) -> None:
            """Called when all nodes are ready."""
            logger.info(f"PingPong starting (N={self.N()})")

            if self.initiator:
                # Get the first peer from the peers dict
                if self.peers:
                    peer_id, peer = next(iter(self.peers.items()))
                    self.pong_peer = peer
                    logger.info(f"I am initiator, sending Ping 0 to {peer_id}")
                    try:
                        # The handler name 'pong' on the peer becomes 'peer.pong()'
                        response = await peer.ping(Ping(sequence=0))
                        logger.info(f"Got response to first ping: {response}")
                    except Exception as e:
                        logger.error(f"Error sending ping: {e}")
                else:
                    logger.warning("Initiator has no peers, terminating.")
                    await self.terminate()


    async def on_exit(self) -> None:
        """Called during shutdown for cleanup."""
        logger.info("PingPong finished")


    async def report(self) -> dict[str, str]:
        """Optional: return metrics/results as a dictionary."""
        return {
            "pings_received": str(self.pings_received),
            "am_initiator": str(self.initiator)
        }


    @handler
    async def ping(self, src: PeerId, msg: Ping) -> str:
        """Handles an incoming Ping message."""
        logger.info(f"Received Ping {msg.sequence} from {src}")
        self.pings_received += 1

        # Send a Pong back to the sender
        peer = self.peers[src]
        await peer.pong(Pong(sequence=msg.sequence))

        # Return a value for request-response
        return f"ACK_Ping_{msg.sequence}"

    @handler
    async def pong(self, src: PeerId, msg: Pong) -> None:
        """Handles an incoming Pong message (fire-and-forget)."""
        logger.info(f"Received Pong {msg.sequence} from {src}")

        if msg.sequence >= self.max_rounds:
            logger.info("Max rounds reached, terminating.")
            await self.terminate()
        else:
            # Continue the game
            peer = self.peers[src]
            new_seq = msg.sequence + 1
            logger.info(f"Sending Ping {new_seq} to {src}")
            await peer.ping(Ping(sequence=new_seq))


