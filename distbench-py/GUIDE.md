# Distbench Implementation Guide (Python)

This guide teaches you how to implement distributed algorithms using the Python port of the `distbench` framework.

## Table of Contents

- [Overview](#overview)
- [Step-by-Step Tutorial: PingPong](#step-by-step-tutorial-pingpong)
- [Algorithm API Reference](#algorithm-api-reference)
- [Signed Messages & Verification](#signed-messages--verification)
- [Algorithm Layering](#algorithm-layering)
- [Configuration](#configuration)
- [Example Algorithms](#example-algorithms)
- [Execution Modes & Testing](#execution-modes--testing)
- [Docker Deployment](#docker-deployment)
- [Tips and Best Practices](#tips-and-best-practices)

## Overview

A `distbench` algorithm in Python consists of a single class that inherits from `Algorithm`. This class is decorated with `@distbench` and defines:

1.  **Message Definitions** - Dataclasses marked with `@message`.
2.  **Algorithm State** - Class attributes defined with `config_field()`.
3.  **Lifecycle Hooks** - `async` methods like `on_start` and `report`.
4.  **Message Handlers** - `async` methods marked with `@handler`.

The framework handles all networking, serialization, and lifecycle coordination, letting you focus purely on the algorithm's logic.

## Step-by-Step Tutorial: PingPong

Let's build a simple PingPong algorithm.

### Step 1: Create the File

Create a new file in `distbench/algorithms/pingpong.py`. The framework will automatically discover it.

### Step 2: Define Messages

Use the `@message` decorator on a class to define a message. We'll use dataclasses for simplicity.

```python
# distbench/algorithms/pingpong.py
import logging
from distbench import (
    Algorithm, PeerId,
    message, distbench, handler, config_field
)

logger = logging.getLogger(__name__)

@message
class Ping:
    sequence: int

@message
class Pong:
    sequence: int
```

### Step 3: Define Algorithm State

Use the `@distbench` decorator on your `Algorithm` subclass. Define configuration fields using `config_field()`.

```python
@distbench
class PingPong(Algorithm):
    # Configuration fields (loaded from YAML)
    initiator: bool = config_field(required=True)
    max_rounds: int = config_field(default=5)
    items: list = config_field(default_factory=list)

    def __init__(self, config: dict, peers: dict):
        """
        Initialize the algorithm's internal state.
        This is called by the framework.
        """
        super().__init__()
        self.pings_received = 0
        self.pong_peer = None # To store who to play with
```

**Note:** Unlike the Rust version, you don't need `Mutex` or `Atomic` types. Each node runs in a single-threaded `asyncio` task, so simple instance variables (`self.pings_received`) are safe.

### Step 4: Implement Algorithm Lifecycle

Implement the `async` methods of the `Algorithm` base class.

```python
# Inside the PingPong class...

async def on_start(self) -> None:
    """Called when all nodes are ready."""
    logger.info(f"PingPong starting (N={self.N()})")

    if self.is_sender:
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
        "am_initiator": str(self.is_sender)
    }
```

### Step 5: Implement Message Handlers

Add `async` methods marked with `@handler` to your class. The framework automatically generates the `self.peers` proxies from these.

```python
# Inside the PingPong class...

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

        # No return value means this is a "cast" (fire-and-forget)
```

**Handler Patterns:**

- `async def handler(...) -> ResponseType:`: Defines a **request-response** message. The caller will wait for the response.
- `async def handler(...) -> None:`: Defines a **fire-and-forget (cast)** message. The caller will not wait.

### Step 6: Create Configuration File

Create `configs/pingpong.yaml`:

```yaml
n1:
  neighbours: [n2]
  initiator: true
  max_rounds: 10

n2:
  neighbours: [n1]
  initiator: false
```

### Step 7: Run Your Algorithm

```bash
uv run distbench -c configs/pingpong.yaml -a pingpong --mode local -v
```

## Algorithm API Reference

The following methods and attributes are available on `self` inside your `Algorithm` subclass.

### `self.id() -> PeerId`

Returns the unique `PeerId` of the current node.

```python
logger.info(f"Node {self.id()} starting")
```

### `self.N() -> int`

Returns the total number of nodes in the system (including this one).

```python
n = self.N()
majority = (n // 2) + 1
```

### `self.peers -> dict[PeerId, Peer]`

A dictionary mapping all other `PeerId`s to their `Peer` proxy objects. You use this to send messages.

```python
# Broadcast to all peers
for peer_id, peer in self.peers.items():
    await peer.some_message(MyMessage())

# Send to a specific peer
peer_n1 = self.peers[PeerId("n1")]
await peer_n1.hello("Hi n1!")
```

### `await self.terminate()`

Signals to the framework that this node has completed its execution. The node will shut down gracefully once all other nodes also terminate (or on timeout).

```python
@handler
async def done(self, src: PeerId, msg: DoneMessage):
    logger.info("Algorithm complete, terminating")
    await self.terminate()
```

### `self.sign(message: T) -> Signed[T]`

Creates a cryptographically signed wrapper for a message.

```python
from distbench.signing import Signed

@message
class Vote:
    value: bool

@handler
async def send_vote(self, src: PeerId, msg: RequestVote):
    # Create a signed vote
    signed_vote = self.sign(Vote(value=True))

    # Send the Signed[Vote] object
    peer = self.peers[src]
    await peer.receive_vote(signed_vote)
```

## Public Interface (@interface)

You can expose public methods that take raw bytes (or other types) and automatically takes care of serialization. This is useful for defining the public API of your algorithm, especially when it's used as a child layer.

Use the `@interface` decorator:

```python
from distbench.decorators import interface

@distbench
class MyAlgo(Algorithm):

    @interface
    async def broadcast(self, msg: bytes) -> None:
        # msg here is the SERIALIZED AlgorithmMessage(type_id, payload)
        # You typically wrap it in a transport message
        transport_msg = TransportMsg(payload=msg)
        for peer in self.peers.values():
            await peer.handle_msg(transport_msg)
```

Calls to `my_algo.broadcast(some_obj)` will:

1. Serialize `some_obj`
2. Wrap it in an `AlgorithmMessage`
3. Serialize the `AlgorithmMessage`
4. Pass the resulting bytes to `broadcast`

## Signed Messages & Verification

The framework provides a `Signed[T]` wrapper for Byzantine fault-tolerant algorithms.

```python
from distbench.signing import Signed

@message
class Vote:
    value: bool

# The handler type annotation is Signed[Vote]
@handler
async def receive_vote(self, src: PeerId, msg: Signed[Vote]) -> None:
    # You can access inner fields directly
    logger.info(f"Received vote: {msg.value} from {msg.signer}")

    # The __str__ method is also customized
    logger.info(f"Full message: {msg}")
    # Prints: "Vote(value=True) (signed by n1)"
```

### 🔐 Automatic Verification

**This is a key difference from the Rust version.**

The Python framework **automatically verifies all incoming `Signed[T]` messages** _before_ your handler is called.

The framework checks two things:

1.  The signature is cryptographically valid for the message content and the signer's public key.
2.  The `signer` field inside the message (`msg.signer`) matches the `PeerId` of the node that sent the message (`src`).

If _either_ check fails, the framework **automatically drops the message** and logs a warning. You do not need to call `self.verify()` in your handler.

## Algorithm Layering

The framework supports **algorithm layering**, which allows you to compose complex distributed protocols from simpler building blocks. This is particularly useful for:

- **Separation of concerns** - Split protocol logic into independent layers
- **Reusability** - Use the same lower-layer protocol with different upper layers
- **Modularity** - Test and develop each layer independently

### How Layering Works

A **parent algorithm** can have one or more **child algorithms**. Child algorithms:

- Run as part of the parent algorithm's lifecycle
- Can deliver messages up to the parent
- Have their own message handlers and state

The parent algorithm can:

- Intercept messages from child algorithms
- Call methods on child algorithms
- Configure child algorithms independently

### Defining a Layered Algorithm

#### Step 1: Mark Child Algorithms

Use the `child_algorithm()` function to declare child algorithms in your parent class:

```python
from distbench import Algorithm, child_algorithm, distbench, config_field

@distbench
class UpperLayer(Algorithm):
    # Configuration
    start_node: bool = config_field(default=False)

    # State
    messages_from_lower: int = 0

    # Child algorithm
    broadcast: SimpleBroadcast = child_algorithm(SimpleBroadcast)
```

**Important**: The type annotation must match the child algorithm class.

#### Step 2: Child Delivers Messages to Parent

In the child algorithm, use `self.deliver_message()` to send messages to the parent:

```python
@distbench
class LowerLayer(Algorithm):
    @handler
    async def broadcast_message(self, src: PeerId, msg: BroadcastMessage) -> str:
        logger.info(f"LowerLayer: Received message from {src}")

        # Payload should be bytes
        if self._parent:
            await self.deliver_message(src, msg.payload)

        return "Acknowledged"
```

**Note**: The child uses `self._parent` to check if it has a parent, and `self.deliver_message(src, msg)` to send messages up. The framework automatically handles deserialization in the parent.

#### Step 3: Parent Intercepts Child Messages

Use `@handler(from_child="child_name")` to intercept messages from a child:

```python
@distbench
class UpperLayer(Algorithm):
    # Child algorithm
    broadcast: SimpleBroadcast = child_algorithm(SimpleBroadcast)

    @handler(from_child="broadcast")
    async def on_broadcast(self, src: PeerId, msg: BroadcastMessage) -> None:
        logger.info(
            f"UpperLayer: Intercepted message from lower layer! "
            f"Source: {src}, Content: {msg.content}"
        )
        self.messages_from_lower += 1
```

**Important**: The `from_child` parameter must match the attribute name of the child algorithm (in this case, `"broadcast"`).

#### Step 4: Parent Calls Child Methods

The parent can call public methods on the child algorithm:

```python
@distbench
class UpperLayer(Algorithm):
    broadcast: SimpleBroadcast = child_algorithm(SimpleBroadcast)
    start_node: bool = config_field(default=False)

    async def on_start(self) -> None:
        if self.start_node:
            logger.info("UpperLayer: Initiating broadcast through lower layer")
            await self.broadcast.broadcast("Hello from upper layer!")
```

### Configuring Layered Algorithms

Child algorithms are configured using nested structures in the YAML configuration file:

```yaml
node1:
  neighbours: [node2, node3]
  start_node: true
  # Child algorithm configuration
  broadcast:
    is_sender: false

node2:
  neighbours: [node1, node3]
  start_node: false
  broadcast:
    is_sender: false
```

#### Using YAML Anchors for Reusability

You can use YAML anchors to avoid repeating configuration:

```yaml
# Shared template (starts with _ to indicate it's not a node)
_template: &config_template
  broadcast:
    is_sender: false

node1:
  <<: *config_template # Merge template
  neighbours: [node2, node3]
  start_node: true

node2:
  <<: *config_template
  neighbours: [node1, node3]
  start_node: false
```

**Note**: Keys starting with `_` are ignored and can be used for templates.

### Complete Example

See **[simple_broadcast.py](distbench/algorithms/simple_broadcast.py)** for a complete working example that demonstrates:

- A `SimpleBroadcast` lower layer that broadcasts messages
- A `SimpleBroadcastUpper` parent layer that intercepts and tracks messages
- Communication between layers using `deliver_message()`
- Nested configuration with YAML anchors

The example shows:

```python
@message
class BroadcastMessage:
    content: bytes


@message
class UpperBroadcastMessage:
    content: bytes

@distbench
class SimpleBroadcast(Algorithm):
    """Lower layer that handles broadcasting."""
    is_sender: bool = config_field(default=False)

    @handler
    async def broadcast_message(self, src: PeerId, msg: BroadcastMessage) -> str:
        # Deliver to parent if it exists
        if self._parent:
            await self.deliver_message(src, msg.bytes)
        return "Ack"

    @interface
    async def broadcast(self, content: bytes) -> None:
        """Public method for broadcasting."""
        msg = BroadcastMessage(content=content)
        for peer in self.peers.values():
            await peer.broadcast_message(msg)

@distbench
class SimpleBroadcastUpper(Algorithm):
    """Upper layer that uses SimpleBroadcast."""
    start_node: bool = config_field(default=False)
    broadcast: SimpleBroadcast = child_algorithm(SimpleBroadcast)
    messages_received: int = 0

    async def on_start(self) -> None:
        if self.start_node:
            await self.broadcast.broadcast(UpperBroadcastMessage(
                content=b"Hello from upper layer!"
            ))

    @handler(from_child="broadcast")
    async def on_broadcast(self, src: PeerId, msg: BroadcastMessage) -> None:
        logger.info(f"Intercepted: {msg.content} from {src}")
        self.messages_received += 1
        await self.terminate()
```

### Layering Best Practices

1. **Clear Responsibilities** - Each layer should have a well-defined purpose
2. **Minimal Coupling** - Layers should communicate through well-defined message types
3. **Independent Testing** - Each layer should be testable on its own
4. **Documentation** - Document the interface between layers clearly
5. **Error Handling** - Handle delivery errors when using `deliver_message()`

## Configuration

### File Format

YAML configuration maps node IDs to node definitions:

```yaml
node_id:
  neighbours: [list of neighbor node IDs]
  host: "network address" # For network/local mode
  port: port_number # For network/local mode
  algorithm_field: value # Passed to @distbench config
```

### Fully Connected Topology

Use an empty `neighbours: []` list to connect a node to all other nodes in the file.

```yaml
n1:
  neighbours: [] # Connects to n2, n3
  is_sender: true
n2:
  neighbours: [] # Connects to n1, n3
n3:
  neighbours: [] # Connects to n1, n2
```

## Example Algorithms

Study these examples to learn different patterns:

- **[echo.py](distbench/algorithms/echo.py)**: Request-response, `Signed[T]`, `report()`.
- **[chang_roberts.py](distbench/algorithms/chang_roberts.py)**: Ring logic, ID comparison, message forwarding, termination.
- **[message_chain.py](distbench/algorithms/message_chain.py)**: Forwarding chains, `Signed[T]` in collections (`list[Signed[...]]`), deduplication.
- **[simple_broadcast.py](distbench/algorithms/simple_broadcast.py)**: Layered architecture with parent-child communication, demonstrates `child_algorithm()` and `@handler(from_child=...)`.

## Execution Modes & Testing

### 1. Offline Mode (`--mode offline`)

Runs all nodes in a **single process** using in-memory queues.

```bash
uv run distbench -c configs/echo.yaml -a echo --mode offline
```

- **Pros**: Extremely fast, deterministic, easy to debug with one log stream.
- **Cons**: Not realistic; no real network latency or failures.
- **Use For**: Initial development and logic testing.

### 2. Local Mode (`--mode local`)

Spawns all nodes as separate **OS processes** on `localhost`, communicating over TCP.

```bash
uv run distbench -c configs/echo.yaml -a echo --mode local --port-base 10000
```

- **Pros**: Realistic simulation of network behavior (serialization, TCP) without a cluster.
- **Cons**: Slower, subject to OS scheduler.
- **Use For**: Concurrency testing, integration testing.

### 3. Network Mode (`--mode network`)

Runs a **single node** in one process, expecting to connect to other nodes over the network.

```bash
# Terminal 1
uv run distbench -c configs/echo.yaml -a echo --mode network --id n1

# Terminal 2
uv run distbench -c configs/echo.yaml -a echo --mode network --id n2

# ...and so on for n3, n4...
```

- **Pros**: The most realistic, "production" mode.
- **Cons**: Requires manual startup on all machines.
- **Use For**: Final deployment and testing on a real cluster.

## Docker Deployment

Distbench can run in Docker containers with each node in a separate container. The system uses node IDs as hostnames in a dedicated Docker network.

### Quick Start

#### 1. Generate Docker Compose File

Use the `dockerize.py` script to create a docker-compose configuration:

```bash
python3 dockerize.py \
  -c configs/echo.yaml \
  -a echo \
  -o docker-compose.yaml
```

#### 2. Run the System

```bash
# Build and start all containers
docker-compose up --build

# Run in detached mode
docker-compose up --build -d

# View logs
docker-compose logs -f

# Stop and clean up
docker-compose down
```

### Script Options

```bash
Required:
  -c, --config PATH         Path to configuration YAML file
  -a, --algorithm NAME      Algorithm name (e.g., Echo)

Optional:
  -o, --output PATH         Output path (default: docker-compose.yaml)
  -f, --format FORMAT       Serialization: 'json'/'bincode/msgpack'
  -t, --timeout SECONDS     Timeout in seconds (default: 30)
  -v, --verbose             Verbosity (-v, -vv, -vvv)
  --latency RANGE           Network latency in ms (e.g., '10-50')
  --startup-delay MS        Startup delay in milliseconds (default: 600)
  --report-dir PATH         Directory for storing node reports
  --port PORT               Port number for all nodes (default: 8000)
```

### Examples

**With network latency simulation:**

```bash
python3 dockerize.py \
  -c configs/echo.yaml \
  -a echo \
  --latency 10-50 \
  -o docker-compose.yaml
```

**With report collection:**

```bash
mkdir reports
python3 dockerize.py \
  -c configs/echo.yaml \
  -a echo \
  --report-dir ./reports \
  -o docker-compose.yaml
```

## Tips and Best Practices

- **Termination**: Always call `await self.terminate()` when your node's logic is complete. Don't rely on the timeout.
- **Concurrency**: You do **not** need `Mutex` or locks in your algorithm. Each node's `Algorithm` instance runs in its own single-threaded `asyncio` task.
- **Handlers**: Keep handlers fast and non-blocking. Use `await` for I/O (like sending messages), but avoid long-running computations.
- **Logging**: Use `logger.info`, `logger.debug`, and `logger.trace` liberally. Use the `-v` and `-vv` flags to control verbosity.
- **Start Offline**: Always develop and debug your algorithm in `offline` mode first.
