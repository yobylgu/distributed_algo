# Distbench Implementation Guide

This guide teaches you how to implement distributed algorithms using the Distbench framework.

## Table of Contents

- [Overview](#overview)
- [Step-by-Step Tutorial](#step-by-step-tutorial)
- [Algorithm API Reference](#algorithm-api-reference)
- [Signed Messages](#signed-messages)
- [Configuration](#configuration)
- [Example Algorithms](#example-algorithms)
- [Testing](#testing)

## Overview

Distbench algorithms consist of four main components:

1. **Message Definitions** - Data structures exchanged between nodes
2. **Algorithm State** - Per-node state and configuration
3. **Lifecycle Hooks** - Methods called during node startup/shutdown
4. **Message Handlers** - Functions that process incoming messages

## Step-by-Step Tutorial

### Step 1: Define Messages

Use the `#[distbench::message]` macro to define message types:

```rust
#[distbench::message]
struct Ping {
    sequence: u32,
}

#[distbench::message]
struct Pong {
    sequence: u32,
}
```

This automatically derives serialization traits and makes the message type usable by the framework.

### Step 2: Define Algorithm State

Use the `#[distbench::state]` macro to define your algorithm's state:

```rust
#[distbench::state]
pub struct PingPong {
    // Configuration fields (loaded from YAML)
    #[distbench::config]
    initiator: bool,

    #[distbench::config(default = 5)]
    max_rounds: u32,

    // Internal state (must implement Send + Default)
    pings_received: AtomicU64,
}
```

**Configuration attributes:**

- `#[distbench::config]` - Required field (must be in config file)
- `#[distbench::config(default = value)]` - Optional field with default

**Important:** All non-config fields must implement `Send` and `Default` traits.

### Step 3: Implement Algorithm Lifecycle

Implement the `Algorithm` trait to define lifecycle hooks:

```rust
use async_trait::async_trait;
use distbench::Algorithm;

#[async_trait]
impl Algorithm for PingPong {
    async fn on_start(&self) {
        // Called when all nodes are ready
        if self.initiator {
            if let Some((_, peer)) = self.peers().next() {
                peer.ping(&Ping { sequence: 0 }).await.ok();
            }
        }
    }

    async fn on_exit(&self) {
        // Called during shutdown for cleanup
        info!("PingPong finished");
    }

    async fn report(&self) -> Option<HashMap<impl Display, impl Display>> {
        // Optional: return metrics/results
        Some(hash_map! {
            "pings_received" => self.pings_received.load(Ordering::Relaxed).to_string()
        })
    }
}
```

### Step 4: Implement Message Handlers

Use the `#[distbench::handlers]` macro to define message handlers:

```rust
#[distbench::handlers]
impl PingPong {
    async fn ping(&self, src: PeerId, msg: &Ping) {
        info!("Received ping {} from {}", msg.sequence, src);
        self.pings_received.fetch_add(1, Ordering::Relaxed);

        // Send pong back to sender
        if let Some(peer) = self.peers().find(|(id, _)| **id == src) {
            peer.1.pong(&Pong { sequence: msg.sequence }).await.ok();
        }
    }

    async fn pong(&self, src: PeerId, msg: &Pong) -> Option<String> {
        info!("Received pong {} from {}", msg.sequence, src);

        if msg.sequence >= self.max_rounds {
            self.terminate().await;
            None
        } else {
            // Continue ping-pong
            if let Some(peer) = self.peers().find(|(id, _)| **id == src) {
                peer.1.ping(&Ping { sequence: msg.sequence + 1 }).await.ok();
            }
            Some("acknowledged".to_string())
        }
    }
}
```

**Handler patterns:**

- `async fn handler(&self, src: PeerId, msg: &MsgType)` - Fire-and-forget
- `async fn handler(&self, src: PeerId, msg: &MsgType) -> Option<Response>` - Request-response

The macro automatically generates peer proxy methods, so you can call `peer.ping(&msg)` and `peer.pong(&msg)`.

### Step 5: Create Configuration File

Create a YAML file in `configs/`:

```yaml
node1:
  neighbours: [node2]
  initiator: true
  max_rounds: 10

node2:
  neighbours: [node1]
  initiator: false
  max_rounds: 10
```

**Special features:**

- Empty `neighbours: []` creates a fully connected topology
- Algorithm-specific fields are passed to your state struct

### Step 6: Run Your Algorithm

The build system automatically discovers and registers your algorithm:

```bash
cargo run -- \
  --config configs/pingpong.yaml \
  --algorithm PingPong \
  --mode offline \
  --timeout 30
```

## Algorithm API Reference

### Methods Available on Algorithm State

When you use `#[distbench::state]`, your struct automatically gets these methods:

#### `self.id() -> &PeerId`

Returns the unique identifier of the current node.

```rust
info!("Node {} starting", self.id());
```

#### `self.N() -> u32`

Returns the total number of nodes in the entire network.

```rust
let total_neighbors = self.N();
let majority = (total_neighbors / 2) + 1;
```

#### `self.peers() -> impl Iterator<Item = (&PeerId, &Peer)>`

Returns an iterator over all neighboring peers.

```rust
// Broadcast to all peers
for (peer_id, peer) in self.peers() {
    peer.some_message(&msg).await?;
}

// Get first peer
if let Some((id, peer)) = self.peers().next() {
    peer.ping(&Ping { seq: 0 }).await?;
}
```

#### `self.terminate() -> impl Future<Output = ()>`

Signals that this node has completed execution.

```rust
async fn handler(&self, src: PeerId, msg: &Done) {
    info!("Algorithm complete, terminating");
    self.terminate().await;
}
```

**Important:** Nodes continue running until either:

- The timeout expires, OR
- All nodes have called `terminate()`

#### `self.sign<M: Digest>(value: M) -> Signed<M>`

Creates a cryptographically signed version of a message.

```rust
let signed_vote = self.sign(Vote {
    round: 1,
    value: "commit".to_string(),
});

for (_, peer) in self.peers() {
    peer.vote(&signed_vote).await?;
}
```

## Signed Messages

The framework provides `Signed<M>` for Byzantine fault-tolerant algorithms.

### Basic Usage

```rust
use distbench::signing::Signed;

#[distbench::message]
struct Vote {
    round: u32,
    value: String,
}

// Create signed message
let signed = self.sign(Vote { round: 1, value: "commit".to_string() });

// Handler receives Signed wrapper
async fn handle_vote(&self, src: PeerId, msg: &Signed<Vote>) {
    // Access inner value via Deref
    info!("Vote for round {}: {}", msg.round, msg.value);

    // Display shows signature
    info!("Received: {}", msg);  // Prints "Vote(...) (signed by node1)"
}
```

### Signed Messages in Collections

You can store signed messages in vectors, sets, and maps:

```rust
#[distbench::state]
pub struct ByzantineConsensus {
    proposals: Mutex<Vec<Signed<Proposal>>>,
    votes: Mutex<HashMap<String, Vec<Signed<Vote>>>>,
}
```

### Nested Signatures

You can chain signatures: `Signed<Signed<M>>` for multi-level authentication.

See **[Message Chain](src/algorithms/message_chain.rs)** for signature usage patterns.

## Configuration

### File Format

YAML configuration maps node IDs to node definitions:

```yaml
node_id:
  neighbours: [list of neighbor node IDs]
  host: "network address" # For network mode
  port: port_number # For network mode
  algorithm_field: value # Passed to algorithm
```

### Fully Connected Topology

Use empty `neighbours` list to connect to all other nodes:

```yaml
node1:
  neighbours: [] # Automatically connects to node2, node3, node4
  is_sender: true

node2:
  neighbours: []

node3:
  neighbours: []

node4:
  neighbours: []
```

This simplifies configuration for broadcast and consensus algorithms.

## Example Algorithms

Study these examples to learn different patterns:

### [Echo Algorithm](src/algorithms/echo.rs)

- **Pattern:** Request-response
- **Concepts:** Basic message handling, peer communication
- **Complexity:** Beginner

### [Chang-Roberts Election](src/algorithms/chang_roberts.rs)

- **Pattern:** Ring-based coordination
- **Concepts:** ID comparison, message forwarding, termination detection
- **Complexity:** Intermediate

### [Message Chain](src/algorithms/message_chain.rs)

- **Pattern:** Chain forwarding with authentication
- **Concepts:** Signed messages, message collections, deduplication
- **Complexity:** Advanced
- **Note:** Demonstrates `Signed<M>` in vectors

## Testing

### Offline Mode (Recommended for Development)

Runs all nodes in a single process with in-memory channels:

```bash
cargo run -- \
  --config configs/your_config.yaml \
  --algorithm YourAlgorithm \
  --mode offline \
  --timeout 10
```

**Advantages:**

- Fastest execution (no network overhead)
- Fully deterministic
- Easy debugging with a single log stream

### Local Mode

Runs each node as a separate thread communicating over `localhost`:

```bash
cargo run -- \
  --config configs/your_config.yaml \
  --algorithm YourAlgorithm \
  --mode local \
  -p 4242 \
  --nodes 4 \
  --timeout 10
```

**Advantages:**

- More realistic than Offline Mode
- Still runs in one OS process
- Good for testing timing and concurrency effects

### Network Mode

> For this mode, configuration needs to have ips and ports defined

Runs each node as a separate process communicating over TCP:

```bash
# Terminal 1
cargo run -- \
  --config configs/your_config.yaml \
  --id node1 \
  --algorithm YourAlgorithm \
  --mode network

# Terminal 2
cargo run -- \
  --config configs/your_config.yaml \
  --id node2 \
  --algorithm YourAlgorithm \
  --mode network
```

**Advantages:**

- Most realistic deployment behavior
- Useful for distributed system / cluster testing

### Logging

Control verbosity with `-v` flags:

```bash
# Basic logging
cargo run -- -v --config config.yaml --algorithm Echo

# Debug logging
cargo run -- -vv --config config.yaml --algorithm Echo

# Trace logging
cargo run -- -vvv --config config.yaml --algorithm Echo
```

### Unit Tests

Write unit tests for your algorithm logic:

```bash
cargo test
```

## Tips and Best Practices

### Termination

- Always call `self.terminate().await` when your algorithm completes
- Don't rely on timeout for normal termination
- All nodes should eventually terminate for clean shutdown

### Thread Safety

- Use `Mutex` or atomic types for shared state
- Don't hold `MutexGuard` across `.await` points
- Release locks before async operations

### Message Handling

- Keep handlers fast and non-blocking
- Use `cast` for fire-and-forget, `send` for request-response
- Handler return type determines response behavior

### Debugging

- Use `info!`, `debug!`, and `trace!` logging liberally
- Test in offline mode first
- Check `elapsed_time` in reports

## Troubleshooting

### Algorithm Not Found

Ensure your file is in `src/algorithms/` and uses `#[distbench::state]`:

```bash
cargo clean && cargo build
```

### Nodes Not Synchronizing

- Check that all node IDs in config are unique
- Verify neighbor lists are correct
- Ensure bidirectional connections (if A lists B, B should list A)

### Handler Not Called

- Verify `#[distbench::handlers]` is on the impl block
- Check message type name matches handler function name exactly
- Ensure handler signature is correct: `async fn name(&self, src: PeerId, msg: &MsgType)`

### Messages Not Delivered

- Check that peers are in the neighbors list
- Verify nodes are in Running state when sending
- Check for errors in handler code (use `.await?` or `.await.ok()`)

## Further Reading

- **[Architecture Documentation](ARCHITECTURE.md)** - Framework internals
- **[Python Port Guide](PYTHON_PORT.md)** - Implementation in other languages
- **Rust Async Book** - Understanding async/await
- **Tokio Documentation** - Async runtime details

## Getting Help

- **Issues**: Report bugs or request features on GitHub
- **Discussions**: Ask questions in GitHub Discussions
- **Examples**: Study the algorithms in `src/algorithms/`

---

Happy building! 🚀
