# Architecture Documentation

This document describes the architecture of the distributed algorithms framework. It is designed to provide sufficient detail for porting the framework to another language while remaining language-agnostic.

## Overview

The framework provides infrastructure for implementing and testing distributed algorithms. It abstracts away networking, message passing, and node lifecycle management, allowing developers to focus on algorithm logic.

### Core Concepts

- **Node**: A participant in the distributed system that executes an algorithm
- **Community**: A collection of nodes that can communicate with each other
- **Algorithm**: The distributed algorithm logic that runs on each node
- **Transport**: The underlying communication mechanism (TCP, in-memory channels, etc.)
- **Message**: Data exchanged between nodes

## System Architecture

### Layered Design

```
┌─────────────────────────────────┐
│   Algorithm Implementation      │  User-defined algorithm logic
├─────────────────────────────────┤
│   Algorithm Framework Layer     │  State, handlers, lifecycle
├─────────────────────────────────┤
│   Node Management Layer         │  Node lifecycle, coordination
├─────────────────────────────────┤
│   Community Layer              │  Peer discovery, status tracking
├─────────────────────────────────┤
│   Connection Management        │  Connection pooling, retries
├─────────────────────────────────┤
│   Transport Layer              │  Network abstraction
└─────────────────────────────────┘
```

## Component Details

### 1. Transport Layer

The transport layer provides an abstraction over different communication mechanisms.

#### Responsibilities
- Establishing connections between nodes
- Sending and receiving raw bytes
- Serving incoming connections
- Connection lifecycle management

#### Key Abstractions

**Address**: Identifies a node's network location. Must be:
- Hashable and comparable (for use in maps/sets)
- Cloneable
- Thread-safe
- Convertible to string for display

**Connection**: Represents an active connection to another node. Provides:
- `send(bytes)`: Send message and wait for response (request-response pattern)
- `cast(bytes)`: Send message without waiting for response (fire-and-forget)
- `close()`: Terminate the connection

**Transport**: Manages the overall networking. Provides:
- `connect(address)`: Establish a connection to a peer
- `serve(server, stop_signal)`: Start accepting incoming connections

#### Implementation Notes
- Connections should be multiplexable (multiple concurrent messages)
- Transport implementations should handle reconnection logic
- Both synchronous (request-response) and asynchronous (cast) messaging must be supported

### 2. Connection Management Layer

Sits above the transport layer to manage connections efficiently.

#### Responsibilities
- Lazy connection establishment
- Connection pooling and reuse
- Automatic reconnection on failures
- Thread-safe access to connections

#### Connection Manager Interface
- `new(transport, address)`: Create a manager for a specific peer
- `send(bytes)`: Send and wait for response (establishes connection if needed)
- `cast(bytes)`: Send without response (establishes connection if needed)

#### Design Pattern
Connection managers act as per-peer proxies that:
1. Lazily establish connections on first use
2. Cache connections for reuse
3. Handle connection failures transparently
4. Provide thread-safe access

### 3. Community Layer

Manages the set of peers a node can communicate with.

#### Responsibilities
- Maintaining the set of neighbor nodes
- Mapping addresses to peer identities
- Tracking peer status (not started, starting, running, stopping, terminated)
- Providing access to connection managers
- Storing peer metadata (public keys, etc.)

#### Data Structures
- **Neighbors Set**: The subset of peers this node directly communicates with
- **Connections Map**: Maps peer IDs to their connection managers
- **Address Map**: Maps network addresses to peer IDs (for identifying incoming messages)
- **Status Map**: Tracks the current lifecycle status of each peer
- **Keys Map**: Stores cryptographic keys for peers (optional)

#### Key Operations
- `neighbours()`: Get connection managers for all neighbor peers
- `connection(peer_id)`: Get connection manager for a specific peer
- `id_of(address)`: Look up peer ID from network address
- `set_status(peer_id, status)`: Update a peer's status
- `statuses()`: Get current status of all peers

### 4. Node Management Layer

Coordinates the lifecycle of a single node in the system.

#### Node States
1. **NotStarted**: Initial state
2. **KeySharing**: Exchanging cryptographic public keys with peers
3. **Starting**: Node is synchronizing startup with peers
4. **Running**: Algorithm is executing
5. **Stopping**: Node is coordinating shutdown
6. **Terminated**: Node has stopped

#### State Transitions

```
NotStarted
    ↓
KeySharing ───→ (exchange public keys)
    ↓
Starting ─────→ (sync with peers)
    ↓
Running ─────→ (algorithm executes)
    ↓
Stopping ────→ (coordinate shutdown)
    ↓
Terminated
```

#### Responsibilities
- Managing node lifecycle state
- Synchronizing startup with other nodes
- Coordinating shutdown with other nodes
- Invoking algorithm lifecycle hooks
- Handling incoming messages
- Routing messages to algorithm handlers

#### Synchronization Protocol

**Key Exchange Phase** (KeySharing state):
1. Node generates a unique Ed25519 key pair
2. Node announces its public key to all peers
3. Node waits for public keys from all peers
4. Public keys are stored in the Community's KeyStore
5. Transition to Starting state when all keys received

**Startup Synchronization**:
1. Node enters Starting state
2. Node broadcasts "Started" message to all peers
3. Node waits for "Started" from all peers
4. When all peers have started, transition to Running
5. Call algorithm's `on_start()` hook

**Shutdown Synchronization**:
1. Algorithm calls `terminate()`
2. Node enters Stopping state
3. Node broadcasts "Finished" message to all peers
4. Node waits for "Finished" from all peers
5. When all peers have finished, transition to Terminated
6. Call algorithm's `on_exit()` hook

#### Message Handling
Nodes handle two types of messages:
1. **Node Messages**: Lifecycle coordination (Started, Finished)
2. **Algorithm Messages**: Algorithm-specific logic

### 5. Algorithm Framework Layer

Provides the programming model for implementing distributed algorithms.

#### Algorithm Trait

Algorithms must implement:
- `on_start()`: Called when all nodes are ready (after synchronization)
- `on_exit()`: Called during shutdown (for cleanup)
- `report()`: Optional method to return algorithm-specific metrics/results
- `terminate()`: Signal that this node wants to stop
- `terminated()`: Check if algorithm has terminated

#### Message Handler Trait

Algorithms handle incoming messages:
- `handle(src, msg_type_id, msg_bytes, format)`: Process incoming message
  - Returns `Some(response_bytes)` for request-response
  - Returns `None` for fire-and-forget messages

#### Algorithm State

Each algorithm has:
- **Configuration Fields**: Loaded from config file at startup
- **Internal State**: Algorithm-specific data
- **Peer Access**: Methods to access connection managers for sending messages:
  - `peers()`: Iterator over all neighbor peers
  - `id()`: Get this node's unique identifier
  - `N()`: Get total number of nodes in the system
  - `sign(message)`: Create cryptographically signed message
- **Termination Signal**: Mechanism to signal completion

### 6. Serialization Layer

Provides abstraction over serialization formats.

#### Format Trait
- `serialize(value)`: Convert object to bytes
- `deserialize(bytes)`: Convert bytes to object
- `name()`: Format identifier (for logging)

#### Two-Level Serialization

**Outer Layer** (Node Messages):
- Uses a fixed binary format (rkyv)
- Handles node lifecycle messages
- Wraps algorithm messages

**Inner Layer** (Algorithm Messages):
- Pluggable format (JSON, bincode, etc.)
- User-configurable
- Algorithm-specific messages

#### Message Envelope Format
```
NodeMessage::Algorithm(type_id, payload_bytes)
```
Where:
- `type_id`: String identifying the message type (for deserialization)
- `payload_bytes`: Serialized algorithm message (using chosen format)

## Message Flow

### Sending a Message

1. Algorithm calls peer method: `peer.my_message(&msg).await`
2. Framework serializes message using configured format
3. Message is wrapped in NodeMessage envelope
4. Envelope is serialized with rkyv
5. Connection manager sends bytes via transport
6. For request-response, wait for reply and deserialize

### Receiving a Message

1. Transport receives bytes from network
2. Server handler is invoked with source address and bytes
3. Outer envelope (NodeMessage) is deserialized with rkyv
4. If lifecycle message (Started/Finished): update peer status
5. If algorithm message: extract type ID and payload
6. Algorithm handler is invoked
7. Handler deserializes payload using configured format
8. Handler processes message and optionally returns response
9. Response is serialized and sent back

## Configuration System

### Configuration File Format

YAML file mapping node IDs to node definitions:

```yaml
node_id:
  neighbours: [list of neighbor node IDs]
  host: "network address"
  port: port_number
  algorithm_specific_field: value
```

**Special Behavior**:
- If `neighbours` is an empty list, the node automatically connects to all other nodes (fully connected topology)
- This simplifies configuration for algorithms that require full connectivity (e.g., broadcast, consensus)

### Configuration Loading

1. Parse YAML file into map of node definitions
2. Extract standard fields (neighbours, host, port)
3. Remaining fields are passed to algorithm as configuration
4. Algorithm factory deserializes config into typed configuration struct

### Algorithm Configuration

Algorithms define configuration via attributes:
- Required fields: Must be present in config file
- Optional fields: Use default value if not specified

## Execution Modes

### Offline Mode

- All nodes run in same process
- Uses in-memory channels for communication
- No network overhead
- Useful for testing and debugging
- Deterministic ordering possible

### Network Mode

- Each node runs in separate process
- Uses TCP sockets for communication
- Real network conditions
- Production deployment
- True distribution

## Code Generation

The framework uses build-time code generation:

### Algorithm Registry

1. Scan `src/algorithms` directory
2. Find structs marked with algorithm state attribute
3. Generate macro that maps algorithm names to implementations
4. Allows dynamic algorithm selection at runtime

### Generated Components

For each algorithm, generate:
- Configuration struct
- Algorithm factory implementation
- Message handler dispatch logic
- Peer proxy methods

## Implementation Requirements

### Language-Agnostic Requirements

1. **Async/Concurrent Runtime**: Need non-blocking I/O and task spawning
2. **Serialization**: Need at least one binary and one text format
3. **Networking**: TCP sockets with async I/O
4. **Thread Safety**: Shared state must be safely accessible
5. **Weak References**: For avoiding circular references in shutdown

### Critical Implementation Details

#### Termination Handling

- Nodes must support both self-termination and external stop signals
- Algorithm termination should trigger graceful shutdown
- Must handle case where some nodes terminate before others

#### Status Tracking

- Status updates must be atomic
- Status checks must be consistent
- Race conditions during startup/shutdown must be handled

#### Connection Lifecycle

- Connections should be established lazily
- Failed connections should be retried
- Connection cleanup on shutdown

#### Message Ordering

- No strict ordering guarantees required
- Implementation may provide ordered delivery per connection
- Algorithm should not assume global message ordering

## Extensibility Points

### Custom Transports

New transport implementations can:
- Use different protocols (UDP, QUIC, etc.)
- Implement custom connection pooling strategies
- Add encryption, compression, etc.

### Custom Serialization Formats

New formats can:
- Trade speed for size (or vice versa)
- Support schema evolution
- Add type safety or validation

### Custom Connection Managers

Can implement:
- Connection pooling strategies
- Load balancing across multiple connections
- Circuit breakers and backoff strategies
- Custom retry logic

## Testing Strategy

### Unit Testing
- Test individual components in isolation
- Mock transport layer for algorithm testing
- Test configuration parsing

### Integration Testing
- Test complete node lifecycle
- Test multi-node coordination
- Test failure scenarios

### Offline Mode Testing
- Run complete distributed algorithm in single process
- Deterministic testing with controlled message ordering
- Fast iteration without network overhead

## Performance Considerations

### Memory
- Connection managers cache connections
- Status maps grow with number of peers
- Message buffers are temporary

### CPU
- Serialization/deserialization overhead
- Message routing and dispatch
- State synchronization

### Network
- Startup synchronization requires all-to-all communication
- Shutdown synchronization requires all-to-all communication
- Algorithm messages depend on algorithm design

## Security Considerations

### Cryptographic Signing

The framework provides built-in support for cryptographically signed messages:

#### Key Management
- Each node generates a unique key pair on startup
- Public keys are automatically exchanged during the startup synchronization phase
- Keys are stored in a KeyStore within the Community
- Uses Ed25519 signatures via the `ed25519-dalek` library

#### Signed Messages
- Messages can be wrapped in `Signed<M>` type
- Signed messages include:
  - The original message content
  - A cryptographic signature
  - The signer's peer ID
- Signatures are created using `algorithm.sign(message)`
- Signatures can be verified against the KeyStore
- Useful for Byzantine fault-tolerant algorithms

#### Trust Model
- Nodes exchange public keys during startup
- Each node trusts the authenticity of exchanged keys
- Signed messages prove message origin and integrity
- No certificate authority or PKI required
- No built-in message encryption (signatures only provide authentication, not confidentiality)

### Attack Vectors
- Malicious messages (mitigated by type-safe deserialization and optional signing)
- Resource exhaustion (no rate limiting built-in)
- Network partitions (no automatic handling)
- Man-in-the-middle during key exchange (no authentication of initial key exchange)

## Future Extensions

Possible enhancements:
- Failure detection and recovery
- Dynamic topology changes
- Message encryption
- Rate limiting and backpressure
- Metrics and observability
- Checkpointing and recovery
