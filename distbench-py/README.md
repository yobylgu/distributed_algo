# Distbench (Python Port)

> A Python framework for implementing and testing distributed algorithms, ported from the original Rust version.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org)

This project is a Python port of the `distbench` framework, designed for the lab assignments in TU Delft’s [Distributed Algorithms](https://studyguide.tudelft.nl/courses/study-guide/educations/14765) course.

It handles the infrastructure (networking, message passing, node lifecycle) so you can focus on algorithm logic, using modern `asyncio`, type hints, and simple decorators.

## ✨ Features

-   🎯 **Clean API** - Python decorators (`@distbench`, `@message`) eliminate boilerplate.
-   🔌 **Pluggable Transports** - In-memory (`offline`) or TCP sockets (`local`, `network`).
-   📦 **Multiple Formats** - JSON (human-readable) or `msgpack` (fast, binary).
-   🔄 **Three Execution Modes** - **offline** (single-process), **local** (multi-process on localhost), **network** (distributed).
-   🔐 **Automatic Signing** - Built-in Ed25519 signatures via `Signed[T]`, with automatic verification of all incoming messages.
-   ⚡ **Automatic Lifecycle** - Node synchronization (key-sharing, startup) is handled for you.

## 🚀 Quick Start

### Prerequisites

-   Python 3.10 or later
-   `pip` (Python package installer)

### Installation

```bash
# Clone the repository
git clone [https://github.com/your-username/distbench-python](https://github.com/your-username/distbench-python)
cd distbench-python

# Sync uv environment and install dependencies
uv sync
````

### Running an Example

Run the Echo broadcast algorithm in `local` mode (spawns all nodes on your machine):

```bash
uv run distbench -c configs/echo.yaml -a echo --mode local -v
```

Run the Chang-Roberts leader election algorithm in `offline` mode (single process):

```bash
uv run distbench -c configs/chang_roberts.yaml -a chang_roberts --mode offline -v
```

## 📚 Example Algorithms

The framework automatically discovers any algorithm in the `distbench/algorithms/` directory.

  - **[echo](https://www.google.com/search?q=distbench/algorithms/echo.py)** - Simple request-response pattern.
  - **[chang\_roberts](https://www.google.com/search?q=distbench/algorithms/chang_roberts.py)** - Ring-based leader election.
  - **[message\_chain](https://www.google.com/search?q=distbench/algorithms/message_chain.py)** - Demonstrates cryptographic signatures and message forwarding.

## 📖 Documentation

  - **[Implementation Guide](GUIDE.md)** - Learn how to implement your own algorithms in Python.

## 🎯 Usage

### Command-Line Options

```
Usage: distbench [OPTIONS]

Options:
  -c, --config PATH               Path to configuration YAML file. [required]
  -a, --algorithm TEXT            Name of algorithm to run (must match filename
                                  in algorithms/). [required]
  -m, --mode [offline|local|network]
                                  Execution mode. [default: offline]
  -f, --format [json|msgpack]     Serialization format. [default: json]
  -t, --timeout FLOAT             Timeout in seconds. [default: 30.0]
  -v, --verbose                   Increase verbosity (-v: DEBUG, -vv: TRACE)
  --id TEXT                       Node ID (required for --mode network)
  --port-base INTEGER             Base port for --mode local. [default: 10000]
  --report-dir PATH               Directory to append node reports (as .jsonl
                                  files).
  --help                          Show this message and exit.
```

### Configuration

Create a YAML configuration file. The format is compatible with the Rust version.

```yaml
# configs/echo.yaml
n1:
  neighbours: [] # Empty list means fully connected
  start_node: true

n2:
  neighbours: []
  start_node: false

n3:
  neighbours: []
  start_node: false
```

**Key Feature**: An empty `neighbours: []` list creates a **fully connected topology**, automatically connecting the node to all other nodes defined in the file.

## 🏗️ Project Structure

```
distbench/
├── distbench/              # The main Python package
│   ├── algorithms/         # Algorithm implementations (auto-scanned)
│   │   ├── __init__.py     # (Handles automatic registration)
│   │   └── ...
│   ├── encoding/           # Serialization (json, msgpack)
│   │   ├── format.py
│   │   ├── json_format.py
│   │   └── msgpack_format.py
│   ├── transport/            # Network abstractions
│   │   ├── base.py
│   │   ├── offline.py      # In-memory transport (offline mode)
│   │   └── tcp.py          # TCP transport (local & network modes)
│   ├── __init__.py
│   ├── algorithm.py        # Algorithm base class
│   ├── community.py        # Peer management
│   ├── config.py           # YAML config parsing
│   ├── connection.py       # Connection manager (retries, pooling)
│   ├── context.py          # Logging context (for node IDs)
│   ├── decorators.py       # @distbench, @message, @handler
│   ├── main.py             # CLI entry point
│   ├── messages.py         # Node-level message envelope
│   ├── node.py             # Node lifecycle and coordination
│   └── signing.py          # Ed25519 signing and Signed[T] wrapper
├── pyproject.toml          # Project definition and dependencies
├── README.md               # This file
└── GUIDE.md                # How to implement algorithms
```

## 🔧 Development

This project uses `ruff` for linting/formatting and `mypy` for type checking.

```bash
# Run the linter
uv run ruff check .

# Format all code
uv run ruff format .

# Run the static type checker
uv run mypy .
```

## ↔️ Comparison with Rust Version

This port maintains the same core architecture but adapts it to be idiomatic Python.

| Feature | Rust | Python |
| :--- | :--- | :--- |
| **Async Runtime** | Tokio | `asyncio` |
| **Code Generation** | Procedural Macros | Decorators (`@distbench`) |
| **Binary Format** | Bincode | `msgpack` |
| **Cryptography** | `ed25519-dalek` | `PyNaCl` |
| **Type Safety** | Compile-time (Rust) | Static Analysis (`mypy`) |
| **Concurrency** | Multi-threaded (`Mutex`) | Single-threaded (`asyncio`) |
| **Verification** | Manual (in handler) | **Automatic** (by framework) |

## 📄 License

This project is licensed under the MIT License.
