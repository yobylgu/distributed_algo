# Distbench (Python Port)

> A Python framework for implementing and testing distributed algorithms, ported from the original Rust version.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org)

This project is a Python port of the `distbench` framework, designed for the lab assignments in TU Delft’s [Distributed Algorithms](https://studyguide.tudelft.nl/courses/study-guide/educations/14765) course.

It handles the infrastructure (networking, message passing, node lifecycle) so you can focus on algorithm logic, using modern `asyncio`, type hints, and simple decorators.

## ✨ Features

- 🎯 **Clean API** - Python decorators (`@distbench`, `@message`) eliminate boilerplate.
- 🔌 **Pluggable Transports** - In-memory (`offline`) or TCP sockets (`local`, `network`).
- 📦 **Multiple Formats** - JSON (human-readable) or `msgpack` (fast, binary).
- 🔄 **Three Execution Modes** - **offline** (single-process), **local** (multi-process on localhost), **network** (distributed).
- 🔐 **Automatic Signing** - Built-in Ed25519 signatures via `Signed[T]`, with automatic verification of all incoming messages.
- ⚡ **Automatic Lifecycle** - Node synchronization (key-sharing, startup) is handled for you.
- 🏗️ **Algorithm Layering** - Compose complex protocols from simpler building blocks using child algorithms.

## 🚀 Quick Start

### Prerequisites

- Python 3.10 or later
- `pip` (Python package installer)

### Installation

```bash
# Clone the repository
git clone [https://github.com/your-username/distbench-python](https://github.com/your-username/distbench-python)
cd distbench-python

# Sync uv environment and install dependencies
uv sync
```

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

- **[echo](distbench/algorithms/echo.py)** - Simple request-response pattern.
- **[chang_roberts](distbench/algorithms/chang_roberts.py)** - Ring-based leader election.
- **[message_chain](distbench/algorithms/message_chain.py)** - Demonstrates cryptographic signatures and message forwarding.
- **[simple_broadcast](distbench/algorithms/simple_broadcast.py)** - Demonstrates algorithm layering with parent-child communication.

## 📖 Documentation

- **[Implementation Guide](GUIDE.md)** - Learn how to implement your own algorithms in Python.

## 🎯 Usage

### Command-Line Options

```
Usage: distbench [OPTIONS]

Options:
  -c, --config PATH               Path to configuration YAML file  [required]
  -a, --algorithm TEXT            Name of algorithm to run  [required]
  -m, --mode [offline|local|network]
                                  Execution mode: offline (in-memory
                                  channels), local (TCP on localhost), network
                                  (TCP over network)  [default: offline]
  -f, --format [json|msgpack]     Serialization format  [default: json]
  -t, --timeout FLOAT             Timeout in seconds  [default: 30.0]
  -v, --verbose                   Increase verbosity (-v: DEBUG, -vv: TRACE)
  --id TEXT                       Node ID (required for --mode network)
  -p, --port-base INTEGER         Base port for --mode local  [default: 10000]
  --report-dir DIRECTORY          Directory to append node reports (as .jsonl
                                  files)
  -l, --latency TEXT              Network latency simulation in milliseconds
                                  (e.g. '10-50' for 10-50ms range, '20-20' for
                                  fixed 20ms)  [default: 0-0]
  -s, --startup-delay INTEGER     Startup delay in milliseconds before nodes
                                  begin  [default: 0]
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

| Feature             | Rust                     | Python                       |
| :------------------ | :----------------------- | :--------------------------- |
| **Async Runtime**   | Tokio                    | `asyncio`                    |
| **Code Generation** | Procedural Macros        | Decorators (`@distbench`)    |
| **Binary Format**   | Bincode                  | `msgpack`                    |
| **Cryptography**    | `ed25519-dalek`          | `PyNaCl`                     |
| **Type Safety**     | Compile-time (Rust)      | Static Analysis (`mypy`)     |
| **Concurrency**     | Multi-threaded (`Mutex`) | Single-threaded (`asyncio`)  |
| **Verification**    | Manual (in handler)      | **Automatic** (by framework) |

## 📄 License

This project is licensed under the MIT License.
