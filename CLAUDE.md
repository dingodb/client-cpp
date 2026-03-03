# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **TiKV Client for C++** - a C++ client library for TiKV (distributed transactional key-value database). It is built on top of the [TiKV Client in Rust](https://github.com/tikv/client-rust) via [cxx](https://github.com/dtolnay/cxx) - a safe FFI bridge between Rust and C++.

**Status**: Proof-of-concept stage, under heavy development. Currently only supports synchronous API.

## Build Commands

### Prerequisites
```bash
# Install Rust environment
curl https://sh.rustup.rs -sSf | sh
```

### Build the Library
```bash
# Debug build
cmake -S . -B build && cmake --build build

# Release build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

# Install to /usr/local
sudo cmake --install build
```

### Build and Run Examples
Examples require a running TiKV server:
```bash
# Start TiKV (using tiup)
tiup playground nightly

# Build examples
cd example && cmake -S . -B build && cmake --build build

# Run examples
./build/raw   # Raw KV API example
./build/txn   # Transaction API example
```

## Architecture

### Hybrid Rust/C++ Architecture

This project uses a dual-language architecture with Rust providing the core client functionality and C++ providing the public API:

1. **Rust Layer** (`src/lib.rs`): Implements the FFI bridge using `cxx`. It wraps the `tikv-client` Rust crate and exposes functions to C++ via the `#[cxx::bridge]` macro.

2. **C++ Wrapper Layer** (`src/tikv_client.cpp`, `include/tikv_client.h`): Provides idiomatic C++ classes (`RawKVClient`, `TransactionClient`, `Transaction`) that wrap the Rust FFI functions.

3. **Generated Bindings**: During build, `cxx` generates C++ headers and source files from the Rust bridge:
   - `build/cxxbridge/client-cpp/src/lib.rs.h` (generated header)
   - `build/cxxbridge/client-cpp/src/lib.rs.cc` (generated source)

### Build System Integration

- **CMake** orchestrates the entire build process
- CMake invokes `cargo` to build the Rust static library (`libtikvrust.a`)
- The C++ library (`libtikvcpp.a`) links against the Rust static library
- Both libraries must be linked by consumers (see `example/CMakeLists.txt`)

### Key API Classes

- **`tikv_client::RawKVClient`**: Simple key-value operations (`get`, `put`, `scan`, `remove`, `batch_put`, `remove_range`)
- **`tikv_client::TransactionClient`**: Transaction management (`begin`, `begin_pessimistic`)
- **`tikv_client::Transaction`**: Individual transaction operations (`get`, `put`, `scan`, `commit`, `rollback`)
- **`tikv_client::KvPair`**: Key-value pair structure

### Important Implementation Details

- Each Rust client struct (`RawKVClient`, `TransactionClient`) contains its own `tokio::runtime::Runtime` for executing async Rust code synchronously
- The `Transaction` struct holds a `tokio::runtime::Handle` cloned from its parent client
- Raw KV operations accept a timeout parameter (milliseconds); transactional operations do not
- `batch_get_for_update` is currently unimplemented (returns error)

### Dependencies

**Rust** (via Cargo): `tikv-client`, `cxx`, `tokio`, `futures`, `anyhow`, `log`, `libc`, `env_logger`

**System Libraries**: `pthread`, `dl`, `ssl`, `crypto`

**C++ Standard**: C++17
