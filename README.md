# go-p2p

High-performance P2P networking library for blockchain applications in Go.

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Overview

`go-p2p` is a production-grade P2P networking library designed for blockchain and distributed systems. It features lock-free data structures, optimized I/O patterns, and extensive benchmarking to ensure maximum performance under high concurrency.

### Key Features

- **Lock-free Data Structures**: Benchmarked implementations with detailed performance analysis
- **Optimized Peer Management**: `sync.Map` based concurrent peer tracking
- **Efficient Message Broadcasting**: Gossip protocol with duplicate filtering
- **Zero-allocation Networking**: `sync.Pool` based buffer reuse
- **Pluggable Discovery**: Seed nodes, gossip-based peer exchange

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                           Node                               │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Server    │  │   Dialer    │  │    PeerManager      │  │
│  │  (Accept)   │  │  (Connect)  │  │    (sync.Map)       │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         │                │                     │             │
│         └────────┬───────┘                     │             │
│                  ▼                             │             │
│  ┌───────────────────────────────┐            │             │
│  │        Peer Connections        │◄───────────┘             │
│  │  ┌──────┐ ┌──────┐ ┌──────┐   │                          │
│  │  │Peer A│ │Peer B│ │Peer C│   │                          │
│  │  └──┬───┘ └──┬───┘ └──┬───┘   │                          │
│  └─────┼────────┼────────┼───────┘                          │
│        │        │        │                                   │
│        ▼        ▼        ▼                                   │
│  ┌───────────────────────────────┐                          │
│  │     BroadcastManager          │                          │
│  │      (sync.Map cache)         │                          │
│  └───────────────┬───────────────┘                          │
│                  │                                           │
│                  ▼                                           │
│  ┌───────────────────────────────┐                          │
│  │          Mempool              │                          │
│  │      (Mutex + Heap)           │                          │
│  └───────────────────────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Benchmark Results

> Comprehensive benchmarks comparing lock-free vs mutex-based implementations.

### Test Environment

| Spec | Value |
|------|-------|
| OS | Windows 10 |
| CPU | AMD Ryzen 5 3500 (6-Core) |
| Go | 1.21+ |
| Command | `go test -bench=. -benchmem -count=5` |

### Summary

```
┌────────────────────┬─────────────┬─────────────────────────────────┐
│ Data Structure     │ Winner      │ Notes                           │
├────────────────────┼─────────────┼─────────────────────────────────┤
│ Queue              │ Mutex       │ Go mutex highly optimized       │
│ HashMap (read)     │ sync.Map    │ No contention between readers   │
│ HashMap (write)    │ Mutex       │ CAS retry overhead              │
│ Priority Queue     │ Mutex       │ Heap more cache-friendly        │
└────────────────────┴─────────────┴─────────────────────────────────┘
```

### Detailed Benchmarks

#### Queue (Single Thread)

```
Operation   Mutex       Lock-free
─────────────────────────────────────
Enqueue     85 ns       80 ns
Dequeue     15 ns       18 ns
```

#### Queue (Concurrent - Mixed Operations)

```
Goroutines  Mutex       Lock-free   Diff
───────────────────────────────────────────
1           88 ns       144 ns      +64%
4           83 ns       149 ns      +79%
8           83 ns       149 ns      +79%
16          85 ns       143 ns      +68%
32          85 ns       140 ns      +65%
64          82 ns       146 ns      +78%
```

**Result**: Mutex wins. Lock-free queue allocates per-enqueue (32 B/op).

#### HashMap (Concurrent - 75% Read, 25% Write)

```
Goroutines  Mutex       Lock-free   Diff
───────────────────────────────────────────
1           377 ns      194 ns      -49%
4           375 ns      189 ns      -50%
8           377 ns      188 ns      -50%
16          371 ns      184 ns      -50%
32          377 ns      197 ns      -48%
```

**Result**: Lock-free (sync.Map) wins by 2x for read-heavy workloads.

#### Priority Queue (Concurrent)

```
Goroutines  Mutex(Heap) Lock-free(SkipList)
─────────────────────────────────────────────
1           166 ns      ~200 ns
4           184 ns      ~200 ns
8           182 ns      ~200 ns
16          184 ns      ~200 ns
```

**Result**: Mutex+Heap wins. Skip list has high memory overhead (311 B/op per node).

### Benchmark Screenshots

> Add your benchmark screenshots here

<!--
실제 벤치마크 실행 결과 스크린샷을 여기에 추가하세요.

#### Queue Benchmark
![Queue Benchmark](docs/images/bench-queue.png)

#### HashMap Benchmark
![HashMap Benchmark](docs/images/bench-hashmap.png)

#### Priority Queue Benchmark
![Priority Queue Benchmark](docs/images/bench-pq.png)
-->

```
[INSERT BENCHMARK SCREENSHOT HERE]
```

---

## Implementation Decisions

Based on benchmark results, we selected optimal implementations for each component:

| Component | Implementation | Rationale |
|-----------|---------------|-----------|
| **PeerManager** | `sync.Map` | 90%+ reads, lock-free reads |
| **BroadcastCache** | `sync.Map` | Duplicate check is read-heavy |
| **Mempool** | `Mutex + Heap` | Heap is cache-friendly, simple |
| **MessageQueue** | Channel | Go channels well-optimized |

### Why sync.Map for Read-Heavy Workloads?

```go
// sync.Map internal structure
type Map struct {
    mu     Mutex          // protects dirty map
    read   atomic.Value   // read-only map (lock-free!)
    dirty  map[any]*entry // write map
    misses int
}

// Load operation (read)
// If key exists in read map → atomic load, NO LOCK
// Only falls back to dirty map if not found
```

**RWMutex Problem**: Even `RLock()` requires atomic increment of `readerCount`, causing cache line contention across cores.

**sync.Map Solution**: Reads from `read` map use only `atomic.Load` - no shared counter, no contention.

---

## BroadcastManager Optimizations

The message broadcasting hot path was optimized for maximum throughput. All benchmarks run on AMD Ryzen 5 3500 (6-Core).

### Hash Function: SHA256 → xxhash

Message deduplication requires hashing every incoming message. We replaced cryptographic SHA256 with non-cryptographic xxhash.

| Implementation | ns/op | B/op | allocs/op | Speedup |
|----------------|------:|-----:|----------:|--------:|
| SHA256 + hex.EncodeToString | 426.8 | 160 | 3 | baseline |
| xxhash + sync.Pool | 45.0 | 0 | 0 | **9.5x** |
| xxhash.Sum64 (direct) | 25.5 | 0 | 0 | **16.7x** |

**Why 0 B/op?**
- `sync.Pool` reuses `xxhash.Digest` instances across calls
- After pool warmup, no new allocations occur in steady state
- Direct `Sum64` benchmark pre-allocates buffer before timing

```go
// Before: 426.8 ns/op, 160 B/op, 3 allocs/op
hasher := sha256.New()
hasher.Write([]byte{byte(msg.Type)})
hasher.Write(msg.Payload)
hash := hex.EncodeToString(hasher.Sum(nil))  // string key

// After: 45.0 ns/op, 0 B/op, 0 allocs/op
h := digestPool.Get().(*xxhash.Digest)
h.Reset()
h.Write([]byte{byte(msg.Type)})
h.Write(msg.Payload)
hash := h.Sum64()  // uint64 key
digestPool.Put(h)
```

### SeenCache: RWMutex → sync.Map

The duplicate message cache is read-heavy (90%+ lookups are cache hits).

| Scenario | sync.Map | RWMutex | Speedup |
|----------|------:|-------:|--------:|
| ReadOnly (100% read) | 10.12 ns/op | 40.46 ns/op | **4.0x** |
| ReadHeavy (90% read, 10% write) | 83.88 ns/op | 123.7 ns/op | **1.5x** |
| WriteHeavy (50% read, 50% write) | ~equal | ~equal | 1.0x |

**Key insight**: P2P networks see the same message multiple times from different peers. Most `MarkSeen()` calls hit existing entries → read-heavy workload → sync.Map wins.

### RNG: Global → Goroutine-Local

Gossip target selection requires shuffling peer lists.

| Implementation | Behavior |
|----------------|----------|
| `rand.Shuffle()` (global) | Lock contention under parallel calls |
| `rng.Shuffle()` (local) | No contention, each goroutine has own RNG |

```go
// Before: global RNG with internal mutex
rand.Shuffle(len(candidates), func(i, j int) { ... })

// After: goroutine-local RNG (race-free)
rng := rand.New(rand.NewSource(time.Now().UnixNano()))
rng.Shuffle(len(candidates), func(i, j int) { ... })
```

### Full Hot Path Improvement

Message receive path: `Receive → Hash → MarkSeen → Broadcast`

| Component | Before | After | Improvement |
|-----------|-------:|------:|------------:|
| MessageHash | 426.8 ns | 45.0 ns | 9.5x faster |
| MarkSeen (cache hit) | 40.46 ns | 10.12 ns | 4.0x faster |
| Memory allocation | 160 B/msg | 0 B/msg | **zero-alloc** |

**Total hot path: ~10x faster, zero allocations**

### Run BroadcastManager Benchmarks

```bash
# All broadcast benchmarks
go test -bench=. -benchmem ./pkg/node/... -run=^$

# Hash comparison only
go test -bench=BenchmarkHash -benchmem ./pkg/node/...

# SeenCache comparison only
go test -bench=BenchmarkSeenCache -benchmem ./pkg/node/...

# Full flow comparison
go test -bench=BenchmarkFullFlow -benchmem ./pkg/node/...
```

---

## Test Results

### Unit Tests

```bash
$ go test ./...

ok  	github.com/go-p2p-network/go-p2p/pkg/mempool    0.222s
ok  	github.com/go-p2p-network/go-p2p/pkg/node       6.736s
```

### Test Coverage

| Package | Tests | Coverage |
|---------|-------|----------|
| `pkg/mempool` | 14 tests | Transaction pool, priority, validation |
| `pkg/node` | 11 tests | P2P connections, broadcast, discovery |

### Test Screenshots

> Add your test execution screenshots here

```
[INSERT TEST SCREENSHOT HERE]
```

---

## Quick Start

### Installation

```bash
go get github.com/go-p2p-network/go-p2p
```

### Run a Node

```bash
# Start node on port 3000
go run cmd/node/main.go --port 3000

# Start second node, connect to first
go run cmd/node/main.go --port 3001 --seed localhost:3000

# Start third node
go run cmd/node/main.go --port 3002 --seed localhost:3000
```

### Use as Library

```go
package main

import (
    "github.com/go-p2p-network/go-p2p/pkg/node"
    "github.com/go-p2p-network/go-p2p/pkg/protocol"
)

func main() {
    cfg := node.Config{
        ListenAddr: ":3000",
        MaxPeers:   50,
        Seeds:      []string{"seed1.example.com:3000"},
    }

    n := node.New(cfg)

    // Register message handler
    n.OnMessage(protocol.MsgTx, func(p *peer.Peer, msg *protocol.Message) {
        // Handle transaction
    })

    n.Start()
}
```

---

## Running Tests

### All Tests

```bash
go test ./...
```

### Verbose Output

```bash
go test -v ./...
```

### Specific Package

```bash
# Node tests (P2P integration)
go test -v ./pkg/node/...

# Mempool tests
go test -v ./pkg/mempool/...
```

### With Race Detector

```bash
go test -race ./...
```

---

## Running Benchmarks

### All Benchmarks

```bash
go test -bench=. -benchmem ./pkg/ds/...
```

### Specific Benchmarks

```bash
# Queue only
go test -bench=BenchmarkQueue -benchmem ./pkg/ds/...

# HashMap only
go test -bench=BenchmarkHashMap -benchmem ./pkg/ds/...

# Priority Queue only
go test -bench=BenchmarkPriorityQueue -benchmem ./pkg/ds/...
```

### With Multiple Iterations (Stable Results)

```bash
go test -bench=. -benchmem -count=5 ./pkg/ds/...
```

### CPU Profiling

```bash
go test -bench=BenchmarkQueue -cpuprofile=cpu.prof ./pkg/ds/...
go tool pprof cpu.prof
```

### Compare Results

```bash
# Install benchstat
go install golang.org/x/perf/cmd/benchstat@latest

# Run and compare
go test -bench=. -count=10 ./pkg/ds/sync/ > mutex.txt
go test -bench=. -count=10 ./pkg/ds/lockfree/ > lockfree.txt
benchstat mutex.txt lockfree.txt
```

---

## Project Structure

```
go-p2p/
├── cmd/
│   └── node/
│       └── main.go              # Node entry point
│
├── pkg/
│   ├── node/
│   │   ├── node.go              # Core node logic
│   │   ├── broadcast.go         # Gossip broadcast (sync.Map)
│   │   ├── config.go            # Configuration
│   │   └── node_test.go         # P2P integration tests
│   │
│   ├── peer/
│   │   ├── peer.go              # Peer connection
│   │   └── manager.go           # Peer management (sync.Map)
│   │
│   ├── transport/
│   │   ├── tcp.go               # TCP transport
│   │   ├── connection.go        # Connection handling
│   │   └── buffer_pool.go       # sync.Pool buffers
│   │
│   ├── protocol/
│   │   ├── message.go           # Message types
│   │   ├── codec.go             # Encoding/decoding
│   │   └── handshake.go         # Handshake protocol
│   │
│   ├── discovery/
│   │   ├── seed.go              # Seed node discovery
│   │   └── gossip.go            # Gossip discovery
│   │
│   ├── mempool/
│   │   ├── mempool.go           # Transaction pool (Mutex+Heap)
│   │   ├── tx.go                # Transaction structure
│   │   └── mempool_test.go      # Mempool tests
│   │
│   └── ds/                      # Data structures
│       ├── lockfree/
│       │   ├── queue.go         # Lock-free MPSC queue
│       │   ├── hashmap.go       # Lock-free hashmap
│       │   └── priority_queue.go # Lock-free skip list
│       ├── sync/
│       │   ├── queue.go         # Mutex-based queue
│       │   ├── hashmap.go       # Mutex-based hashmap
│       │   └── priority_queue.go # Mutex-based heap
│       └── benchmark_test.go    # Comparison benchmarks
│
└── docs/
    └── benchmarks.md            # Detailed benchmark analysis
```

---

## Protocol

### Message Format

```
┌──────────────────────────────────────┐
│  Magic (4 bytes)  │  0x50325031      │
├───────────────────┼──────────────────┤
│  Type (1 byte)    │  Message type    │
├───────────────────┼──────────────────┤
│  Length (4 bytes) │  Payload length  │
├───────────────────┼──────────────────┤
│  Checksum (4 B)   │  CRC32           │
├───────────────────┴──────────────────┤
│  Payload (variable)                  │
└──────────────────────────────────────┘
```

### Message Types

| Type | Value | Description |
|------|-------|-------------|
| Handshake | 0x00 | Connection initialization |
| Ping | 0x01 | Keepalive check |
| Pong | 0x02 | Ping response |
| GetPeers | 0x10 | Request peer list |
| Peers | 0x11 | Peer list response |
| Tx | 0x20 | Transaction broadcast |
| Block | 0x21 | Block broadcast |

---

