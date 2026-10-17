# raft-kv

[#raft-kv](#raft-kv)

A from-scratch implementation of the **Raft consensus algorithm** in Go, with a small replicated key-value store built on top. This project exists to get hands-on with distributed consensus, leader election, log replication, and fault tolerance — it's a learning/portfolio project, not a production-grade distributed database. See **Known Limitations** below for an honest breakdown of what it does and doesn't do yet.

> Status: 🚧 work in progress. This README is being filled in as the project develops (leader election → log replication → fault tolerance → persistence → KV layer).

---

## What it does (so far)

[#what-it-does-so-far](#what-it-does-so-far)

_To be filled in as each block lands._

---

## Tech Stack

[#tech-stack](#tech-stack)

- **Core language:** Go
- **Inter-node communication:** TBD (gRPC or HTTP+JSON)
- **Testing:** Go's built-in `testing` package, plus scripted fault-injection tests (process kills, simulated network partitions)

---

## Repository Layout

[#repository-layout](#repository-layout)

```
├── node/      # Raft node state machine: Follower/Candidate/Leader, elections, log replication
├── rpc/       # RPC definitions and transport (RequestVote, AppendEntries)
├── cmd/       # Entry points: server binary, CLI client
├── test/      # Fault-injection and integration tests
├── go.mod
└── README.md
```

---

## Getting Started

[#getting-started](#getting-started)

_To be filled in once the first runnable node exists (Bloque 1)._

---

## Known Limitations / Roadmap

[#known-limitations--roadmap](#known-limitations--roadmap)

Documented deliberately, since knowing the edges of a system matters as much as building it. This section will be kept current as the project develops. Known gaps planned from the outset:

- **No log compaction / snapshotting.** The log grows unboundedly; a long-running cluster would eventually exhaust memory/disk.
- **No dynamic cluster membership changes.** The set of nodes is fixed at startup — no adding/removing nodes from a live cluster.
- **Single datacenter / local-network assumption.** No handling of realistic WAN latency or Byzantine failures — only crash faults and network partitions.

---

## Author

[#author](#author)

**Pablo Jorda** — Software Engineering Student (Computing and AI)
GitHub: [@pablojordab](https://github.com/pablojordab)
