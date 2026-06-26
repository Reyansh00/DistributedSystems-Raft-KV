# Distributed Key-Value Store

A fault-tolerant distributed key-value store built from scratch in Go, 
using the Raft algorithm to keep multiple servers consistent 
even when nodes crash or get partitioned from the network.

Built while working through MIT 6.5840 (Distributed Systems).

---

## What it does

Any client can send a Put/Get/Append request to any server in the cluster.
The cluster guarantees that:
- Every committed write is durable ie, a node can crash and restart without 
  losing data
- Every read reflects the latest committed write (linearizability)
- The cluster stays available as long as a majority of nodes are up

If the leader crashes mid-operation, the remaining nodes elect a new leader 
within ~300ms and continue serving requests. The old leader's uncommitted 
entries are safely discarded.

---

## How it's built

Implements the algorithm described in the 
[extended Raft paper](http://nil.csail.mit.edu/6.824/2020/papers/raft-extended.pdf) 
, closely following Figure 2.

**Raft (src/raft/raft.go)**

The consensus layer. Every write goes through Raft before it touches the 
KV store. Raft replicates the write to a majority of nodes, and only once 
a majority confirms it does the KV layer apply it.

Key pieces:
- Leader election with randomized timeouts to avoid split votes
- Log replication with fast conflict resolution (backs up by term, not 
  one entry at a time)
- Persistence: currentTerm, votedFor and log are written to disk before 
  any RPC response, so a crashed node restores correct state on restart

**KV Server (src/kvraft/server.go)**

Sits on top of Raft. When a request comes in, the server submits it to 
Raft and waits on a channel for it to be committed. Once Raft commits the 
entry, the server applies it to an in-memory map and responds to the client.

Duplicate detection: each client tags requests with a unique ID and sequence 
number. The server tracks the last applied sequence per client, so a retried 
request on a timeout never applies twice.

---

## Test results

All tests pass with the race detector enabled:

Lab 2 Raft:
- Initial election, re-election after failure
- Log replication under concurrent clients
- Leader backup over incorrect follower logs 
- Persistence across crashes and restarts
- Figure 8 (unreliable network + simultaneous failures)
- Churn (continuous crashes + unreliable network)

Lab 3A KV Store:
- Single and many concurrent clients
- Unreliable network, server restarts, partitions
- Linearizability checks

---

## Setup

This uses the MIT 6.824 lab infrastructure (labrpc, labgob). Clone the 
full 6.824 repo, drop these files in, and run:

    export GOPATH=~/6.824
    cd src/raft && go test
    cd src/kvraft && go test -run 3A

---

## What's missing

- Log compaction / snapshots — the log grows unboundedly right now
