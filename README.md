# Raft KV Store

Implementation of MIT 6.824 labs involving the Raft consensus algorithm and a distributed key-value store.

## What it does

Implemented a distributed key-value store where multiple servers stay consistent using Raft consensus.

## Features

Raft:

* Leader election
* Heartbeats
* Log replication
* Persistence
* Recovery after crashes

KV Store:

* Get / Put / Append operations
* Client request handling
* Replicated state machine
* Duplicate request detection

## Structure

```text
.
├── kvraft
│   ├── client.go
│   ├── common.go
│   └── server.go
├── raft
│   ├── raft.go
│   ├── persister.go
│   └── util.go
```

## Notes

Built while working through MIT 6.824 Distributed Systems.

Only implementation files are included here. Course infrastructure and provided testing framework have been omitted.

