# Distributed Wait Cycles

## The Pattern

Two machines each hold a local lock while waiting for a response from
the other. The response would be produced by a handler on the remote
side, but that handler needs the remote's local lock, which is held
by another handler that is *also* waiting for a response.

Classical deadlock needs two processes contending for the same two
resources. A distributed wait cycle looks the same from the outside
but the resources are per-node — each lock lives on its own machine,
and the cycle closes through message-passing rather than through
shared memory.

## The Canonical Raft Instance

Two nodes concurrently handling client writes, one direct and one
forwarded. Each holds its own node lock inside a blocking
`wait_for` call; each is waiting for an ack the other node can't
produce without releasing its lock. Neither timeout is about a
crash — both nodes are alive and responsive, just locked out of
each other.

## Why It's Worse Than Local Deadlock

A local deadlock in a well-written program is deterministically
reproducible and usually caught in review (lock ordering, etc.).
A distributed wait cycle has three extra properties that make it
harder to diagnose:

1. **No shared resource is contested.** Static analysis of either
   node's code finds no deadlock — the lock ordering within each
   node is perfectly consistent. The cycle only exists when the two
   nodes' runtime states align through the network.
2. **It's load-dependent.** At low rates the overlap never happens.
   The cycle only forms when independent requests on two nodes
   happen to be in their wait windows at the same time. The same
   code is "correct" under unit tests and "broken" under real load.
3. **The timeout hides the bug.** Because every cycle is broken by a
   watchdog, the system appears to keep working — just with absurd
   tail latency. Tests without tight latency SLAs pass.

## Other Flavors

### Synchronous Replication to All Followers

A leader that waits for *every* follower to ack before replying is
structurally a wait cycle with N-1 nodes: one slow node holds up all
clients, and those clients' retries can in turn block that node's
handlers. Formally a one-way dependency, operationally a cycle once
retries and backlogs get involved.

### Two-Phase Commit with a Silent Coordinator

Participants hold row-level locks after PREPARE, waiting for the
coordinator's COMMIT/ABORT. The coordinator crashes. Every
transaction that touched those rows blocks until the participant
decides to presume-abort. Not a symmetric cycle, but the same
shape: local state is pinned by the expectation of a specific
remote message.

### Distributed Lock with Client Pause

Client A holds a lock acquired from Zookeeper and pauses (GC, cgroup
throttle). Its lease expires. Client B acquires the lock and starts
writing. Client A resumes, thinks it still holds the lock, writes
too. Not a wait cycle per se, but a related failure mode: the
remote lock service can't tell pause from death, and the local
client can't tell "lease live" from "lease expired without my
noticing."

## How to Recognize It in Logs

- A timeout fires at its configured maximum, not at a shorter
  duration. Repeatedly.
- Multiple nodes time out in synchronized bursts.
- Tail latency of a particular operation equals the timeout exactly.
- Throughput collapses non-linearly with request rate — small
  increases in load produce disproportionate latency spikes as the
  overlap probability crosses a threshold.

Jitter distinguishes a partition (variable fire time) from a wait
cycle (consistent fire at exactly `T_timeout`).

## The Design Rule

**Never hold a local lock across a remote round trip.** A handler
may send a message and forget about it; a handler must not wait for
a specific response while pinning other handlers on the same node.
The handler for the ack must be able to run without contending for
the state the waiting handler owns.

In practice:

- Split state into "local mutation, guarded by lock" and "pending
  remote acks, lock-free or separately locked."
- Restructure the protocol so no handler ever waits for a reply.
  Replication becomes a background loop; client response is driven
  by commit advancement, not by RPC completion.
