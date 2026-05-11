# Blocking Inside Message Handlers

## TL;DR

My first Raft implementation treated message handlers as synchronous RPCs:
a client `write` handler would append to the log, *block* waiting for a
majority of followers to ack, and only then return. That design is wrong —
not just slow, but correctness-hostile — and fixing it required a real
mental-model shift.

## What I Built First

`handle_write` held the node lock and called a `replicate_blocking`
helper, which sent `AppendEntries` to every peer and spin-waited on a
`pending_ok` dict until a majority had responded (or a 2-second timeout
expired). Only then did `handle_write` send `write_ok` back to the client.

Effectively:

```
handle_write(msg):
    with lock:
        append to log
        send AppendEntries to all peers
        wait up to 2s for majority acks   # <-- still holding lock
        send write_ok to client
```

## Why It's Wrong

1. **Lock starvation under load.** The handler holds the node lock while
   spin-waiting. Every other handler (vote requests, heartbeats, peer
   acks) queues behind it. At high write rates the cluster grinds to a
   halt — and ironically the acks the handler is waiting for can't be
   processed, because their handler also needs the lock.
2. **One write serializes the node.** Raft's whole point is pipelined
   replication. A synchronous handler turns the leader into a one-write-
   at-a-time box.
3. **It conflates two separate events.** "Handler returns" and "client
   gets `write_ok`" are not the same moment. The former is a local
   scheduling detail; the latter is a durability guarantee. Fusing them
   forces the handler to block.
4. **It doesn't generalize to the commit rule.** Real Raft replies to a
   client only when the entry is *committed* per the leader's commit
   rule (majority `matchIndex` ≥ N, entry is in current term). A
   synchronous handler can't express "reply when commit_index advances
   past N" — it only knows "reply when my specific RPCs came back."

## Case Study: Two Nodes Deadlock Each Other

The "ironically the acks can't be processed" claim under reason (1)
has a nastier two-node variant that shows up constantly under
concurrent load. It's a distinct mechanism from single-node
tight-loop lock starvation — this one is a genuine cycle between
separate machines.

### Setup

Leader n4, follower n1. Jepsen client `c16` writes directly to n4.
Client `c19` writes to n1. Both arrive at T=0. At `--rate 100` with
multiple clients per node this overlap is not an edge case; it is
the common case.

### Trace

**T=0+ε** — handler threads launch on both nodes:

- **n4** acquires `n4.lock`, appends c16's entry, sends
  `append_entries` with msg_ids `A0, A1, A2, A3` to n0–n3, enters
  `wait_for([A0..A3])` — *still holding `n4.lock`*.
- **n1** acquires `n1.lock`, appends c19's entry. It is not leader,
  so `persist_write` calls `send_blocking(n4, c19_body)` to forward
  the request, enters `wait_for([W2])` — *still holding `n1.lock`*.

**T≈10ms** — cross-traffic arrives:

- On **n1**: n4's `append_entries` (msg_id `A1`) arrives.
  `handle_append_entries` spawns a thread; its first line is
  `with self.lock:`, which blocks on `n1.lock`. `A1` cannot be
  acked until n1 releases that lock.
- On **n4**: n1's forwarded write (msg_id `W2`) arrives.
  `handle_write` spawns a thread; it blocks on `n4.lock`. `W2`
  cannot be acked until n4 releases that lock.
- n0, n2, n3 (idle) ack instantly; `A0, A2, A3` pop from
  `pending_ok`. But `A1` doesn't, so n4's `wait_for` keeps spinning.

### The Cycle

    n4.wait_for ── needs A1 ack ── needs n1.lock
        ▲                              │
        │                              ▼
     n4.lock ── needed by W2 ack ── n1.wait_for

Neither handler can exit its `wait_for` until the 2-second watchdog
fires, because each is waiting for a response that requires the
other's lock to be released.

### Consequences

- Every overlap of this shape costs a 2-second stall on both nodes.
- The 300ms `replication_loop` is also gated on `self.lock`, so it
  doesn't fire during the stall. Even if it did, its sends carry
  *fresh* msg_ids, not `A1` — popping those wouldn't unblock the
  caller waiting specifically on `A1`.
- n4's `wait_for` eventually returns 3 (plus a bogus +1 from the
  skip_node bonus when skip_node isn't a neighbor), so c16's write
  succeeds with 2s latency. n1's `wait_for` times out at 0, rolls
  back its log entry, and never replies to c19. Client timeout.

### The General Shape

Any handler that holds a local lock across a round trip can enter a
cycle with any other node whose ack-handler needs the same local
lock pattern. The synchronous design doesn't just starve one node —
it couples the liveness of every pair of nodes that concurrently
handle client traffic.

## The Mental Model Shift

Treat handlers as **event-driven state transitions**, not synchronous
RPCs. A handler's only job is:

- Update local state (append to log, update term, etc.)
- Emit outbound messages
- Return

The client response is *not* the handler's job. When a `write` arrives,
the leader:

1. Appends the entry at index `N`.
2. Stashes a pending-client record: `{index: N, client, msg_id}`.
3. Returns from the handler. Replication proceeds asynchronously via
   the existing replication loop / ack handler.

Separately, whenever `commit_index` advances, a resolver step walks the
newly-committed indices and, for each, sends `write_ok` to any stashed
client. Leadership loss cancels pending records with an explicit
`temporarily-unavailable` error, so the client retries against a
new leader rather than waiting on a node that no longer owns the
slot.

This also cleanly handles the "two overlapping writes" case: both land
in the log in order, both get `write_ok` when their index commits, and
the final value reflects the later write. Success means *durably
committed in order*, not *value persists*.

## Why This Is the Interview Answer

The mistake is a good story because it's a specific, concrete
manifestation of a general lesson: **distributed protocols are defined
by state-machine transitions over events, not by the call graph of the
code that implements them.** Mapping protocol steps onto synchronous
function calls felt natural coming from request/response web code, and
it's exactly what the Raft paper's pseudocode *looks* like on first
read — but the paper's "upon receiving…" blocks are event handlers,
not blocking calls.

## Fix Summary

- Handlers never block on network I/O or peer responses.
- Locks are held only for local state updates, released before any
  wait.
- Pending client requests are keyed by log index, resolved by the
  commit advancement step.
- The replication loop and ack handler are the sole drivers of
  `matchIndex` / `commit_index` advancement.
