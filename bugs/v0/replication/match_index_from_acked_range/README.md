# Match-Index Updates: What An Ack Actually Acknowledges

## The Bug

When an `AppendEntriesOk` arrives with `success=true`, the obvious-looking
update is:

```python
self.follower_match_indexes[src] = self.record.last_index()
self.follower_next_indexes[src] = self.record.next_index()
```

This is wrong. It treats the response as if it acknowledges *the leader's
current log*, when what it actually acknowledges is *the specific range
the leader sent in the request that produced this response*. Those two
values are equal only in the degenerate single-in-flight case.

## Why It Looks Reasonable

The Raft paper's pseudocode reads almost like RPC:

> If successful: update nextIndex and matchIndex for follower (§5.3)

If you mentally model this as a synchronous RPC — "I called
AppendEntries, it returned success, therefore it has my whole log now" —
then `match_index = last_index()` looks right. The follower successfully
applied the entries; the leader's `last_index()` is what those entries
ended at; QED.

The trap is that "the entries" in that sentence refers to the entries
that were in the request, not the entries in the leader's log *now*.
The leader is free to append new entries in between sending and
receiving.

## The Asynchrony You Forgot

Two things happen concurrently:

1. The replication loop fires every 100ms, sending AppendEntries.
2. Client write handlers append new entries to the leader's log
   whenever a write arrives.

Between any two ticks of the replication loop, an arbitrary number of
new entries can be appended. If a `RequestVote` reply lands during that
window, it is acknowledging a *snapshot* of the log that no longer
matches `record.last_index()`.

## A Trace That Goes Unsafe

Three-node cluster: leader L, followers F1, F2.

**T=0** — L's log is `[1,2,3,4,5]`. Replication loop fires; L sends
F1 an `AppendEntries` carrying entries 4..5 (assume next_index=4).

**T=10ms** — Client write arrives. L appends entry 6, log is now
`[1..6]`. `last_index() = 6`.

**T=15ms** — F1's ack for the T=0 AppendEntries arrives at L. F1
applied entries 4..5 successfully. F1's log is `[1..5]`.

**T=15ms** — Buggy update fires:

```python
self.follower_match_indexes["F1"] = self.record.last_index()   # = 6
```

L now believes F1 has entry 6. F1 does not have entry 6.

**T=20ms** — Suppose F2's match_index is also 6 (by the same bug, or
by genuine replication catching up). Median of `[6, 6]` is 6. L
commits index 6.

**T=25ms** — L crashes before sending entry 6 in any subsequent
AppendEntries.

**T=2s** — F1 and F2 hold an election. Their last log entry is
index 5, term whatever. The new leader's log does **not** contain
entry 6. But L told its client `write_ok` for entry 6 because L
committed it.

**Linearizability violated.** A client received `ok` for an operation
that no quorum ever stored.

In Maelstrom this manifests as the Knossos checker rejecting the
history with a "can't reach this state" message, often around CAS
operations whose preconditions depend on an entry that "committed" but
isn't really there.

## Why The Cluster Doesn't Always Catch You

The cluster *does* eventually correct itself most of the time:

- The next replication tick sends entry 6 to F1.
- F1 acks. The mistaken match_index of 6 happens to become true
  retroactively.
- The leader doesn't notice it was briefly wrong.

This is what makes the bug so insidious. The match_index "lies" for
~100ms-ish before truth catches up. In a healthy network you can run
millions of operations and never see it. The window only widens
under:

- A burst of writes between replication ticks (100ms is a long time at
  high write rate).
- A leader crash inside that window.
- A network partition that delays the catch-up replication.

Maelstrom's partition nemesis manufactures exactly those conditions,
which is why the bug is far more visible there than in steady-state.

## The Right Mental Model

An `AppendEntriesOk` carries no payload describing *what* it
acknowledges. The leader has to reconstruct that from the request that
elicited the response — and the leader is the one that sent the
request, so it knew the answer at send time.

There are two correct shapes:

1. **Stamp the request.** Include `prev_log_index` and `len(entries)`
   in the AppendEntries; on success, set
   `match_index = prev_log_index + len(entries)`.
2. **Echo it back.** The follower repeats `prev_log_index` and the
   number of entries it accepted in the response; the leader uses
   those.

Either way, the source of truth is the *range that was negotiated by
that specific request/response pair*, not a global property of the
leader's current state.

A subtler version: if you *do* echo back, you also have to ignore
acks that arrive out of order. An old, in-flight ack saying "I have
through index 5" must not overwrite a more recent ack of "I have
through index 8" — match_index is monotonic from the leader's
perspective. The cleanest way is to take the max:

```
match_index[src] = max(match_index[src], acked_through)
```

## The General Lesson

Asynchronous protocol responses are about the request that produced
them, not about the responder's or sender's *current* state. If your
code resolves a response by reading global state instead of inspecting
the request/response pair, you have introduced an implicit assumption
that nothing else moved between send and receive. That assumption is
false in any system worth building.

The same shape shows up in:

- HTTP retries replaying a stale request body against a moved-on server.
- Cache-invalidation messages whose payload describes "what was true
  when I sent this," not "what is true now."
- Database two-phase commit where the coordinator's notion of the
  transaction state must be derived from the prepare/commit messages
  themselves, not from current row state.

Whenever you write `state[remote] = local_state` in response to a
remote message, ask: *did the remote acknowledge `local_state`, or
did it acknowledge whatever local_state was at send time?*
