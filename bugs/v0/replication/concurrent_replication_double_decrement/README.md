# Multiple Replication Triggers: One Mismatch, Many Decrements

## The Bug

`follower_next_indexes[src]` was drifting down past every legal value
— eventually negative — and `replicate_if_applicable` would crash
trying to slice a log from a non-existent index.

The handler that decrements it looks innocuous:

```python
if success:
    ...
else:
    self.follower_next_indexes[src] = follower_next_index - 1
```

That is the textbook §5.3 backoff: when AppendEntries is rejected at
some `prev_log_index`, retry one slot earlier. One mismatch should
produce exactly one decrement. The bug is that *one mismatch was
producing two or more decrements*, because the leader was sending two
or more AppendEntries describing the same `prev_log_index` and the
follower was rejecting all of them.

## Why It Looks Reasonable

The decrement-on-failure rule reads like an idempotent retry
strategy. If the follower rejects, slide back; eventually you find
the agreement point. It feels safe to call `replicate_if_applicable`
from anywhere — write handler, election handler, periodic tick — on
the assumption that the "next" snapshot just gets sent again.

The trap is that the snapshot isn't really a snapshot. It is
*derived from* `follower_next_indexes[src]`, and the handler that
mutates that map fires once per AppendEntriesOk, regardless of
whether two of those acks are responses to what is logically the
same probe.

## The Asynchrony You Forgot

Replication used to be triggered from three places:

1. The periodic background loop, every ~100ms.
2. Inline at the end of `handle_write`, so a freshly appended entry
   ships immediately.
3. Inline at the end of `become_leader`, to push a heartbeat as soon
   as a node takes office.

Two of these can fire within the same handful of milliseconds. A
client write arriving 5ms after the periodic tick gets both: the tick
already sent AppendEntries to F based on `next_index = k`, then
`handle_write` appends a new entry and sends AppendEntries to F
*again* — also based on `next_index = k`, because no ack has come
back yet to advance it.

F now has two AppendEntries in its inbox, both claiming the same
`prev_log_index = k - 1`. If F's log already agrees there (the happy
case), both succeed and the duplication is silently absorbed. If F's
log disagrees there — and after any leader change with even brief
divergence, it does — F rejects both, returning `success=false`
twice.

The leader processes each rejection independently:

- Ack 1 arrives → `next_index[F] = k - 1`.
- Ack 2 arrives → `next_index[F] = k - 2`.

One disagreement at index `k - 1` cost the leader two decrements. If
three triggers overlapped, three decrements. Run that loop a few
times during a partition with churn and `next_index[F]` is suddenly
–4, then `slice_from(-3)` and the indexing math goes sideways.

## A Trace That Goes Negative

Three-node cluster: leader L, follower F. F has log `[1..3]`. L
inherited `[1..5]` from a prior term and is at term 7. F's last entry
disagrees with L's at index 4.

**T=0** — Periodic tick. L sends F: `prev_log_index=5, prev_log_term=7,
entries=[]`. (Heartbeat-shaped probe; `next_index[F] = 6`.)

**T=2ms** — Client write arrives. L appends entry 6.
`handle_write` calls `replicate_if_applicable`. L sends F:
`prev_log_index=5, prev_log_term=7, entries=[6]`. Still based on
`next_index[F] = 6`, because no ack has come back yet.

**T=20ms** — F processes both. F's entry at index 5 is from term 3,
not term 7. F rejects both. Two `success=false` messages cross back
to L.

**T=25ms** — L processes ack 1: `next_index[F] = 5`.
**T=26ms** — L processes ack 2: `next_index[F] = 4`.

L "learned" that F disagrees at index 5 *twice*. It now believes the
agreement point is somewhere ≤ 3, but actually it is at index 3
exactly. The next probe goes out at `prev_log_index = 3` and
succeeds — but L overshot. Worse: the same overlap pattern fires
every replication cycle while the partition is in effect, and each
cycle shaves another slot off `next_index[F]` even though no new
information was learned.

After enough cycles, `next_index[F] < 1` and `record.slice_from`
returns a garbage prefix or raises.

## Why The Cluster Doesn't Always Catch You

In a healthy network with no leader churn, F always agrees with L at
every probe. Both duplicate AppendEntries succeed. The match_index
update path is idempotent (`max(..., current)`), so duplicates wash
out. The bug stays invisible until two conditions hold:

1. F genuinely disagrees with L somewhere — i.e., there's a real
   need to back off. This requires a recent leader change with a
   brief inconsistent tail.
2. Two replication triggers fire in the same ack-round-trip window.

Maelstrom's partition nemesis manufactures both: it forces leader
churn (inconsistent tails are common) and it stretches RTTs (the
overlap window grows). That's why this only surfaced under partition
testing.

## The Right Mental Model

`follower_next_indexes[src]` is not a piece of advisory state that
each handler can independently nudge. It is a *cursor* whose
correctness depends on a strict one-to-one correspondence between
"probe sent" and "ack processed." Decrement-on-failure assumes
exactly that correspondence. The moment two probes share a
`prev_log_index`, their acks are no longer independent observations
— they are duplicate reports of one underlying truth, and counting
both double-counts the evidence.

Two correct shapes:

1. **Serialize the trigger.** Replication is fired from one place
   only — the background loop — so at most one in-flight probe per
   follower per tick. Inline calls from `handle_write` and
   `become_leader` are removed; they were latency optimizations
   masquerading as correctness paths.
2. **Make the response self-describing.** Echo the probed
   `prev_log_index` in the rejection, and have the leader ignore
   rejections whose `prev_log_index` is no longer current (i.e., the
   leader has already retreated past it).

The codebase took the first path. It's simpler and is consistent
with the broader rule that ack handlers must reason about the
specific request that produced them, not about the leader's current
global state.

## The General Lesson

When the response handler for an RPC mutates state non-idempotently
based on the *count* of responses (decrement, increment, append),
there must be exactly one outstanding request whose ack triggers
that mutation. Fan-out on the request side — multiple call sites
issuing what is logically the same probe — silently fans out the
response side too, and any non-idempotent handler will overcount.

The same shape shows up in:

- TCP duplicate ACK handling: a single lost packet can elicit many
  duplicate ACKs from the receiver, but the sender treats them as
  one signal (fast retransmit fires once, not per-dupack).
- Cache stampedes: N concurrent misses for the same key fire N
  origin requests, and any "decrement TTL on miss" or "increment
  refresh counter" logic compounds.
- Optimistic locking retries: retrying a CAS in a tight loop from
  multiple call sites (sync handler + async refresher) can cause
  one logical conflict to look like several.

Whenever the body of a response handler is non-idempotent — anything
that isn't `max`, `min`, `set`, or `union` — verify that the
*requests* are also serialized. Idempotent handlers tolerate
duplicate sends. Non-idempotent handlers don't, and the bug shows up
only when the duplicates happen to disagree.
