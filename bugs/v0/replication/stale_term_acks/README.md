# Stale-Term Acks: Replies From A Previous Leadership Tenure

## The Bug

`handle_append_entries_ok` originally bailed out only on the
step-down or not-leader cases:

```python
if (
    self.become_follower_if_applicable(message)
    or self.state != State.LEADER
):
    return
```

Missing: a check that the **ack's term matches the leader's current
term**. Without it, a delayed `AppendEntriesOk` from a prior
leadership tenure (term N) is processed by the same node now leading
at term N+k as if it were a current-term acknowledgement.

The fix is one extra clause:

```python
if (
    self.become_follower_if_applicable(message)
    or self.state != State.LEADER
    or message["body"]["term"] != self.term
):
    return
```

Same guard you already apply (or should apply) at every other
incoming-RPC handler. §5.1 of the Raft paper states it as a universal
rule: *"if a server receives a request with a stale term number, it
rejects the request."* The same must hold for replies — a reply from
a stale term carries no information about your current term.

## Why It Looks Reasonable

The Raft paper's pseudocode for `AppendEntriesOk` doesn't restate
the term filter — it shows the success-path update in three lines.
If you transcribe just those three lines, the filter is missing.

The `become_follower_if_applicable` call also *looks* like it covers
term checking, and for `>` it does. But it deliberately doesn't fire
on `incoming_term < self.term` (you don't *step down* in response to
a stale message — you ignore it). So a stale-term ack passes through
that check untouched and into the success branch.

Short version: the step-down hook and the stale-message filter are
two different responsibilities, and conflating them leaves a gap on
the lower-term side.

## A Trace That Goes Unsafe

Three-node cluster: A, B, C. A is leader at term 5.

**T=0** — A sends `AppendEntries` to B carrying entries through
index 10 (term 5). B applies them. B's log: `[..t5×10]`. B sends
`AppendEntriesOk` with `match_index=10, term=5`. The reply gets
delayed in the network.

**T=50ms** — A is partitioned away from B and C. A's heartbeats
stop reaching the others. B and C run an election. C wins term 6.
C's log diverges from A's at, say, index 5 (C's log was shorter at
the divergence point but won §5.4.1 by some other path — exact
details aren't load-bearing).

**T=200ms** — C, as leader at term 6, sends `AppendEntries` to B
that overwrites B's log from index 5 onward. B's log is now
`[..t1..t3..t6×3]` — only 8 entries, none of them the term-5
entries B previously acked.

**T=500ms** — Partition heals. A learns of term 6 from a heartbeat,
steps down briefly, and after another election cycle wins term 7.
A's log at this point is whatever survived: say
`[..t1..t3..t5×4..t7×1]`.

**T=510ms** — B's original `AppendEntriesOk` from T=0 finally
arrives at A. Its body says `term=5, success=true, match_index=10`.

**Without the term filter:**

- `become_follower_if_applicable` does nothing (5 < 7).
- `self.state == LEADER`, so we proceed.
- Success branch fires. A sets `follower_match_indexes[B] = 10`.

But B does not have 10 entries. B has 8, and none of the entries
above index 5 match what A's log has at those positions. A has just
recorded a `match_index` that is **a fact about a snapshot of B's
state from two leadership tenures ago**, not about B's current log.

**T=515ms** — Median across A's view: `[A's last_index, 10 (B,
fake), C's match_index]` → easily 10 or higher. A advances
`commit_index` past entries that B *does not currently have* and
that no quorum currently stores.

A applies one of its term-5 entries and replies `:ok` to a client.
If A loses leadership again before re-replicating, that `:ok` is a
phantom commit. **Linearizability violated.**

## Why The Cluster Doesn't Always Catch You

This bug needs a specific timing sandwich:

1. Leader L sends an AE.
2. L loses leadership before the ack arrives.
3. A different leader takes over and rewrites the follower's log in
   the relevant range.
4. L regains leadership.
5. The original ack only now arrives at L.

In a steady-state cluster the ack arrives well within one tenure and
the term matches. The bug requires leader churn *during* a single
in-flight round-trip — which Maelstrom's partition nemesis produces
on purpose.

It also compounds badly with the bug where `match_index` is
derived from the leader's current log state instead of the request
that elicited the ack. If you've fixed that one with the
"echo back" approach so the follower's reply carries the precise
`prev_log_index` and accepted-count, the reply now contains an
authoritative-looking `match_index` value — and the stale-term
filter is what stops you from believing it.

## The Right Mental Model

Every RPC in Raft is contextual to a specific term. A request says
"in term N, do X"; a reply says "in term N, X happened (or didn't)."
A reply from term N has *nothing* to say about state in term N+k,
because:

- The follower may have rolled forward and rolled back log entries
  any number of times between the two terms.
- The leader's own log may have diverged from what it had when it
  sent the original request.
- Even the *meaning* of `match_index` is term-scoped: an index
  number is only interpretable in the context of a specific log
  history.

The mechanical rule is simple: at the top of every RPC handler
(request *or* reply), drop messages whose term doesn't match the
expected one.

- For requests: `incoming_term < self.term` → reply false (or
  ignore).
- For replies: `incoming_term != self.term` → ignore. (Note: not
  `<` — `>` is also stale, in the sense that you've stepped down,
  and `become_follower_if_applicable` should already have demoted
  you before the filter runs.)

## The General Lesson

Filtering by term and *handling step-down* are two different
responsibilities and want two different lines of code. It's tempting
to consolidate them in one helper because they both touch
`incoming_term vs self.term`, but they answer different questions:

- **Step-down** is about *me*: "do I now know I'm not the latest
  authority? If so, demote myself."
- **Stale filter** is about *the message*: "is this message
  describing a world I still inhabit? If not, drop it."

A higher incoming term answers yes to step-down. A lower incoming
term doesn't trigger step-down at all but should still drop the
message. A helper that only does step-down on `>` will silently let
stale `<`-term replies through.

The same rule shows up wherever there's a logical clock and
asynchronous replies:

- Lamport-clock RPCs that carry a sender clock alongside the body.
- Distributed leases where a holder's view of "I have the lease"
  must be validated against the message's lease epoch, not just the
  current epoch.
- Anywhere the protocol round number can advance between send and
  receive.

Whenever you find a handler that begins with "step down if needed,
then process," ask: *what about messages from a strictly-earlier
round?* Step-down doesn't fire on those, but they almost certainly
shouldn't be processed either.
