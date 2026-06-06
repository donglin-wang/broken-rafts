# Reject First, Update State Second

## The Bug

`handle_append_entries` does work in the wrong order:

```python
def handle_append_entries(self, message):
    incoming_term = message["body"]["term"]
    self.election_deadline = (...)                 # line 163
    self.become_follower_if_applicable(message)    # line 166
    if incoming_term < self.term:
        return reject                              # line 171
    ...
```

The election deadline is bumped and `become_follower_if_applicable`
is run **before** the stale-term check. A delayed AppendEntries from
a dead older-term leader still:

1. Pushes the receiver's election deadline out (compounding any
   `+=` drift in deadline arithmetic).
2. Runs through `become_follower_if_applicable`, which inspects the
   message's `leader_id` field and overwrites `self.leader` —
   pointing the node at a leader the cluster has already moved past.

Only then does the term check fire and reject the message. By that
point the side effects have already happened.

The fix is the canonical RPC handler shape: reject by term first,
*then* mutate state.

```python
def handle_append_entries(self, message):
    incoming_term = message["body"]["term"]
    if incoming_term < self.term:
        return reject
    self.become_follower_if_applicable(message)
    self.election_deadline = (...)
    ...
```

## Why It Looks Reasonable

"Reset the election deadline when you receive an AppendEntries" is
one of the standard election-timer reset obligations, and
`become_follower_if_applicable` is the standard step-down hook that
fires on any incoming message. Putting both at the top of
`handle_append_entries` reads as "do the universal bookkeeping
before getting into the message-specific logic."

The trap is that "universal bookkeeping" is universal only with
respect to *valid* messages. A stale-term AE is not evidence that
the cluster has a live leader, is not evidence that any election
should be postponed, is not evidence about who the current leader
is. It is evidence that some node has fallen behind. The right
response is to ignore it; the wrong response is to update local
state on the assumption that the message is current.

§5.1 of the Raft paper states the rule universally: *"if a server
receives a request with a stale term number, it rejects the
request."* Every other side effect in the handler is gated on the
message being non-stale.

## A Trace That Goes Unsafe

Five-node cluster: healthy leader L at term 5. Long-isolated node X
that thinks it is leader at term 2. Followers F1, F2, F3.

**T=0** — Partition heals. X reconnects.

**T=10ms** — X's stale replication loop fires. X sends
`AppendEntries{term=2, leader_id=X, ...}` to F1.

**T=11ms** — F1 (at term 5, follower of L) processes the message:

- `self.election_deadline = datetime.now() + timeout`. F1's deadline
  was about to fire — it's now pushed 750ms further out.
- `become_follower_if_applicable(msg)`: `incoming_term (2) > self.term
  (5)` is false, so no step-down. But the early `leader_id` branch
  fires: `if "leader_id" in body and body["leader_id"] is not None`
  → `self.leader = X`. F1 now believes X is the leader.
- `incoming_term < self.term` → reject. Reply false.

F1 has just adopted the dead leader's identity as its current leader,
*and* postponed its election deadline. From F1's perspective:

- A client write arriving at F1 will be forwarded to X — a dead-end.
- L's next heartbeat will eventually correct `self.leader` back to L,
  but the window between T=11ms and L's next tick (up to ~100ms here)
  is a black-hole for any traffic F1 forwards.
- F1's election timer, were L actually dead, would now fire later
  than it should.

The compounding case is even worse: a partition that has *not*
healed leaves X periodically retrying. Each retry resets F1's
deadline and re-pins `self.leader = X`. F1 will never time out and
never recognize that L is gone, because the stale AE from X is
itself acting as a (false) liveness signal.

## Why the Cluster Doesn't Always Catch You

In a healthy network, no stale-term AppendEntries are in flight.
Every AE that reaches a follower is from the current term's leader.
The buggy order produces identical behavior to the correct order in
that regime.

The bug needs a node that thinks it's leader at a stale term and is
willing to send. Two ways that happens:

1. **Partition healing.** A node isolated long enough that the
   cluster advanced terms without it. When the partition heals, the
   stale leader's replication loop pushes a flurry of AEs before its
   own state catches up.
2. **Slow step-down propagation.** A leader has been demoted (via
   `become_follower_if_applicable` on a higher-term RV), but its
   in-flight AEs from before the demotion are still landing on
   followers.

Both are textbook partition-nemesis scenarios. The bug is invisible
on quiet networks.

## The Right Mental Model

An RPC handler has two phases, and they have to stay in order:

1. **Validate.** Decide whether the message is one this node should
   act on at all. Term checks live here. So does any sanity check
   about source, message type, or expected state.
2. **Process.** Update local state, emit replies, run side effects.

Mixing the two — running side effects before validation completes —
is the same shape as a web handler that writes to the database
before checking the auth token. It "works" in the happy path
because no malicious or stale requests arrive, and it fails
silently the moment one does.

The election-deadline reset and the leader-id update are *side
effects* of accepting an AppendEntries. Both belong after the term
check, not before.

A subtler corollary: `become_follower_if_applicable` is its own
small handler, and it has the same two-phase structure internally —
its term comparison decides whether to step down. But the
side-effect branch inside it that updates `self.leader` from
`message.body.leader_id` is *not* gated by that term comparison —
it trusts the leader-id field from any message type, including
`RequestVote` (no real leader-id) and failure replies (which echo
the *follower's* belief, not the leader's). So even if you fix the
ordering in `handle_append_entries`, the inner side-effect path can
still mis-fire from other call sites that don't gate it.

The cleanest fix to the ordering bug above is the conservative one:
both side effects move below the term check. The leader-id trust
issue is a separate fix in a separate place.

## The General Lesson

Any handler whose body contains *both* "decide this message is
valid" and "update state based on this message" must execute those
in that order, regardless of how tempting it is to put
"bookkeeping" first. The bookkeeping is part of accepting the
message.

The same pattern shows up in:

- **HTTP servers** that log request bodies before authenticating —
  attacker-supplied data lands in logs whether or not the request
  was legitimate.
- **State-machine replication** generally: any "I heard from peer X"
  signal must be predicated on the message being a current-epoch
  message, not just on it having arrived.
- **Distributed lock services** where a heartbeat from a client
  whose lease has expired is sometimes mistakenly used to renew the
  lease.

The mechanical rule: if a handler has an early-return reject path,
put it as the *literal first* logic after parsing. Everything else
is a side effect predicated on the reject not firing.
