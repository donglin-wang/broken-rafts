# `leader_id` Is Authoritative Only From a Current Leader

## The Bug

`become_follower_if_applicable` updates `self.leader` from whatever
message it's processing, with no filter on the message's type or
term:

```python
def become_follower_if_applicable(self, message):
    if "leader_id" in message["body"] and message["body"]["leader_id"] is not None:
        self.leader = message["body"]["leader_id"]
    ...
```

The function is called from every RPC handler — `handle_request_vote`,
`handle_append_entries`, `handle_append_entries_ok` (including
failure replies), `handle_request_vote_ok`. Three of those four
message types carry a `leader_id` field whose value is *not*
authoritative:

1. **`RequestVote`.** The candidate sending it does not know who the
   leader is. The field may be `None` or stale.
2. **`AppendEntriesOk` failure replies** (see `raft.py:183`). These
   echo the *follower's* notion of who the leader is. The follower
   may itself be out of date.
3. **Stale-term `AppendEntries`.** A delayed message from a dead
   older-term leader.

Only one message type carries an authoritative `leader_id`: a
**current-or-higher-term `AppendEntries`** received as a fresh
heartbeat or replication message. Everything else is hearsay.

The fix is to restrict the update to that one case: move it
out of `become_follower_if_applicable` and into the body of
`handle_append_entries`, after the term check.

## Why It Looks Reasonable

`become_follower_if_applicable` is the universal "incoming message
arrived" hook. It already does term comparison and step-down. It is
tempting to also hoist the `self.leader = body.leader_id` update
into it, on the theory that every message tells you *something*
about who the cluster thinks the leader is.

The trap is that "tells you something" is doing all the work in
that sentence. A `RequestVote` tells you that *someone is
challenging the current leader* — which is the opposite of telling
you who the leader is. A failure reply tells you that *the follower
disagrees with the leader's view of its log* — orthogonal to leader
identity. A stale-term AE tells you about a *past* leader.

Hoisting the update into the universal hook collapses these
distinctions and produces "I last heard from someone, that someone
named X as leader, so X is leader" — which is wrong in three of the
four cases that fire it.

## A Trace Where the Leader Loses Its Own Identity

Three-node cluster: A is leader at term 5. B and C are followers.

**T=0** — A appends entry 10, sends AE to B. AE has
`leader_id = A`.

**T=10ms** — B's log already diverges from A's at index 9. B
rejects: `AppendEntriesOk{term=5, success=false, leader_id=B...}`.
(B's failure reply echoes B's *own* belief about the leader — which
*is* A, but for trace clarity let's say B was very briefly demoted
to follower of C in a prior partition that just healed, so B's
`self.leader` is stale at C. The failure reply carries
`leader_id=C`.)

**T=15ms** — A processes the failure reply. The handler routes
through `become_follower_if_applicable(message)`:

- `incoming_term (5) > self.term (5)` is false. No step-down.
- But the early `leader_id` branch unconditionally fires:
  `self.leader = "C"`.

A — the *actual* leader — has just adopted C's identity as the
leader it forwards client traffic to. Any client `read` or `write`
that arrives at A while A is processing other handlers will be
forwarded to C, which is a follower and will likely reject or
re-forward.

The window is small (A's next AE-handler-or-self-check will
overwrite `self.leader` back to A's own id when A confirms its
state == LEADER), but it exists, and it amplifies any other source
of follower-leader confusion.

## A Worse Trace: Stale AE Pins the Wrong Leader

A stale-term AE from a dead leader runs through
`become_follower_if_applicable` and clobbers `self.leader` on the
follower. This composes with the related bug where
`handle_append_entries` updates state (deadline, leader-id) before
checking the term: the ordering bug ensures the function runs even
for stale messages, and *this* bug ensures the function does damage
when it runs.

Fixing one without the other still leaves a hole:

- Fix only the ordering: a current-term `RequestVote` that arrives
  legitimately still runs `become_follower_if_applicable`, and that
  RV carries no authoritative `leader_id`. The update fires anyway
  if the candidate ever populates the field (defensively or by
  accident).
- Fix only the `leader_id` source: a stale-term AE that updates
  `self.leader` still gets its update through, because the AE *is*
  the authoritative source — except in this case it's stale.

Both fixes are needed. They live in different functions and address
different aspects of the same underlying confusion: "any message
that names a leader is a statement about leadership."

## Why the Cluster Doesn't Always Catch You

In a steady-state cluster:

- `RequestVote` messages don't include `leader_id` (candidates don't
  fill it in). The branch is gated on `is not None`, so it doesn't
  fire on real RVs.
- Failure replies usually echo the same leader the requester already
  has — both nodes are in sync. The update is a no-op.
- Stale-term AEs require leader churn that hasn't happened.

The bug only fires when at least two of:

1. A reply path serializes a `leader_id` that the leader doesn't
   know to expect.
2. A node's `self.leader` is briefly stale during step-down.
3. Stale messages are in flight across leadership changes.

Maelstrom's partition nemesis produces (2) and (3) routinely, and
(1) is a static fact about this codebase — the failure reply path
in `raft.py:183` does echo `leader_id`.

## The Right Mental Model

`self.leader` is **the identity of the most recent current-term
leader I have heard from directly**. The only message that satisfies
"current-term leader heard from directly" is a `AppendEntries` whose
term equals or exceeds the receiver's term. Every other message is
indirect evidence at best.

Stated as code:

```python
def handle_append_entries(self, message):
    incoming_term = message["body"]["term"]
    if incoming_term < self.term:
        return reject
    self.become_follower_if_applicable(message)   # term, voted_for, state
    self.leader = message["body"]["leader_id"]    # only here
    self.election_deadline = (...)
    ...
```

`become_follower_if_applicable` is left to do its term/state/vote
work; the leader-identity update is hoisted up to the one handler
where it makes sense.

The cleaner version of this in many codebases lives in a separate
helper, e.g. `note_current_leader(leader_id)`, which is called only
from `handle_append_entries` after the term check passes. That makes
it syntactically impossible for the update to fire from RV or
failure-reply handlers.

## The General Lesson

In a protocol with multiple message types that all carry overlapping
fields (term, leader_id, last_index, etc.), each *field* needs an
explicit policy about which *messages* it is authoritative on. The
default of "if the field is present, use it" is wrong by
construction — a field's meaning depends on the message that
carries it.

The same pattern shows up in:

- **DNS responses.** The `Authority` section is authoritative only
  when the responding server is itself authoritative for the zone;
  recursive resolvers populate it informationally. Treating it as
  authoritative regardless is a classic cache-poisoning vector.
- **HTTP headers like `X-Forwarded-For`.** Authoritative only from
  trusted reverse proxies; spoofable from any client otherwise.
- **Gossip protocols.** Membership claims from a peer are
  authoritative for that peer's own state, not for transitively
  reported peers' states.

The mechanical rule: for every field that updates local state from
a remote message, write down the exact set of (message type, term
relation, sender state) tuples under which that field is
authoritative. Reject every other combination at the handler
boundary, not deep in a shared helper.
