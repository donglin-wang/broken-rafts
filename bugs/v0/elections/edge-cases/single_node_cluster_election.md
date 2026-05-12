# Single-Node Clusters Need an Election Shortcut

## The Bug

`trigger_election` sets up the candidate state and sends
`RequestVote` to every peer:

```python
def trigger_election(self):
    self.state = State.CANDIDATE
    self.term += 1
    self.voted_for = self.node_id
    self.votes = {self.node_id}
    self.request_vote()           # sends to every neighbor
```

`handle_request_vote_ok` is the *only* place that checks the
majority threshold:

```python
def handle_request_vote_ok(self, message):
    ...
    if message["body"]["vote_granted"]:
        self.votes.add(message["src"])
    if len(self.votes) >= majority(len(self.neighbors)):
        self.become_leader()
```

In a single-node cluster, `request_vote` sends no messages (no
peers). `handle_request_vote_ok` is never called. `self.votes`
contains the candidate's own self-vote — already a majority of one,
but no code path checks. The node sits in CANDIDATE forever.

The fix is to evaluate the majority threshold at the end of
`trigger_election`:

```python
if len(self.votes) >= majority(len(self.neighbors) + 1):
    self.become_leader()
```

(The `+ 1` is for the candidate itself, depending on whether the
codebase's `neighbors` includes self or not. The point is that the
threshold check has to fire from the self-vote site, not only the
reply-handler site.)

## Why It Looks Reasonable

The code reads as a clean split of responsibilities:
`trigger_election` casts the self-vote and broadcasts;
`handle_request_vote_ok` tallies replies and crowns. Each function
has one job. In any cluster with at least one peer, this works
correctly — the self-vote alone never clears the threshold (for
`n=2`, majority is 2, self-vote is 1; for `n=3`, majority is 2,
self-vote is 1), so the threshold check naturally lives at the
reply-handling site.

The trap is the `n=1` edge case. Majority of 1 is 1. The self-vote
*does* clear the threshold immediately. There is no reply ever
expected, so the only check site is unreachable, and the
threshold-crossing event has no place to fire.

It is the same shape as "off-by-one at the empty boundary": code
that operates on `n ≥ 2` and silently does nothing for `n = 1`.

## Why It Matters

A one-node "cluster" is a degenerate but legitimate configuration:

- **Single-node Maelstrom runs** (`--node-count 1`) — used for
  smoke-testing the state machine and the message plumbing without
  the complication of replication.
- **Bootstrap mode.** Some operational setups start a new cluster
  as a single node and grow it via configuration changes (§6 of the
  paper).
- **Test isolation.** A unit-test harness that wants to drive a
  single Raft node through a sequence of operations without
  spinning up peers.

In all three, the cluster wedges silently. No error, no log line
suggesting why — the node just never reports a leader, and every
client write returns `temporarily-unavailable: No leader elected`.
Debugging it from the outside is brutal because every diagnostic
points at "election in progress" — which is correct, just never
ending.

## The Right Mental Model

Election in Raft has two phases that *usually* happen at different
times: cast self-vote, then collect peer votes. The threshold check
is conceptually "after every new vote is added to the tally, see if
we've crossed it." In the common case, the self-vote alone doesn't
cross, so the check happens reply-by-reply.

The bug is treating the self-vote as a *separate stage* from peer
votes rather than as the first vote in the tally. From the
threshold's perspective, votes are votes — the candidate's own and
the peers' alike. The check should fire after the self-vote with
the same logic as after every peer vote.

A clean way to encode this is a single `_add_vote(voter)` helper:

```python
def _add_vote(self, voter):
    self.votes.add(voter)
    if len(self.votes) >= self.cluster_majority():
        self.become_leader()
```

Called from both `trigger_election` (with `voter = self.node_id`)
and `handle_request_vote_ok` (with `voter = message["src"]`).
There's then only one threshold check site, and it naturally
handles every cluster size including 1.

## Adjacent Subtleties

- **Cluster size source of truth.** `len(self.neighbors)` is
  peers-not-including-self in this codebase. `majority` therefore
  must take `len(self.neighbors) + 1` for the correct cluster
  size. If the existing reply-handler check passes only `len(self.
  neighbors)`, that's a *second* bug (under-counting cluster size)
  hiding behind this one. Worth verifying when fixing.
- **No peers, no replication either.** A single-node cluster's
  `replicate_if_applicable` also has nothing to do. The replication
  loop should be a no-op for an empty neighbor set; the commit-
  advance logic must derive committedness from the leader's own log
  alone. (Trivial, but worth confirming the median-of-empty case
  isn't a divide-by-zero.)

## The General Lesson

Code that handles the "common case" of `n ≥ 2` participants and
silently misbehaves for `n = 1` is a recurring pattern in
distributed-systems code. The diagnostic question to ask is
*"what's the smallest cluster this code handles correctly?"* If
the answer is `n ≥ 2`, the code is incomplete unless the broader
system enforces `n ≥ 2` somewhere.

The same shape shows up in:

- **Consensus protocols generally.** Paxos, ZAB, and others all
  have the same "self-vote counts" subtlety. Implementations that
  forget to count it have the same wedge.
- **Quorum reads/writes in distributed databases.** A
  single-replica deployment must answer reads from the local copy
  without expecting peer responses. Code that always waits for
  `quorum_size - 1` peer acks deadlocks at `quorum_size = 1`.
- **Leader election in coordination services.** A single ZooKeeper
  or etcd node must self-elect; implementations that hard-code "I
  need at least one peer to confirm" can't bootstrap.

The mechanical rule: every threshold check should fire from every
event that could change the count, *including the initial self-
event*. If the only check site is reachable only via remote
responses, the code is implicitly assuming remote participants
exist.
