# Stale Vote Acks: Replies From An Earlier Election

## The Bug

`handle_request_vote_ok` originally counted votes regardless of which
term they were cast in:

```python
def handle_request_vote_ok(self, message: Message):
    with self.lock:
        if self.become_follower_if_applicable(message):
            return
        if message["body"]["vote_granted"]:
            self.votes.add(message["src"])
        if len(self.votes) >= majority(len(self.neighbors)):
            self.become_leader()
```

Two missing rules, both anchored in §5.1:

1. The reply's `term` is never compared to `self.term`. A grant cast for
   term 1, delayed in the network, still gets counted after the
   candidate has advanced to term 2.
2. `self.votes` is never cleared when the term advances. Votes from any
   number of prior terms accumulate into one bag.

The two interact: the missing term filter says "stale-term grants are
welcome," and the never-cleared set says "by the way, here are some
that have been sitting around since last election."

The fix is three changes:

```python
# in handle_request_vote_ok:
if self.state != State.CANDIDATE or message["body"]["term"] != self.term:
    return

# in trigger_election:
self.votes = {self.node_id}     # was self.votes.add(self.node_id)

# in become_follower_if_applicable, on the term-advance branch:
self.votes.clear()
```

This is the symmetric form of the same §5.1 term-filter applied to
`AppendEntriesOk` replies. The two together close the gap on all
RPC replies: every reply must be filtered by term before its body
is acted on.

## Why It Looks Reasonable

`set()` is exactly the right shape for "collect votes, deduplicate,
check threshold." Adding the same node twice doesn't double-count; the
threshold check is a single `len() >= majority`. A Raft author thinking
*term-locally* writes exactly this code.

The mismatch is that `set` semantics are timeless, while votes have an
expiration date. The expiration is the term boundary. A vote granted
for term N is meaningless evidence about term N+1 — not because the
voter changed its mind, but because the *thing being voted on* is a
different event.

The omitted term filter has a parallel pull. Figure 2's
"Rules for Servers / Candidates" entry for becoming leader is one line:
*if votes received from majority of servers: become leader*. The
"drop stale-term replies" rule lives at the top of the same figure as
a universal property — it is not restated at the candidate-rules level.
If you transcribe just the candidate line, you've correctly implemented
the trigger but not the gate.

Adjacent contributing factor: `become_follower_if_applicable` already
clears `voted_for` on term advance. It feels like we've handled
"election-cycle reset," and it's easy to overlook that `self.votes`
lives on the *other* side — the candidate counting incoming grants vs.
the voter remembering its outgoing grant. The two fields rhyme but are
different state.

## The Asynchrony You Forgot

A `RequestVoteOk` reply has no protocol-level deadline. It travels
through whatever queue Maelstrom is modeling — possibly behind a
partition — and arrives whenever it arrives.

Between sending `RequestVote` and receiving `RequestVoteOk`, a
candidate can:

- time out and start a fresh election (`term += 1`, `voted_for = self`),
- receive an `AppendEntries` from the term's winner and step down,
- receive a higher-term message and step down to follower at that term,
- start *another* election after the step-down,

any number of times. The reply was a true statement about the term it
references at the moment it was sent; it keeps moving the whole time.
By the time it arrives, the term it speaks for may be 1, 5, or 50 behind.

## A Trace That Goes Unsafe

Five-node cluster: A, B, C, D, E. All start as followers at term 0.

**T=0** — A's election timer fires. `trigger_election`: term 0→1,
`voted_for=A`, `votes.add(A)`. `votes = {A}`. Sends
`RequestVote{term=1}` to all four peers.

**T=10ms** — B receives, grants. Reply delivers normally.
`votes = {A, B}`. `len = 2 < majority(5) = 3`. Below threshold.

**T=15ms** — C grants. **Reply delayed in the network.**

**T=20ms** — D and E's replies are also delayed (or lost). A holds at
two votes.

**T=600ms** — A's election deadline expires (still no third grant).
`trigger_election` runs:

```python
self.term += 1                  # 1 → 2
self.voted_for = self.node_id
self.votes.add(self.node_id)    # set was {A, B}, .add(A) is a no-op
```

`self.votes` is **`{A, B}`** — B's term-1 grant is still in the bag. A
sends `RequestVote{term=2}` to all peers. For trace simplicity assume
none has replied yet.

**T=700ms** — C's delayed term-1 reply finally reaches A.
`handle_request_vote_ok`:

- `become_follower_if_applicable`: incoming_term=1, self.term=2. 1>2
  false. Returns False; no step-down.
- (No term filter.) `vote_granted=true`. `votes.add(C)`.
  `votes = {A, B, C}`.
- `len = 3 ≥ majority(5) = 3`. **`become_leader`** at term 2.

A is "leader at term 2" on a quorum entirely constructed from term-1
state: its own self-vote bumped to term 2, B's grant from term 1 left
over in the set, C's grant from term 1 just delivered. Not one peer
cast a term-2 vote for A.

If, in the same window, the B-C-D-E side elects a different term-2
leader (the delayed-replies model permits this — they may have received
a competing candidate's term-2 RVs before A's belated `become_leader`
fires), we have **same-term split brain.** From there, both sides
can independently commit divergent entries on disjoint sub-quorums,
and no in-protocol recovery puts the diverged history back together.

## Why The Cluster Doesn't Always Catch You

Both halves of the bug need leader churn to express:

- The **term filter** absence only matters when a vote-OK is in flight
  across a term boundary. In a healthy network, replies arrive within
  one election timeout (≤2s in this codebase), and the term hasn't
  advanced yet.
- The **never-cleared `self.votes`** only matters across an election
  cycle that didn't end with `become_leader` — which already resets
  the set. In a healthy network every election succeeds quickly and
  the set is reset on the way out.

Maelstrom's partition nemesis stretches both windows simultaneously.
It delays vote-OK replies (so they cross term boundaries on arrival)
and forces unsuccessful elections (so the set never gets reset by a
`become_leader`). The bug is latent on a quiet network and reliably
visible under partition.

## The Right Mental Model

A vote is a statement scoped to a specific term: "in election round N,
voter X granted candidate Y its single term-N vote." Two consequences
follow, and they are the two halves of the fix:

- **From the candidate's side:** an incoming reply is contextual to
  the term in its body. Drop it if the term doesn't match. Accept it
  only if you are still the entity it's a statement about — meaning
  still candidate, in the same term.
- **From the cluster's side:** the tally `self.votes` is *the set of
  votes I have collected in my current term*. The moment my term
  changes — by self-promotion or step-down — the prior tally has
  nothing to do with the new term's question. Reset.

The two failures `voted_for` (the *outgoing* vote) and `votes` (the
*incoming* tally) need parallel discipline:

| Side | Field | Resets on |
|---|---|---|
| voter (outgoing) | `voted_for` | term advance |
| candidate (incoming) | `votes` | term advance |
| candidate (incoming) | `votes` (additionally) | becoming leader (already correct) |

`voted_for` is reset in the existing code; `votes` was the missing peer.

## The General Lesson

**Any state scoped to a logical-clock epoch must be cleared when the
epoch advances**, and **any reply tagged with an epoch must be
filtered by that epoch on receipt**. Each rule alone is insufficient
if the other is missing.

The pattern shows up wherever a protocol carries an epoch alongside
asynchronous replies:

- **Distributed leases.** A holder's "I have N acks for this lease
  generation" set must be cleared when the generation advances. Acks
  for a prior generation must be filtered out.
- **View-stamped replication, Paxos.** A proposer's "I have N
  promises for this proposal number" tally is the same shape as
  Raft's `votes`. Same two rules.
- **Round-based BFT consensus.** Every round-based message carries a
  round number, and every receiver tally is round-scoped. Same two
  rules.
- **Generic request/response with retries.** When you re-send a
  request with a new request_id, late replies for the old request_id
  must not be matched against the new one's pending state. The
  request_id is the epoch.

If you find yourself thinking "well, sets accumulate monotonically, so
this can't go wrong," ask whether the set's elements have an
expiration date. If they do, the set type isn't quite right — you
want a `set` *plus* a reset hook on the boundary.
