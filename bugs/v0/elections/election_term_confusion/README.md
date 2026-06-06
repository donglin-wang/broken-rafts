# Two Different "Terms" in `RequestVote`

## The Symptom

Run a partition test long enough and the cluster never elects a leader.
Not "elects a leader, loses it, re-elects" — never elects one at all.
Every operation returns `temporarily-unavailable: No leader elected`,
elections fire continuously, terms climb into the hundreds, and the
string `"Leader is now"` never appears in any node log.

Concretely, on a 60-second run with `--nemesis partition`, I saw 5152
fail-results against 5 ok-results. The handful of "successful" ops were
the ones that happened to land inside the very first ~500ms before any
node had granted a vote, after which the cluster wedged itself shut.

The bug is not in the partition handling. It is in `handle_request_vote`,
and it triggers even on a perfectly healthy network. Partitions just
make it visible faster by forcing repeated election rounds.

## The Two Terms

A Raft node carries two distinct quantities that both have "term" in
their name and are easy to conflate:

1. **`self.term`** — the node's *current* term. A monotonically
   increasing counter that tracks the latest election the node has
   observed or initiated.
2. **`self.record.last_term()`** — the term of the *last entry in this
   node's log*. Bounded above by `self.term` but usually less; equal
   only right after a leader of the current term has appended an entry
   that this node has accepted.

The `RequestVote` "up-to-date" check (§5.4.1) compares the candidate's
last log term against the **receiver's last log term** — quantity (2).
The check exists to prevent a candidate with a stale log from winning
an election. It has nothing to do with quantity (1).

The buggy code I wrote conflated them.

## What I Built

```python
def handle_request_vote(self, message: Message):
    ...
    if (
        self.voted_for is not None
        or incoming_term < self.term
        or last_log_term < self.term            # <-- wrong RHS
        or last_log_index < self.record.last_index()
    ):
        # deny
    else:
        vote_granted = True
        self.voted_for = ...
        self.term = incoming_term
```

Two bugs in one expression. Walk them in order.

## Bug 1: `last_log_term < self.term`

The third clause compares the candidate's last log term against the
receiver's *current* Raft term. That comparison is meaningless for the
up-to-date rule, and worse, it is biased: `self.term` is usually
*larger* than `self.record.last_term()`, so the check rejects more
candidates than §5.4.1 says it should.

In the empty-log case it is catastrophic. Trace:

**T=0** — fresh cluster, all nodes at term 0, empty logs.

**T=600ms** — `n1`'s election timer fires. `n1` becomes candidate at
term 1. Sends `RequestVote{term=1, last_log_index=0, last_log_term=0}`
to `n0`.

**T=601ms** — `n0` evaluates the deny conditions:
- `voted_for is None` ✓
- `incoming_term (1) < self.term (0)` → false ✓
- `last_log_term (0) < self.term (0)` → false ✓
- `last_log_index (0) < self.record.last_index() (0)` → false ✓

All clear. `n0` grants the vote. **Crucially, the grant sets
`self.term = 1`.** `n0`'s log is still empty: `last_term() == 0`.

**T=602ms** — `n1` collects votes from `n0` and itself, becomes leader
at term 1. So far so good.

**T=∞** — partition. `n1` is isolated. Election timer on `n0` and `n2`
expires. They start an election in term 2.

**T=2.6s** — `n2` (the candidate at term 2) sends
`RequestVote{term=2, last_log_index=0, last_log_term=0}` to `n0`.

**T=2.601s** — `n0` evaluates:
- `voted_for is None` ✓ (reset when stepping down... if we did that
   correctly, see Bug 4)
- `incoming_term (2) < self.term (1)` → false ✓
- `last_log_term (0) < self.term (1)` → **true → deny.**

`n0` refuses to vote despite having an empty log identical to the
candidate's. The candidate cannot win. The election cycles, term
climbs to 3, 4, 5... and `last_log_term < self.term` is true for
every one of them, forever, because the log never grows (no leader
to extend it).

This is a permanent liveness wedge. Once any node has granted a
single vote in any term ≥ 1, that node's empty log is now considered
"out of date" against every future empty-log candidate. With three
nodes that all behave the same way, no candidate can ever assemble
a majority. The cluster is dead.

The fix is the literal text of §5.4.1: compare against the receiver's
own last log term.

```python
or last_log_term < self.record.last_term()
```

## Bug 2: Index Check Applied Unconditionally

Even after fixing the RHS of the term comparison, the fourth clause
is still wrong:

```python
or last_log_index < self.record.last_index()
```

This is *always* checked. But the up-to-date rule's index comparison
only kicks in when the terms are equal:

> If the logs have last entries with different terms, then the log
> with the later term is more up-to-date. If the logs end with the
> same term, then whichever log is longer is more up-to-date.

Counterexample: receiver has log `[A:term=2, B:term=2]` (last_index=2,
last_term=2). Candidate has log `[X:term=5]` (last_index=1,
last_term=5). Candidate's log is more up-to-date — its last term is
strictly higher, length is irrelevant. But the buggy check rejects:
`last_log_index (1) < self.record.last_index() (2) → true → deny`.

This case is harder to hit in a stress test (it needs prior log
divergence) but it's a real safety drift: it can prevent a
legitimately-up-to-date candidate from winning when a stale-but-longer
one might. Compose it with Bug 1 and you get false rejections from
both directions.

Correct form, expressed positively:

```python
candidate_up_to_date = (
    last_log_term > self.record.last_term()
    or (
        last_log_term == self.record.last_term()
        and last_log_index >= self.record.last_index()
    )
)
```

Deny if any of `voted_for is not None`, `incoming_term < self.term`,
or `not candidate_up_to_date`.

## Bug 3: `become_follower_if_applicable` Forgets to Set `self.term`

Adjacent to the voting bugs, but worth its own diagnosis:

```python
def become_follower_if_applicable(self, message: Message) -> bool:
    incoming_term = message["body"]["term"]
    ...
    if incoming_term <= self.term:
        return False

    self.log(f"Became follower {self.node_id}")
    self.state = State.FOLLOWER
    self.voted_for = None
    return True
```

We notice `incoming_term > self.term`, so we step down and clear
`voted_for` — but we never update `self.term` itself. After the
function returns, the node is a follower nominally, but it still
believes it is at the old term.

That has cascading effects. The next outgoing message — an
`AppendEntriesOk`, a `RequestVoteOk`, an `AppendEntries` if we somehow
think we're leader — will carry the *stale* term. Other nodes that
receive it will compare against their own term and ignore it (or
worse, follow us back down on no information). The term-advancement
machinery of Raft hinges on every node monotonically tracking the
highest term it has *seen anywhere*, not just the highest term it has
*participated in*. Skipping that update breaks the contract.

The combination of Bug 1 and Bug 3 is what makes the failure mode so
total. Bug 1 wedges elections at any term ≥ 1; Bug 3 lets terms drift
between nodes so they don't even agree on which election they are
currently failing. Fix one and the other still wedges; fix both and
the cluster recovers.

## Bug 4: Stale Votes from Earlier Terms Count Toward Quorum

```python
def handle_request_vote_ok(self, message: Message):
    ...
    if message["body"]["vote_granted"]:
        self.votes.add(message["src"])
    if len(self.votes) >= majority(len(self.neighbors)):
        self.become_leader()
```

Two issues:

1. The reply has a `term`. We don't check that it matches our current
   term. A vote granted to us in term 5 — that arrived after we have
   already moved on to term 7 — counts toward a majority in term 7.
2. `self.votes` is never cleared on term change. Votes earned across
   different terms accumulate into a single set.

Symptom: a candidate that loses the term-5 election but collected one
out-of-three vote, then loses term 6 with one vote, can find itself
"winning" term 7 with zero new votes — the leftover vote from term 5
plus its self-vote in term 7 happen to clear the majority threshold.

This is a safety bug, not just liveness: it can elect a "leader" that
no quorum of the *current* term ever endorsed.

Two-line fix:

- Clear `self.votes` whenever the term advances (in `trigger_election`
  and in `become_follower_if_applicable`, once the latter is fixed to
  actually advance the term).
- In `handle_request_vote_ok`, ignore replies where
  `body.term != self.term`.

## Why It Sometimes Worked

The user-facing question was "I had two successful runs of the same
test." With Bug 1 in place, a successful run requires that an election
complete in term 1 *before* any other node has independently advanced
its term. The window is roughly the first 500–2000ms after startup,
gated by the randomized election timeout.

If you're lucky:

- One node times out first, requests votes at term 1.
- Other nodes, still at term 0, all match the deny conditions vacuously
  (`last_log_term=0`, `self.term=0`, so `0 < 0` is false).
- Election succeeds, leader appends entries, log grows, `last_term`
  catches up to `self.term`, and the up-to-date check accidentally
  works going forward.

If you're not:

- Two nodes time out close together, each grants its own vote and
  bumps `self.term` to 1.
- Now any subsequent election in any term forever satisfies
  `last_log_term (0) < self.term (≥1)` and gets rejected.

Partition nemeses turn the good draw into the unlucky draw by forcing
re-elections faster than the log can grow. That's why the test passed
on a quiet network and failed under partition — not because partition
introduced a bug, but because partition *exposed* the latent bug by
demanding re-elections from an empty-log state.

## The Mental Model

Raft assigns terms to two different things, and the protocol's
correctness depends on keeping them straight:

- **A node's current term** is a logical clock. It says "this is the
  most recent election round I know about." Used for: rejecting
  stale messages, breaking ties between candidates, advancing
  followers when a higher term appears.
- **A log entry's term** is a leadership fingerprint. It says "this
  entry was placed here by the leader of that term." Used for: the
  Log Matching Property, the up-to-date check, the no-op rule
  (§5.4.2).

Anywhere a Raft predicate names "term," figure out which of the two
it means before you write the code. The §5.4.1 rule is unambiguously
about the *log's* last term — both sides of the comparison. Pulling
`self.term` into one side of that comparison is the protocol-level
equivalent of comparing a wall-clock timestamp to a sequence number
because they both happen to be integers.

The phrase to internalize: **a node's current term and its log's last
term are not the same number, and the gap between them is where most
subtle election bugs live.**
