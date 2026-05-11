# Split Brain: Two Leaders, Same Term

## The Invariant

> Two nodes cannot simultaneously be leaders of the same term, each
> committing entries to a sub-quorum.

This is the load-bearing safety property. Every other Raft theorem —
Log Matching, Leader Completeness, State Machine Safety — assumes it
as a precondition. When it breaks, none of the downstream guarantees
hold, and the failure surfaces as Knossos counterexamples sometimes
seconds or minutes after the actual divergence.

## What "Split Brain" Means In Raft Specifically

Loosely: any time multiple nodes simultaneously claim leadership.
Strictly, two flavors with very different blast radius:

1. **Same-term split brain.** Two nodes both believe they are leader
   at term T, and each has reachability to a sub-quorum willing to
   accept its `AppendEntries`. Each can independently commit. The
   state machines diverge and there is no in-protocol recovery —
   future leaders silently overwrite "committed" history. **This is
   the disaster case.** It is what produced the current run's
   linearizability failures.

2. **Stale-leader split brain.** A deposed leader doesn't yet know
   it lost leadership. As long as the term-comparison rules hold,
   its `AppendEntries` get rejected by anyone who has seen the
   newer term, so it cannot commit. It will reply incorrectly to a
   handful of clients before it learns the truth, but it cannot
   *diverge committed state*. Bad, but recoverable.

The whole "at-most-one-leader-per-term" machinery exists to make
flavor 1 impossible. The partition itself is not the bug — Raft is
designed to tolerate partitions. Split brain only happens when a
partition coincides with a logic error that lets a second node claim
the same term without a fresh majority.

## Recognizing It In Logs

Ordered from most to least definitive:

1. **Two `Leader is now <self>` lines at the same term across
   different node logs.** Smoking gun. Grep for it across all
   `node-logs/*.log` first.
2. **One node logging `Voted for X` and `Voted for Y` at the same
   term.** Direct evidence that the at-most-one-vote-per-term rule
   was violated, which mathematically permits two leaders to win.
3. **`match_index` for a single follower flipping between values
   reported to two different leaders** (visible by grepping
   `Append entries OK` on a follower's log and watching the
   `match_index` field as different leaders' AEs interleave).
4. **`apply_entries` rewriting indices that an earlier `Committing
   up to and including index N` line said were committed** — a
   follower being whiplashed between two divergent logs.
5. **Knossos `final-paths` that don't bottom out in a pending op**
   — the model state contradicts the most recent OK reply with no
   in-flight op able to bridge the two. Walking backward from such
   a contradiction usually lands on split-brain.

## Why It's Catastrophic

The argument the Raft paper makes for safety has the structure
"committed entries survive leader changes because every new leader
contains every committed entry." That argument fails the moment
"committed at term T" stops being a single well-defined event:

- **Log Matching** says: same index + same term ⇒ same entry, and
  identical prefixes thereafter. Two leaders at term T producing
  different entries at the same index voids this for that term and
  every term after — log prefixes from the two halves are no longer
  comparable.
- **Leader Completeness** depends on the election restriction
  (§5.4.1) intersecting any commit-quorum with any election-quorum.
  If two disjoint commit-quorums existed at term T, the next leader
  may be elected from a quorum disjoint from one of them, and that
  leader's AE will overwrite "committed" entries on the other side.
- **State Machine Safety** is what the client sees: replies that
  can't be totally ordered to satisfy real-time constraints. This
  is what Jepsen reports.

The pernicious part is the lag: the divergent commits happen during
the partition, but the overwrite that *makes the linearizability
failure observable* happens later, often after the partition heals
and a new term forms. By the time you see the `:valid? false`, the
two-leader state is gone.

## The Bugs That Cause Same-Term Split Brain (In This Codebase)

- **`voted_for` cleared on same-term AE concession.**
  Lets a node that already voted for the winner vote a second time
  after the winner's first heartbeat erases its memory.
  *This is the cause of the current failing run.*

- **Term not bumped when stepping down.**
  A node that concedes without raising its term remains "current"
  at the old term and can launch new elections there.

- **Election timer not reset on legitimate AE.**
  Followers spuriously time out under a healthy leader, raising
  terms unnecessarily and multiplying chances for races.

- **Phantom log entries on followers.**
  Strictly speaking a different failure (the second "leader" wins
  with a forged log rather than violating one-vote-per-term) but
  the observable outcome is identical: a node imposes writes the
  cluster never sanctioned.

The first three all collapse to: "some node thinks the at-most-one-
leader-per-term invariant is intact when it isn't, and casts a vote
or starts an election that shouldn't be possible."

## Trace From The Current Run

Five-node cluster under `:majorities-ring` partition. `n0` reaches
`{n1, n3}`. `n4` reaches `{n2, n3}`. **`n3` is in both
reachability sets** — this is the load-bearing topological detail.
A majorities-ring partition that left no single node bridging two
candidates would not produce same-term split brain, even with the
voted_for bug, because neither candidate could collect three votes.

| time | node | event | log line |
|---|---|---|---|
| T₀ | `n0` | triggers election, term 0 → 1 | `n0.log:201` |
| T₀+ε | `n4` | triggers election, term 0 → 1 | `n4.log:15` |
| T₁ | `n3` | receives `n0`'s RV first; votes `n0`; `voted_for = "n0"` | `n3.log:11` |
| T₂ | `n0` | tallies 3 votes (`n0`+`n1`+`n3`); becomes leader at term 1 | `n0.log:206` |
| T₃ | `n0` | sends heartbeat AE to `n3` | — |
| T₄ | `n3` | receives `n0`'s AE (term 1, type AE); enters `become_follower_if_applicable`; the same-term concession branch fires; **`voted_for` is cleared to `None`** | `n3.log:13`, code at `key_value_raft.py:443` |
| T₅ | `n3` | receives `n4`'s RV (term 1); sees `voted_for=None`; votes `n4` | `n3.log:16` |
| T₆ | `n4` | tallies 3 votes (`n4`+`n2`+`n3`); becomes leader at term 1 | `n4.log:20` |
| T₇..  | both | replicate concurrently to overlapping sub-quorums; commit divergent entries; `n3`'s log gets whiplashed; snapshots fork | — |
| T₈ | `n4` | applies `cas key=4 from=2 to=0` against its local snapshot (where key 4 happened to be 2 in `n4`'s commit history) and sends `cas_ok` | `n4.log:519` |
| T₉ | client | records `:ok` in Jepsen history at index 497 | `independent/4/history.edn:106` |
| later | Knossos | `last-op` was process 43's OK'd `write 3`; cannot place a `cas [2 0]` after it; `:valid? false` | `independent/4/results.edn` |

The single line of code that breaks the chain is the `voted_for =
None` mutation at step T₄. Every other event in the table is
correct Raft behavior given the state at the moment it ran.

## Mental Model

A leadership claim is a *commitment*. Once `n3` cast its vote for
`n0` at term 1, `n3` had spent its only term-1 vote — that fact
should be as durable as the term itself for the duration of the
term. Treating `voted_for` as transient state that resets whenever
the node "becomes a follower" conflates two things:

- **`state == FOLLOWER`** is a description of the node's current
  *role* — what messages it sends, what loops it runs.
- **`voted_for`** is a description of the node's *commitments
  during the current term* — irreversible until the term advances.

Same-term-AE concession changes the role; it does not undo the
commitment. The bug is essentially a category error: treating a
durable record as if it were transient state.

The general lesson: in any consensus protocol, at any place where
the code is about to clear a field on a state transition, ask "is
this field describing the node's current behavior, or its history of
commitments at this epoch?" The first kind resets freely on
transitions. The second kind only resets when the epoch itself
advances.
