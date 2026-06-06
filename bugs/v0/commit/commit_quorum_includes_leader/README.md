# The Leader Counts: Quorum Math For `commit_index`

## The Symptom

Three-node cluster, no nemesis, low load. Throughput is fine. Now
introduce a partition that isolates one follower. Suddenly **every**
client write fails with `temporarily-unavailable: No leader elected`,
or the operation simply hangs until timeout. The leader is alive,
quorum is intact (leader + one follower = 2 of 3), but nothing
commits.

## The Buggy Computation

```python
def commit_and_reply_if_applicable(self):
    ...
    index = median(self.follower_match_indexes.values())
    ...
```

`follower_match_indexes` only tracks *followers*. The leader is not
in it. The "median" is computed over the followers' match-indexes
alone.

That sounds reasonable — "commit when the median follower has the
entry" is a thing you might say in casual conversation about Raft.
It is not what the protocol says.

## What Raft Actually Requires

Commit happens when a **majority of the cluster** (including the
leader) has replicated the entry. The leader trivially has every
entry it appended; that's one vote already. So the rule reduces to:

> Commit index N when at least `majority(N) - 1` followers have
> match_index ≥ N.

For a 3-node cluster: `majority(3) = 2`, so we need `2 - 1 = 1`
follower with the entry. Leader + that follower = quorum.

For a 5-node cluster: need `3 - 1 = 2` followers.

For a 2-node cluster: need `2 - 1 = 1` follower (i.e., both).

## Cluster-Size Walkthrough

Let `n = total nodes`, `f = followers = n-1`. The threshold I want
to extract from the sorted follower match-indexes is the
`(majority(n) - 1)`-th-largest. Equivalently in ascending order, the
element at index `f - (majority(n) - 1) = f - majority(n) + 1`.

If you instead compute `majority(f)` and pull `f - majority(f)`-th
element (the buggy code), you get:

| n | f | majority(n)-1 | majority(f) | buggy threshold demands | needs |
|---|---|---|---|---|---|
| 2 | 1 | 1 | 1 | 1 of 1 followers | 1 of 1 ✓ |
| 3 | 2 | 1 | 2 | **2 of 2** followers | 1 of 2 |
| 4 | 3 | 2 | 2 | 2 of 3 followers | 2 of 3 ✓ |
| 5 | 4 | 2 | 3 | **3 of 4** followers | 2 of 4 |

The bug is benign for even `n` (4 and 6 happen to coincide) but
catastrophic for odd `n` — including the most common Maelstrom
configuration of 3 nodes, where it demands *unanimity* instead of a
majority. With one follower partitioned away, a 3-node cluster's
buggy leader can never commit anything.

This is exactly why availability collapses under partition: a single
slow or partitioned follower out of two gates *every* write. The
healthy follower acks promptly; the leader sits on its hands waiting
for the dead one. The broader principle is: phrase the condition as
a count over a quorum, not a conjunction over a specific set.

## Why It's Easy To Miss

The Raft paper's commit rule is stated in terms of "a majority of
matchIndex[i] ≥ N." Reading that quickly, "majority of matchIndex"
gets parsed as "majority of the dict I called matchIndex" — and that
dict, in most implementations, only contains followers. The leader's
own progress is implicit and gets dropped on the floor.

The fix is to either:

1. Include the leader's `last_index()` in the input to the median.
2. Special-case the leader and require `majority(n) - 1` followers
   to be at or above the candidate index.

Option (1) is one line and works for all `n`. The whole point is
that the median operates over *the cluster*, not over the followers
dict.

## A Smaller Trap Inside The Same Calculation

Even with the leader added, there's a second subtlety: the candidate
index N must be from the **current term** before it's eligible to
commit (§5.4.2). The check looks something like:

```python
if self.commit_index < index and self.record.last_term() == self.term:
    self.commit_at(index, True)
```

`last_term() == self.term` is a coarse approximation of "the entry at
N is from the current term." It's correct as long as nothing in
between is from an earlier term, which is true for an append-only
leader, but the *reason* the check exists is the §5.4.2 rule, not
"last term equals current term." If you ever change how the log can
be edited (e.g., snapshots), come back here and re-derive.

## The Mental Model

The committed prefix of the log is the prefix that will survive any
future leader change. "Survival" requires that any future leader had
the entry when it was elected. By the election restriction (§5.4.1),
a leader's log includes everything from any majority that voted for
it. So commit = "a majority has it" = "no future leader can be
elected without it."

The leader is a participant in that majority, not a referee on the
sidelines counting the others. Treating it as the latter is the
specific logic error in this bug.
