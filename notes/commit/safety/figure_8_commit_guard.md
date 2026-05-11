# Committing Only Current-Term Entries: §5.4.2 In The Commit Guard

## The Bug

In `commit_and_reply_if_applicable`, the gate on advancing
`commit_index` was originally:

```python
if self.commit_index < index and self.record.last_term() == self.term:
    self.commit_at(index, True)
```

The right-hand condition asks the wrong question. `record.last_term()`
is the term written next to the *most recent* entry in the leader's
log — anywhere in the log. What §5.4.2 of the Raft paper actually
requires is that the entry **at the index being committed by counting
replicas** is from the leader's current term:

```python
if self.commit_index < index and self.record.at(index)["term"] == self.term:
    self.commit_at(index, True)
```

These two checks are usually equivalent, which is what lets the bug
sit unnoticed. They diverge precisely when the leader's log holds a
tail of current-term entries above one or more *uncommitted*
prior-term entries — and the proposed commit index lands on one of
the prior-term ones.

## Why It Looks Reasonable

`last_term() == self.term` translates intuitively to: "I have appended
at least one entry in my current term." That feels like a sensible
precondition for committing, and it actually *is* a sensible
precondition for a different invariant (the no-op-on-election
liveness fix). The mistake is using a liveness-flavored check to
enforce a safety rule.

The safety rule §5.4.2 states is narrower: don't commit *this
specific entry* by quorum count unless *this specific entry* is from
your term. Indices below it ride along under the Log Matching
Property — they get committed transitively, never directly.

## What Goes Wrong (Figure 8)

Construct a leader L at term 5 whose log is:

```
index:  1   2   3   4   5
term:   3   3   3   3   5
```

Indices 1–4 were appended by an earlier term-3 leader that crashed
before getting them to a majority. L inherited them at election time
(its log was at least as up-to-date as the rest, so it won §5.4.1).
L has now appended one term-5 entry at index 5.

`record.last_term() == 5` — the buggy check passes.

Now suppose L's followers have replicated through index 3 but not 4
or 5. Median match index is 3. The buggy code reads "majority has
index 3" and calls `commit_at(3)`. The op at index 3 is applied to
the state machine and a `:ok` is returned to the client.

L crashes before entry 5 reaches anyone. A new leader emerges whose
log diverges from L's at index 3 — its inherited term-3 fragment was
shorter, and the §5.4.1 chain (Figure 8 walks through the exact
sequence in the paper) lets it win the next election. The new
leader's log does **not** contain L's entry 3. But L told a client
that entry 3's op committed.

**Linearizability violated.** This is precisely the pattern Figure 8
of the Raft paper exists to demonstrate.

The corrected check refuses to commit index 3 (its term is 3, not 5).
It only fires when the median reaches index 5 — at which point
indices 1–4 commit transitively, safely, by Log Matching.

## Why The Bug Is Rare In Healthy Networks

Same reason as the other asynchrony-sensitive commit-path bugs:
most of the time the window is empty.

A leader at term N with a healthy network appends one term-N entry
within milliseconds of taking office. Once that entry replicates to
majority, the median lands on a term-N entry, and the buggy and
correct checks agree. The divergence window is the brief interval
between "I have prior-term entries in my log" and "I have replicated
my first current-term entry to majority."

Maelstrom's partition nemesis stretches that window: leader churn
means more nodes inherit uncommitted prior-term tails, and the
partition itself delays the catch-up replication that would otherwise
close the gap.

## The Right Mental Model

There are two term-flavored numbers about the leader's log at any
moment:

- **`last_term()`** — the term written next to the *most recent*
  entry in the log.
- **`at(commit_target).term`** — the term written next to the entry
  *being proposed for commit*.

§5.4.2 talks about the second one. Nothing about the first one
appears in the rule. Whenever a Raft commit-or-vote check needs a
term, the question to ask is *"the term of which entry, exactly?"* —
then read the paper clause again to find the right answer.

This is the third manifestation in this codebase of the same
underlying bug pattern, "two values that look interchangeable but
aren't":

1. **Election up-to-date check (§5.4.1):** `self.term` vs
   `record.last_term()`.
2. **Become-leader no-op (§5.4.2 workaround):** a related site where
   the gap between `self.term` and `last_term()` matters.
3. **Commit guard (§5.4.2 itself):** `record.last_term()` vs
   `record.at(index).term` — this note.

If you find yourself comparing two term-flavored numbers in a Raft
predicate, stop and name *which entry* each term belongs to. There
is almost always a wrong choice that compiles and runs.

## The General Lesson

Safety rules in Raft are stated about specific log positions, not
about the leader's log as a whole. "An entry is safe to commit by
counting replicas if and only if that entry is from the current
term" — the subject of the sentence is the entry, not the leader.
Code that translates the rule into `if leader.something == ...`
instead of `if entry_at(index).something == ...` has dropped the
subject and is asking a different question.

The same shape shows up elsewhere:

- §5.4.1 election check ("candidate's log is at least as up-to-date
  as receiver's log") — about the *candidate's last entry* and
  *receiver's last entry*, not about either node's `currentTerm`.
- AppendEntries consistency check — about the *entry at
  `prev_log_index`* in both logs, not about either log's tail.

Whenever a paper clause names two specific positions, the code has
to name them too. Substituting "the leader's current state" for one
of the positions is the bug.
