# When To Reset The Election Timer

## TL;DR

The election timer has three reset points, and the easy one to miss
is **granting a vote**. Forget it and your followers race their own
votes in a higher term, splitting quorums and stalling elections.

## The Three Reset Points

A follower or candidate must reset its election deadline when:

1. **It receives a valid AppendEntries from the current leader.**
   This is the obvious one — the leader is alive, no need to start a
   new election.
2. **It grants a vote to a candidate in the current or higher term.**
   It has just promised that candidate an election cycle's worth of
   patience. Triggering its own competing election immediately
   contradicts that promise.
3. **It becomes a candidate.** It picks a new randomized timeout for
   the election it just started.

Forgetting (1) makes the cluster too aggressive — followers fire
elections during normal heartbeats. Forgetting (3) re-enters the
election immediately. Forgetting (2) is the silent one: the
follower behaves correctly under low load but generates spurious
elections under partition, and the diagnosis is non-obvious because
the buggy node *did* the right thing (granted a vote) before it did
the wrong thing (timed out anyway).

## Trace: What Forgetting (2) Looks Like

Three nodes, term 7. Election timeouts in [500ms, 2000ms].

**T=0** — n0's election deadline is T=600ms. n1's is T=550ms. n2 is
the current leader.

**T=400ms** — n2 partitions away. Heartbeats stop arriving.

**T=550ms** — n1's deadline fires. n1 becomes candidate at term 8,
sends RequestVote.

**T=560ms** — RequestVote arrives at n0. n0's last_log is up to date,
n1 is a valid candidate. n0 grants the vote, sets `voted_for = n1`,
`term = 8`.

   *In the buggy version, n0 does **not** reset its deadline. It is
   still T=600ms.*

**T=600ms** — n0's deadline expires. n0 becomes a candidate at term
9, sends RequestVote with itself as candidate.

n0 has just contradicted the vote it granted 40ms ago. n0's vote in
term 8 is wasted; n1 cannot win term 8 because n0 has now moved past
it. n0 also can't win term 9 alone — it needs a majority. n2 is
partitioned. n1, who just got promoted to candidate at term 8, will
when it sees n0's term-9 RequestVote either:

- Step down to follower at term 9 (per the higher-term rule), or
- Reject the vote (already `voted_for = self` in term 8).

Either way, term 9 fails to elect. Now n1 has to retry; it bumps to
term 10. n0 may bump first. The cluster spirals up through terms
without electing — the same kind of perpetual-election wedge that
term-confusion bugs produce, but with a different root cause.

## Why It Exists

Randomized election timeouts are the *only* mechanism Raft has to
break symmetry. Two followers at the same time and term will both
trigger elections simultaneously and split the vote unless their
timers happen to differ. The protocol relies on:

> If a follower has just heard from a candidate or leader, it should
> trust them for at least one timeout's worth of time before
> challenging.

A vote grant is a stronger statement than a heartbeat receipt. It
says: "I personally have promised this specific candidate my support
for this term." If the same node fires its own election immediately
after, it is acting in bad faith with respect to its own promise.
Resetting the timer formalizes the cooldown.

## The Subtle Variant: `+=` vs `=`

Even with the reset in place, *how* you reset matters:

```python
# Form A (used in this codebase)
self.election_deadline += self.generate_election_timeout()

# Form B (idiomatic)
self.election_deadline = datetime.now() + self.generate_election_timeout()
```

If the deadline is in the future, both forms push it further out;
fine. But if the deadline already passed (e.g., the node was busy
processing a long handler), Form A adds a timeout to a past
timestamp, which can produce a deadline that is *still in the past*
or only slightly in the future. The next election_loop tick fires
the election anyway, defeating the reset.

Form B is unconditionally correct: the new deadline is always at
least one timeout in the future. Use Form B unless you have a
specific reason for the drift behavior of Form A.

## The General Lesson

Liveness mechanisms in distributed protocols are usually about
*restraint*, not *action*. The election timer is the protocol's way
of saying "don't act yet." Every code path that adds information
about the cluster's state ("a leader is alive," "I have committed
to a candidate") is also a code path that should *delay* the
follower's instinct to act unilaterally.

When you see a timer in a protocol, list the events that should
postpone it. Forgetting any one of them is a class of bug that
behaves identically under load (correct) and under partition
(wedged) — exactly the conditions that make it slip through casual
testing.
