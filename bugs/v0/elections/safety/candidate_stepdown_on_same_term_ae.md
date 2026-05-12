# Stepping Down: Same-Term AppendEntries Is A Concession

## The Invariant

> At most one leader per term.

This is the central safety property of Raft elections. Election
restrictions (§5.4.1), the no-op rule (§5.4.2), match-index
quorums — all of them collapse if two nodes can simultaneously
believe they are leader at the same term.

The invariant is preserved by majority votes: a candidate needs a
quorum, two candidates in the same term cannot both get one. But
the invariant has a *runtime* consequence too: if a candidate hears
*evidence* that someone else won the election it is currently
contesting, it must step down. That evidence takes the form of a
same-term AppendEntries.

## The Three Cases For `become_follower`

A node receives a message with `incoming_term`. The right thing to
do depends on the term comparison **and** the message type:

| condition | action |
|---|---|
| `incoming_term > self.term` | Always step down, adopt new term. State → FOLLOWER. |
| `incoming_term == self.term` and msg is AppendEntries and self.state == CANDIDATE | Step down (do not change term — already same). |
| `incoming_term == self.term` and msg is AppendEntries and self.state == FOLLOWER | No state change; just process the message and reset election timer. |
| `incoming_term == self.term` and msg is RequestVote | Do not step down. Vote logic decides. |
| `incoming_term < self.term` | **Never step down. Reject the message.** |

The most common bug in this table is collapsing it to "step down on
any AppendEntries" — which is wrong for the last row.

## The Wrong-Fix Trap

If your original code was:

```python
def become_follower_if_applicable(self, message):
    if message["body"]["term"] <= self.term:
        return False
    # ... step down
```

Then a candidate ignoring same-term AE never steps down on the
"I lost the election" signal. So you're tempted to relax the
guard:

```python
if (message["body"]["term"] <= self.term
    and message["body"]["type"] != APPEND_ENTRIES):
    return False
```

This relaxation is wrong in a worse way. It now also triggers the
step-down body for AppendEntries with `incoming_term < self.term`
(a stale leader's heartbeat). The body sets
`self.term = incoming_term`, which is a **term decrease**.

After that, the AppendEntries handler runs:

```python
incoming_term = message["body"]["term"]    # e.g., 2
self.become_follower_if_applicable(message) # sets self.term = 2 (was 5)

if incoming_term < self.term:  # 2 < 2 → False
    # reject
    return

# proceed to apply entries from a stale leader at term 2
```

The check that was supposed to reject stale leaders no longer
fires, because the node just adopted the stale leader's term. The
stale leader can rewrite the node's log — a flagrant safety
violation in the very direction the §5.1 term rules exist to
prevent.

## Trace: Stale Leader Rewriting a Log

Five-node cluster. Healthy leader L at term 5. Long-isolated node X
that thinks it is leader at term 2.

**T=0** — Partition heals. X reconnects.

**T=10ms** — X's replication loop fires. X sends an AppendEntries
to follower F at term 2 with entries from X's stale log.

**T=20ms** — F processes the message:
- `become_follower_if_applicable(msg)` runs.
- Buggy condition: `2 <= 5 and type != AE` → `True and False` → `False`. Falls through.
- Sets `self.term = 2`, state = FOLLOWER, voted_for = None.
- F has just lost its memory of term 5.

**T=20ms+ε** — `handle_append_entries` continues:
- `incoming_term < self.term` → `2 < 2` → False. No reject.
- Applies X's stale entries, possibly overwriting committed
  entries from L's term-3 or term-4 work.

**T=21ms** — L's next AppendEntries arrives at F with term 5.
F is now at term 2, so it correctly bumps to term 5 and rejoins.
But the damage is done: F's log has been corrupted with X's stale
data, and any commit that depended on F's match-index was based on
a lie.

The whole point of the term-comparison rule is that **a node never
adopts a smaller term**. The buggy fix violated that, and gave the
network's most behind node a microphone.

## The Right Conditional

The clean way to say this is:

```
should_step_down = (
    incoming_term > self.term
    or (incoming_term == self.term
        and msg_type == APPEND_ENTRIES
        and self.state == CANDIDATE)
)
```

Two distinct reasons compose to one decision. Don't try to express
them as a single arithmetic comparison; the cases aren't aligned in
a way that allows it.

A subtler refinement: the "step down on same-term AE" branch should
*not* update `self.term` (it's already equal) and should *not* clear
`voted_for` (you may have already voted in this term, and that
record should stand for the rest of the term). Only the "higher
term" branch resets those fields.

## Why CANDIDATE Specifically

A FOLLOWER that receives a same-term AppendEntries doesn't need to
"step down" — it's already a follower. It just needs to update its
view of who the leader is and reset its election timer. Code that
indiscriminately does the full step-down dance (clearing
`voted_for`, resetting state) for a follower in the steady state is
wasteful but not unsafe. Code that does it for a CANDIDATE is
mandatory.

## The General Lesson

In any consensus protocol, "I just lost the election" is a runtime
fact you learn from observing other participants succeed. The
protocol gives you a small set of signals — same-term commit
messages, leader heartbeats, etc. — that mean "stop trying, someone
else won this round." Coding the receiver to recognize each signal
and concede is part of the protocol; if you only handle "I see a
higher term," you've implemented promotion but not concession.

Concession is what keeps the at-most-one-leader-per-term invariant
mechanically true even when two nodes briefly raced for it.
