# Stepping Down Means *I* Am Not The Leader, And Neither Is Anyone (Yet)

## The Bug

`become_follower_if_applicable`, on the higher-term branch, updates
the term, state, and `voted_for` — but never clears `self.leader`:

```python
if incoming_term > self.term:
    self.term = incoming_term
    self.state = State.FOLLOWER
    self.voted_for = None
    # self.leader is not touched
    return True
```

The motivating trigger is usually a `RequestVote` from a new
candidate in a higher term. RVs carry no `leader_id` (the candidate
doesn't know who the leader is — *it* is trying to become leader).
So the early `if "leader_id" in body` branch doesn't fire either.

Result: a former leader who steps down because someone else is
campaigning still has `self.leader == self.node_id`. From the
former leader's perspective, "I am no longer leader" and "I am the
leader" are simultaneously true.

The window lasts until the new leader's first AppendEntries
arrives. With ~100ms heartbeat intervals and election timeouts in
the hundreds of milliseconds, that's a 100–500ms window. Long
enough to mishandle multiple client requests.

The fix is one line in the higher-term branch:

```python
self.leader = None
```

## Why It Looks Reasonable

`self.term`, `self.state`, and `self.voted_for` are the three
fields the Raft paper explicitly names as "reset on step-down."
`self.leader` is an implementation detail this codebase added to
track "who do I forward client requests to" — it doesn't appear in
the paper's pseudocode, so it doesn't get the "reset on step-down"
treatment in any obvious place.

The intuition driving the bug is: "the next AppendEntries from the
new leader will overwrite `self.leader` with the correct value, so
I don't need to clear it." That's true *eventually*. The bug lives
in the interval between "I stepped down" and "the new leader has
sent me a heartbeat."

A subtler version of the same intuition: "clearing `self.leader`
just makes things worse — now the node has *no* leader to forward
to, instead of a slightly-wrong one." This is exactly backwards.
The slightly-wrong value isn't a useful guess; it's a confidently
wrong one. Clients deserve a "no leader" error and a chance to
retry — not silent forwarding into a black hole.

## A Trace: Client Forward to Self

Three-node cluster. A is leader at term 5. B and C are followers.

**T=0** — Network glitch isolates A from B and C for a beat.
Heartbeats stop arriving at B and C.

**T=600ms** — B's election timer fires. B becomes candidate at
term 6. Sends `RequestVote{term=6, candidate_id=B}` to A and C.
The RV does not include a `leader_id` field (or sets it to `None`).

**T=605ms** — A receives B's RV. `become_follower_if_applicable`:

- `incoming_term (6) > self.term (5)` ✓.
- `self.term = 6`, `self.state = FOLLOWER`, `voted_for = None`.
- `self.leader` is **not** updated. It is still `A`.

A has just stepped down but still thinks it is the leader.

**T=610ms** — Client `c1` sends `write x=9` to A. A's handler runs:

```python
def handle_write(self, message):
    if self.state == LEADER:
        # append + replicate
    elif self.leader is not None:
        self.forward(self.leader, message)
    else:
        self.reply_error(TEMPORARILY_UNAVAILABLE)
```

A is no longer leader (state is FOLLOWER). `self.leader = A`. A
forwards the message to A. **The message loops back to itself.**

Two outcomes depending on the codebase:

- **Loop suppression at the network layer:** Maelstrom may detect
  the loop or the message is silently dropped, and `c1` times out.
- **No loop suppression:** A re-processes the forwarded message,
  hits the same branch, forwards to A again. Repeat until some TTL
  or queue limit kicks in.

Either way: `c1` gets no response within its timeout, retries, may
hit a different node next time. The bug doesn't violate
linearizability directly — no commit happens — but it converts
"reliable forwarding under leadership change" into "best-effort
forwarding plus client retries."

**T=700ms** — B wins the election, sends its first AppendEntries to
A. A's `handle_append_entries` runs, term check passes, the
`self.leader = leader_id` update fires (assuming the leader-id
update is correctly gated on a current-term AE rather than firing
from any incoming message). `self.leader = B`. The window closes.

## The Compounding Case: Step-Down From CANDIDATE Without Prior Leader

Another path into the same wedge: a candidate at term 5 (so
`self.leader = None` already, fine) receives a higher-term
`AppendEntries` and steps down. The new term has a real leader, but
the RV bug above doesn't apply here — AE *does* carry `leader_id`,
so `become_follower_if_applicable`'s early branch should set it.

But: the early branch fires *before* the term-bump branch. If the
ordering inside `become_follower_if_applicable` is:

```python
if "leader_id" in body and body["leader_id"] is not None:
    self.leader = body["leader_id"]   # step 1

if incoming_term > self.term:
    ...                               # step 2
    self.voted_for = None
    # but no self.leader = ... here
```

then step 1 fires correctly with the new leader's id. Fine for this
case.

The pure step-down-on-RV case is the one this note is specifically
about, and it's the one that needs the fix.

## Why the Cluster Doesn't Always Catch You

In a healthy network, leadership changes are rare. The window
between step-down and the new leader's first AE is bounded by one
heartbeat interval — and during that window, the probability of a
client request arriving at the just-demoted ex-leader is small.

The bug surfaces under:

- High client request rates (more chances of a request landing in
  the window).
- Partition nemeses that produce rapid leadership churn (more
  windows per unit time).
- Maelstrom test configurations with many clients all hammering all
  nodes — the symptom shows up as unexpected `temporarily
  unavailable` rates or client timeouts during transitions.

## The Right Mental Model

`self.leader` answers the question *"to whom should I forward
client requests right now?"* The correct answer at any moment is
exactly one of three things:

1. **A specific node N**, when I have direct evidence — a
   current-term `AppendEntries` from N — that N is the leader.
2. **Myself**, when `self.state == LEADER`.
3. **`None`**, when neither of the above is true.

A node that has just stepped down from leader satisfies neither
(1) nor (2). It must be in case (3) until evidence for (1) arrives.

The bug stems from treating `self.leader` as "the last node I
believed to be leader" — a memory of *past* belief — when its
operational meaning is "the node I currently believe is leader" —
*current* belief. Old leadership beliefs are inadmissible the
moment they're known to be stale; the step-down is itself the
notification of staleness.

The mechanical rule pairs with `voted_for`'s reset on term advance:
both fields are scoped to *the current term*, and both must be
reset (or at least re-evaluated) when the term advances.

| Field | Resets on |
|---|---|
| `voted_for` | term advance |
| `votes` | term advance + becoming leader |
| `self.leader` | term advance + becoming leader (set to self) |

Of the three, `self.leader` was the one missing from the
term-advance reset path.

## The General Lesson

Any cached "who is the authority for X" pointer must be invalidated
when the authority changes — and "I just learned the authority
changed" is the change event. Don't wait for the new authority to
announce itself; the cached value is already known to be wrong.

The same shape shows up in:

- **DNS resolver caches** during a primary-replica failover: the
  cached IP of the old primary is wrong the moment the failover
  signal arrives, regardless of whether the new primary's record
  has propagated.
- **Service discovery** where a client caches "the leader of
  partition K is node N": when N's lease expires (the discovery
  event), the cached value should be cleared rather than retained
  pending a replacement.
- **Database replication topology**: a former primary that has been
  demoted must reject writes until the new topology is known, not
  fall back to "I was primary recently, I'll handle it."

The mechanical rule: if a field's value is "the current authority
for X," it must be invalidated on every event that could change
the authority. The set of such events typically includes term
advances, election losses, and any explicit "you are no longer
authoritative" signal.
