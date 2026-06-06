# Followers Don't Append (Even on Forward)

## The Gotcha

When a client write lands on a follower, the follower has to forward it
to the leader — fine. The tempting micro-optimization is "while I'm at
it, I'll also append the entry to my own log; the leader is going to
replicate it back to me anyway, so I'm just front-running the work."

That is a safety bug, not an optimization. Only the leader is allowed
to extend the log. A follower that appends locally creates **phantom
entries** that no leader has sanctioned, and those phantoms can win
elections, propagate to the cluster, and produce double-applies —
the symptom shape is that a client observes state that no quorum
ever stored, but the root cause here is forged log entries on the
follower rather than reads outrunning commit.

## What I Built First

`try_persist_or_forward_entry` had three branches:

```
if state == LEADER:
    record.append(entry)
    pending_ok[last_index] = message
    replicate
elif leader is not None:
    record.append(entry)        # <-- the bug
    forward(leader, message)
else:
    error(TEMPORARILY_UNAVAILABLE)
```

The middle branch is the offender. The follower mutates its own log
without ever having received an `AppendEntries` for that entry.

## Why It's Wrong (The Principle)

Raft's safety rests on a single invariant for the log: **the leader's
log is authoritative; followers only mutate their log in response to
`AppendEntries` from the current leader.** Every safety property in §5
of the paper — Log Matching, Leader Completeness, State Machine Safety
— assumes followers are passive log recipients. The moment a follower
extends its log on its own, all of those proofs lose their footing for
that node.

The phantom entry has two attributes that make it dangerous:

1. Its `term` is set to `self.term` at the moment of append — usually
   the *current* term, which makes it look maximally up-to-date.
2. It bumps `record.last_index()` and `record.last_term()` — the two
   quantities every other node consults during the election
   "up-to-date" check (§5.4.1).

Together, those two things let a follower with phantoms win elections
it has no business winning.

## Concrete Trace: Phantom Wins an Election

Three-node cluster: leader `n0`, followers `n1` and `n2`. All logs
agree at `[A, B]` (indices 1 and 2), term 5.

**T=0** — client `c1` sends `write x=9` to `n1`. `n1` is not leader,
so it takes the buggy branch:

- `n1.record.append({term: 5, op: write x=9})` → `n1`'s log is now
  `[A, B, F]`, with `F.term = 5`. Index 3.
- `n1.forward(n0, c1_msg)`.

`n1`'s `last_index` is now 3, `last_term` is 5. `n0` and `n2` still
see `last_index=2`. `n0` hasn't yet appended `F`.

**T=10ms** — the network drops the forwarded message before `n0`
receives it. (No nemesis required; Maelstrom drops messages on the
happy path.) The client will eventually retry, but for now `n0` knows
nothing about `F`.

**T=600ms** — `n1`'s election timer fires (it was a long-tail timeout
draw). `n1` becomes a candidate in term 6 and sends
`request_vote{term: 6, last_log_index: 3, last_log_term: 5}` to `n0`
and `n2`.

**T=605ms** — `n0` and `n2` evaluate the up-to-date rule:

- Their own `last_log_term` is 5, candidate's is 5 → tied.
- Their own `last_log_index` is 2, candidate's is 3 → candidate is
  *more* up-to-date.

So both grant the vote. `n1` wins the election in term 6, despite the
fact that `F` was never replicated to anyone.

**T=610ms** — `n1` becomes leader. Its log is `[A, B, F]`. It starts
sending `AppendEntries` to `n0` and `n2` with `prev_log_index=2,
entries=[F]`. They append `F` (passes the `prev_log_index/term`
check). Once a majority has it, `n1` advances `commit_index` to 3 and
applies `F`: `snapshot[x] = 9`.

The cluster has now committed a write that the client never confirmed
and may never have intended to repeat. From the client's view: it
forwarded `write x=9`, got no response, eventually timed out, retried.
The retry could land elsewhere as a *new* request, get committed
again. Now `x=9` was applied twice — and this is one of the failure
modes a per-client session table (sequence numbers + cached
responses) is supposed to prevent, except the session table can't
help here because the *first* application happened without ever
flowing through a client request that carried a sequence number the
leader had registered.

## Compounding: `apply_entries` Doesn't Truncate

The trace above was the "lucky" case where the phantom-carrying
follower also happened to win the election cleanly. The unluckier
case shows up when the phantom-carrying node *doesn't* win, and the
real leader's `AppendEntries` arrives later.

Look at `Record.apply_entries`. The loop overwrites in place and then
appends the tail of `incoming_entries`, but it **never truncates
existing entries past the end of the incoming batch**:

```
[A, B, F-phantom]                        # follower's log
AppendEntries(prev=2, entries=[C])       # from real leader
->  __entries[2] = C  # F-phantom overwritten
->  end of loop: [A, B, C]               # works in this case
```

So far so good. But:

```
[A, B, F-phantom, G-phantom, H-phantom]  # two more phantoms accumulated
AppendEntries(prev=2, entries=[C])
->  __entries[2] = C
->  end: [A, B, C, G-phantom, H-phantom] # phantoms past index 3 SURVIVE
```

The Raft paper §5.3 is explicit: "If an existing entry conflicts with
a new one (same index but different terms), delete the existing entry
and **all that follow it**." Our `apply_entries` does the first half
and skips the second. Combined with locally-appended phantoms, the
follower's log can carry stale ghost entries indefinitely. If that
follower later wins an election (see previous trace), it will
propagate the ghosts.

This is technically a separate bug from the local-append, but the two
amplify each other. Local-appends manufacture the phantoms;
non-truncating `apply_entries` lets them survive contact with the
real leader.

## Pending_ok Doesn't Save You

You might think: "fine, but the follower didn't put the message in
`pending_ok`, so when the phantom commits, no client gets a wrong
reply." True for the *immediate* commit. But:

- The state machine still mutates. `snapshot[x] = 9` happens on every
  node that commits the phantom. Future reads will observe it.
- The original client retried, hit the new leader, and got an `:ok`
  back for what it thought was its single write. Linearizability is
  defined over the *committed log* compared against the *operations
  the client believes happened*. A committed `write x=9` with no
  corresponding client invocation is not a phantom in the
  read-side "vanishing state" sense, but it does pollute the model
  the Knossos checker is reconstructing.
- For non-idempotent ops (CAS, increment), the second application is
  a real correctness divergence. A `cas x 5→6` that succeeds on the
  first leader, then re-applies as a phantom on a new leader, fails
  the precondition the second time and the snapshots can diverge from
  what the client model predicts.

## The Fix

The follower's job in the forward branch is exactly one thing:
**relay the message.** No log mutation, no `pending_ok` entry, no
local bookkeeping. The leader will append at its own next index, will
own the `pending_ok` slot, and will reply to the client when commit
advances.

Two corollaries follow from this:

1. The client's `src` must survive the forward, so the leader's reply
   goes back to the right place. `forward` already preserves `src`
   via `deepcopy`; just don't add a local append next to it.
2. If the follower can't reach the leader (no leader known, or the
   forward send fails), the right answer is the existing
   `TEMPORARILY_UNAVAILABLE` error, not "I'll hold this for you."
   Holding pending client requests on a non-leader is the same
   problem leaders face on step-down, but in reverse: the non-leader
   has no commit advancement that can ever resolve the record.

Separately, fix `Record.apply_entries` to truncate any tail past the
end of the incoming batch when there's a conflict at the overwritten
index. That closes the "phantoms survive overwrite" amplifier even
if some other path ever manages to introduce a phantom.

## The Mental Model

Raft is built on a strict producer/consumer split for the log: the
leader produces, followers consume. Every place in the code where a
non-leader mutates `record` should be suspicious by default — the
only legitimate case is `handle_append_entries`, and even there it's
always in response to an explicit leader-issued message that says
"here are entries, starting at this index, predicated on this prior
state." Anything that bypasses that handshake is forging the leader's
signature on log entries.

The phrase to internalize: **a log entry's existence is a leadership
claim.** If the entry is in the log at index N with term T, the
implicit assertion is "the leader of term T placed this here." A
follower writing into its own log breaks that assertion silently, and
every safety argument downstream depends on it being true.
