# Vote Grants Must Be Idempotent Within a Term

## The Bug

`handle_request_vote` denied a candidate that the receiver had
*already voted for*:

```python
vote_granted = (
    self.voted_for is None
    and incoming_term >= self.term
    and candidate_up_to_date
)
```

The first clause is too strict. §5.2 of the Raft paper states the
rule precisely:

> If `votedFor` is null **or `candidateId`**, and the candidate's log
> is at least as up-to-date as the receiver's log, grant vote.

The "or `candidateId`" clause is the part that gets dropped on a
casual transcription. It is the only thing standing between the
protocol and a permanent election wedge under retry.

The fix is one line:

```python
self.voted_for in (None, message["body"]["candidate_id"])
```

## Why It Looks Reasonable

"You only get to vote once per term" is the safety property the field
exists to enforce. `voted_for is None` reads as the obvious encoding:
the slot is unset → no vote cast → free to vote. Anything else → vote
already cast → refuse.

The trap is that "vote already cast" and "vote already cast *for
someone else*" are not the same condition. The safety rule only
forbids the latter. A vote granted to candidate X, repeated to
candidate X in the same term, is the *same* vote — it doesn't add
weight, it doesn't risk a two-vote-from-one-voter outcome, it is
literally the act the protocol already authorized.

The casual encoding conflates "I have spent my vote" with "I can
never confirm spending it again." Under reliable delivery these are
equivalent. Under message loss they diverge, and the protocol's
correctness depends on the latter interpretation, not the former.

## The Asynchrony You Forgot

A `RequestVote` reply is a single UDP-shaped message. Maelstrom (and
any realistic network model) is allowed to drop it. The candidate
has no way to distinguish "the voter refused" from "the reply got
lost" except by retrying.

The retry path is critical for liveness. A candidate that times out
waiting for an answer must re-send its `RequestVote` in the same term
— it has not advanced its term, has not lost the election, has
simply not heard back. If the receiver refuses the retry, the
candidate's only options are:

1. Wait longer. The candidate's own election timeout fires first and
   it bumps to a new term — losing whatever votes it has already
   accumulated and starting from zero.
2. Give up. Equivalent.

Neither produces a leader. With unlucky drop patterns (and Maelstrom
will find them) the cluster wedges itself shut: every candidate's
votes get scattered across multiple terms, no term collects a
majority, terms climb forever.

## A Trace That Wedges

Three-node cluster: A, B, C. All at term 0, empty logs.

**T=0** — A's election timer fires. `trigger_election`: term 0→1,
`voted_for = A`, `votes = {A}`. Sends `RequestVote{term=1,
candidate_id=A}` to B and C.

**T=10ms** — B receives, evaluates: `voted_for is None` ✓, term/log
checks pass. B sets `voted_for = A`, replies
`RequestVoteOk{vote_granted=true}`. **Reply is dropped by the
network.**

**T=10ms** — C receives, grants similarly. Reply delivers normally.
A has `votes = {A, C}`. `len = 2 ≥ majority(3) = 2`. **A becomes
leader at term 1.** (For trace clarity assume C's reply is in flight
when A retries; the wedge generalizes if A is still pending.)

Actually let's reshape the trace — the more pathological case is
when *no one* has hit majority yet:

**T=0** — A starts election at term 1, sends RV to B and C.

**T=10ms** — B grants, reply dropped. C grants, reply dropped.
B: `voted_for = A`. C: `voted_for = A`. A: `votes = {A}`. Still below
majority.

**T=300ms** — A's retry timer fires (or the user is using an
explicit per-RPC timeout). A sends `RequestVote{term=1,
candidate_id=A}` to B and C again — still in term 1, hasn't bumped
because it doesn't know its votes have been counted.

**T=310ms** — B receives the retry. Buggy check: `voted_for is None`
→ false (it's A). **Vote denied.** Symmetric for C.

**T=320ms** — Both denials arrive at A. A still has `votes = {A}`.

**T=600ms** — A's election timeout fires (no majority assembled). A
runs `trigger_election` again: term 1 → 2, `voted_for = A`,
`votes = {A}`. Sends `RequestVote{term=2}`.

B and C receive. Their `voted_for` is still A (from term 1) — but
the buggy check is `voted_for is None`, not term-scoped, so even if
B and C had updated `voted_for` on the term-2 RV (which they should,
since the higher term resets), the underlying issue is that the
term-1 retry path was already broken.

Now consider the same trace with B and C at slightly different
deadlines. B times out, becomes candidate at term 2; C times out,
becomes candidate at term 2 too. They split the vote (each votes for
itself), no one wins term 2. Term climbs. Repeat.

The cluster does not converge. With the fix in place, B and C's
denials at T=310ms turn into grants, A collects majority, election
completes in term 1 — exactly what the protocol intended.

## Why the Cluster Doesn't Always Catch You

Same failure mode as the other "asynchrony-only" Raft bugs in this set: at
reliable-network rates, every `RequestVote` reply gets through on
the first send, no retries are needed, and the buggy clause is never
exercised. The bug is latent under quiet runs and reliably visible
under any network model that drops messages.

Maelstrom's partition nemesis is the obvious trigger, but it shows
up under `--latency` and `--packet-loss` workloads too. Any test
that produces a single dropped `RequestVote` reply will surface it.

## The Right Mental Model

`voted_for` is a *commitment*, not a *latch*. The semantics it
encodes are:

> In term T, I have committed my vote to candidate X. Any subsequent
> question about my term-T vote must be answered consistently:
> "yes, X, that one."

A latch interpretation says: "vote was cast, refuse all subsequent
queries until the latch is cleared." A commitment interpretation
says: "vote was cast, repeat the same answer to anyone who asks the
same question."

The protocol needs the commitment semantics for two distinct
reasons:

- **Liveness (this note):** retries must succeed, or message loss
  permanently wedges elections.
- **Idempotence under duplication:** even without loss, the network
  may deliver the same `RequestVote` twice. Both copies must produce
  the same answer — otherwise the candidate sees inconsistent
  evidence about who voted for it.

Both reasons collapse to the same code change. The mental shift is
recognizing that `voted_for is None` is the wrong encoding of "I
have not yet committed to anyone *other than* the asker."

## The General Lesson

Wherever a protocol records "I have done X in epoch N," the recording
must support *idempotent re-confirmation* by the original initiator
of X. The original asker is allowed (and often required) to ask
again; the receiver's answer must be stable.

The same issue shows up in:

- **Two-phase commit prepare votes.** A participant that voted YES
  must repeat YES on retry. Refusing the retry forces the
  coordinator into an unnecessary ABORT path.
- **Lease acquisitions.** A client that successfully acquired lease
  L and lost the ack must, on retry, learn that it already holds L —
  not be told "someone else has it" because the lease service
  treated the second request as a new claim.
- **Idempotency keys in HTTP APIs.** The same key replayed must
  yield the same response, not a 409 conflict.

The general rule: any state field that records "I committed to X in
epoch N" should be queried as `field in (None, X)`, not `field is
None`. The first form is the commitment; the second form is a latch
that breaks under retry.
