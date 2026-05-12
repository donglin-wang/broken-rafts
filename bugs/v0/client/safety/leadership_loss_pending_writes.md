# Leadership Loss with Pending Client Requests

## The Gotcha

Once you decouple client response from the handler — handler returns
immediately after appending; client `write_ok` is emitted later when
commit advancement crosses the relevant index — the leader
accumulates a dict of pending client requests keyed by log index.
What happens to those records if the node stops being leader before
those indices commit?

The paper does not address this; it only describes the log-level
protocol.

## Why It Matters

Three failure modes:

1. **Silent hang.** If you do nothing, the ex-leader holds the client
   record forever. The client times out and retries, but the ex-leader
   still has the dangling state — memory leak plus potential confusion
   if the client's retry eventually reaches the same node as follower.
2. **Stale success.** If the entry the client was waiting on gets
   overwritten by the new leader's log (allowed by Raft when it's
   uncommitted), the client was about to be told "ok" for a write that
   never happened. Replying based on local commit_index alone is not
   enough — you must confirm you were the one who owned that slot.
3. **Double reply.** If the client retries and the new leader succeeds,
   and the old leader *also* eventually replies based on stale state,
   the client sees two responses for one request.

## Options

- **Eager cancel.** On step-down (any transition to FOLLOWER), walk
  pending requests and reply with an error — in Maelstrom,
  `temporarily unavailable` (code 11) is the idiomatic choice. The
  client retries, probably finds the new leader, and moves on.
- **Silent drop.** Just clear the dict and let the client time out.
  Correct but wastes a client timeout per pending request.
- **Forward.** If the ex-leader knows the new leader, proxy the request.
  Works but adds complexity; not necessary for a learning
  implementation.

Eager cancel is the simplest correct choice.

## Implementation Note

The transition point is the same place `become_follower_if_applicable`
fires. Any `term > self.term` message triggers it, as does an election
timeout from CANDIDATE. In each of those, before clearing leader state,
drain the pending-clients dict and emit errors.

Also: a newly-elected leader should start with an *empty* pending
dict. It does not inherit the previous leader's pending clients — they
were the old leader's problem.
