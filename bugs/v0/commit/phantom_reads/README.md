# Phantom Reads

## The Symptom

A client observes a value that later "vanishes" — either a subsequent
read returns an older value, or the write that produced the value is
not present in the final committed log. The client's earlier read saw
data that, by Raft's guarantees, was never actually there.

This is the single most common correctness failure in a naive Raft
implementation. The Raft paper does not use the term "phantom read"
and addresses the pieces separately (§5.4, §8), so it is easy to
implement each piece correctly in isolation and still end up with
phantoms.

## Three Ways Phantoms Get Born

The same symptom has three distinct root causes. A correct
implementation must close all three.

### 1. Applying Uncommitted Entries to the State Machine

The leader appends at index N and immediately mutates its local state
machine (KV snapshot, counter, whatever). Reads answered from that
state machine will return the new value *before* index N has been
replicated to a majority. If the leader then loses leadership and the
entry is overwritten, every read that observed it was a phantom.

**Rule:** the state machine reflects the **committed** log, not the
log. Apply entries only when `last_applied < commit_index`, and never
past `commit_index`.

**Watch out for:** a `commit_if_applicable` that walks `self.entries`
from `commit_index` to `len(entries)` and applies everything. That is
exactly this bug — it treats "in the log" as "committed."

### 2. Replying to a Write Before Commit

The leader appends at index N, confirms *itself* has the entry, and
immediately returns `write_ok` to the client. The client now believes
the write succeeded and may read it elsewhere. If the entry never
commits, the write is a phantom from the client's perspective.

**Rule:** client `write_ok` is emitted only when `commit_index`
advances past N (and you were still the leader who owned slot N —
on step-down, drain any pending client records with an error rather
than letting them resolve from stale state). The handler must not
block on majority acks; commit advancement is what triggers the
reply.

### 3. Serving Reads from a Stale Leader

A node believes it is leader but has been superseded by a new leader
in a higher term it hasn't heard from yet (partition). Reads answered
from its local (even correctly committed) state are stale — newer
committed writes exist that this node hasn't seen. Every such read is
a phantom with respect to the real committed history.

**Rule:** reads must be ordered against the committed log at the time
they are answered. Either log the read as an entry, use ReadIndex, or
hold a leader lease.

## Why All Three Matter

Fixing any one of these while leaving the others open still produces
phantoms. They are independent failure modes that happen to share a
symptom:

- Fix (1) alone → still get phantoms from (2) if you reply early, or
  from (3) under partition.
- Fix (2) alone → still get phantoms from (1) because reads touch the
  snapshot directly.
- Fix (3) alone → still get phantoms from (1) because the snapshot
  itself is contaminated.

The unifying principle: **the state the client observes — through
reads *or* through write acknowledgments — must be a prefix of the
globally committed log, as of a moment when this node was
authoritatively the leader.** Any path that lets a client observe
state that doesn't satisfy that invariant produces phantoms.

## How Maelstrom Catches It

The linearizability checker (Knossos / elle) reconstructs a plausible
total order of all client-visible operations. A phantom read produces
an operation that cannot be placed in any order consistent with the
others — typically surfaced as "read observed `v` but no prior write
of `v` is consistent with later state." Phantoms are almost always
induced by partition nemeses (`--nemesis partition`), rarely by the
happy path.

## Debugging Checklist

When Maelstrom reports a phantom:

1. Is the state machine being mutated by uncommitted entries? Check
   that `last_applied` is bounded above by `commit_index`.
2. Are client responses gated on commit, or on local append?
3. Are reads answered from local state without a liveness check
   (ReadIndex / logged read)?
4. On step-down, are pending client records cancelled, or can a
   stale reply still be sent?
