# No-op Entry on Leader Election

## The Gotcha

Figure 8 of the Raft paper shows a subtle safety bug: a newly-elected
leader **cannot** safely commit entries from prior terms just because
they are replicated to a majority. The paper states the rule (§5.4.2)
but the practical consequence is easy to miss: *a leader must commit
an entry from its own term before it can commit anything at all.*

## Why

Suppose leader L1 in term 2 replicates entry E to a majority but
crashes before committing it. A new leader L2 is elected in term 3.
E is still on a majority of nodes. If L2 counts that majority and
commits E, a future leader L3 (term 4) could be elected from a minority
that doesn't have E and *overwrite* it — violating the commit
guarantee. The fix is that L2 cannot commit E until L2 also replicates
an entry of its own term to a majority; that entry's commit transitively
commits E under the Log Matching Property.

## Practical Implication

If nothing's happening on the cluster, a newly elected leader has no
term-N entry to replicate, so prior-term entries stay uncommitted
forever. Fix: **have the leader append a no-op entry immediately upon
election** and replicate it. Once that no-op commits, all preceding
entries are safely commitable.

## Symptoms If You Forget

- Idle cluster, no writes, previous term's writes never get applied.
- Maelstrom linearizability checker reports stale reads right after a
  leader change.
- Commit index appears "stuck" for some election cycles.

## Implementation Note

The no-op is a regular log entry with an op the state machine knows to
ignore (e.g. `{"op": "noop"}`). It goes through the same replication
and commit path as any other entry; there is no special-casing beyond
"append it when you become leader."
