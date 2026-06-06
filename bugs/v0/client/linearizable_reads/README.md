# Linearizable Reads Are Not Free

## The Gotcha

It is tempting to answer a `read` directly from the leader's local
state machine — after all, the leader has the latest committed log,
right? Wrong. A node can believe it is the leader while a new leader
has already been elected in a higher term and committed writes that
the stale leader hasn't seen. A local read returns a stale value, and
Maelstrom's linearizability checker will catch it.

The paper addresses this briefly in §8 ("Client interaction") but the
details are easy to skim past.

## Why a Leader Can Be Stale

- Network partition isolates the current leader from a majority.
- Followers on the other side elect a new leader (higher term).
- New leader commits writes.
- Old leader, still thinking it's the leader, has no way to know
  locally. It doesn't see the new term until a message arrives.

## Strategies

1. **Log the read.** Treat reads the same as writes: append a "read"
   entry, replicate, commit, then answer from the state machine at
   that commit point. Correct, simple, expensive (one round of
   replication per read).
2. **ReadIndex (paper §8).** On a read:
   - Record `readIndex = commit_index`.
   - Exchange heartbeats with a majority to confirm you're still
     leader.
   - Wait until `last_applied >= readIndex`.
   - Answer from state machine.
   Cheaper than logging; still requires a round trip per read.
3. **Leader leases.** Periodically acquire a time-bounded lease from a
   majority; within the lease, answer reads locally. Cheapest, but
   depends on clock bounds that Maelstrom won't give you.
4. **No-op commit before read.** Variant of ReadIndex: append a no-op,
   read after it commits. Simpler to code than strict ReadIndex.

For a learning implementation in Maelstrom, **strategy 1 (log the
read)** is the easiest to reason about and still beats local reads on
correctness. Optimize later.

## Symptoms If You Read Locally

- Maelstrom reports: "read observed value v1 but the latest committed
  value is v2" after an induced partition (`--nemesis partition`).
- Passes tests with no nemesis; fails the moment partitions appear.
