# Slow Is Dead (On The Network)

## The Principle

In an asynchronous network, a remote node that is silent for T
seconds might be:

- Dead (process crashed, machine unplugged).
- Partitioned (network split, routing black hole).
- Paused (GC pause, cgroup throttle, swap thrash, VM live migration).
- Merely slow (overloaded, disk backed up, queueing downstream).

The local observer cannot distinguish these cases without external
information — and in most distributed protocols, external information
isn't available. Every protocol decision that hinges on "is this node
up?" must either tolerate the ambiguity or paper over it with a
timeout.

The corollary: **a protocol that waits for a specific response from a
specific node is only as reliable as that node's worst case.**
Designing around individuals instead of quorums makes every remote
GC pause a system-wide incident.

## Why It Matters for Raft

Raft's cleverness is that it never needs to wait for a *specific*
follower. Commit requires a *majority*, not a particular set.
Leader election requires a majority of votes, not particular voters.
Every safety rule is phrased in terms of quorums.

Implementations break this by accident when they slip a "wait for
all followers" or "wait for follower X" into a hot path — usually
through a helper that loops over `neighbors` and waits for every
msg_id to come back. That helper appears to be a small
convenience; it's actually a latency amplifier that couples the
cluster to its slowest live node.

## Where It Showed Up Here

`replicate_if_applicable_blocking` called `wait_for(pending)`, which
only returned when *every* msg_id in `pending` had been popped. One
slow follower out of four forced a full 2-second stall on every
write, even though a majority of three had acked in milliseconds.
The timeout fired not because a node was dead but because *one of
four live nodes was slow*. The broader fix is to decouple client
response from handler completion: the handler appends the entry
and returns immediately; the client `write_ok` is emitted later
when commit advancement crosses the relevant index.

## Symptoms of Violating It

- Tail latency of an operation equals the configured timeout.
- A single slow-but-alive follower spikes p99 latency for all
  clients.
- Isolating one node with a partition nemesis stalls the cluster
  entirely, not just that node.
- Recovery after a partition heal takes far longer than expected
  because catching up the straggler is blocking new work.
- Throughput is tightly bound to the slowest replica's disk, not to
  the median.

## The Rule

When a local decision depends on remote responses, phrase the
condition as a count over a quorum, not as a conjunction over a
specific set. "Three of five have acked" is a decidable condition
with bounded latency. "All five have acked" is not — in an async
network it is formally indistinguishable from "three of five have
acked and the other two are dead," but in practice it pays 100× the
latency waiting to find out.

Applies beyond Raft:

- **Broadcast/gather aggregators** — compute results over the
  replies that arrived within the deadline, mark the rest as
  "unknown," don't block.
- **Leader leases** — renew via a majority, not all followers. Loss
  of lease is a timeout, not a specific follower's silence.
- **Health-check systems** — majority-alive is a usable signal;
  all-alive will page someone at 3 a.m. forever.
- **Distributed locks** — client is treated as "gone" by lease
  expiry, not by TCP close, because pause ≈ dead from the lock
  service's perspective.

## What You Can't Do

The principle has a dual that is easy to trip over: since you can't
distinguish slow from dead, you also can't safely treat a slow node
as gone *for purposes of correctness*. Slow is dead for *liveness*
(stop waiting), but a slow node may wake up and act — so any write
or decision it made before going silent must still be respected by
the rest of the system. This is why Raft uses terms and log
matching: it doesn't need to kill the slow leader to make progress,
it just elects a higher-term leader and lets the old one find out
whenever it wakes up.
