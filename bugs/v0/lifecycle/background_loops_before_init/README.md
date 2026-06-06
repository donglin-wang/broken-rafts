# Background Loops Must Not Run Before `INIT`

## The Bug

`main()` starts the election and replication threads before reading
any message:

```python
def main():
    raft = Raft()
    threading.Thread(target=raft.election_loop, daemon=True).start()
    threading.Thread(target=raft.replication_loop, daemon=True).start()
    for line in sys.stdin:
        handle(json.loads(line))
```

`election_deadline` is initialized to roughly `now() + 500–1000ms`
in the constructor. Maelstrom sends `INIT` as the first message,
which carries the node's id, the neighbor list, and the cluster
topology. Until that arrives, `self.node_id is None` and
`self.neighbors == []`.

If `INIT` is delayed past the initial election timeout — by a slow
Maelstrom startup, a heavily-loaded host, or a stop-the-world
scheduler hiccup — `election_loop` fires `trigger_election`, which
calls `request_vote`, which hits `assert self.node_id is not None`
and crashes the process.

Two fixes, either works:

1. **Lazy start.** Start the loops at the end of `handle_init`,
   after `node_id` and `neighbors` are populated.
2. **Guarded loop bodies.** Each loop's body short-circuits while
   `self.node_id is None`:
   ```python
   def election_loop(self):
       while True:
           time.sleep(0.1)
           with self.lock:
               if self.node_id is None:
                   continue
               if datetime.now() >= self.election_deadline:
                   self.trigger_election()
   ```

The lazy-start option is cleaner — it makes the precondition for
the loop's correctness (initialized state) syntactically obvious
from the call site. The guard option is safer to retrofit if
multiple loop bodies have grown init-dependencies and you want a
single point of defense.

## Why It Looks Reasonable

`main()` is a single sequence of "set up everything, then run the
event loop." Putting thread starts at the top, before message
processing, reads as standard initialization order. The threads
are *daemon* threads, so they will be torn down at exit; no leak
concern. The assertion in `request_vote` is a guard against
programmer error — it would never fire in normal operation, right?

The flaw in that reasoning is the implicit assumption that "the
event loop is running" is the same moment as "`INIT` has been
received." They aren't:

- The event loop runs from `main()` start.
- `INIT` arrives from stdin whenever Maelstrom sends it.

There's a real interval between those two events, however short.
The background loops are alive during that interval, and they can
fire periodic logic during it. Tying the loop's correctness to
"INIT has arrived" without enforcing that dependency is a race
that the test harness will eventually win.

## A Trace: The Race

**T=0** — `main()` starts. Constructor runs. `election_deadline =
T + 700ms` (a random pick in the configured range).
`replication_loop` and `election_loop` threads start. Main thread
begins reading stdin, blocks waiting for the first line.

**T=0..700ms** — Maelstrom is busy. The OS hasn't routed the
`INIT` line to our stdin yet. The two background threads are
running, but their bodies haven't done anything load-bearing yet
(`replication_loop` sees `self.state != LEADER` and short-circuits;
`election_loop` checks `now() < self.election_deadline` and
sleeps).

**T=700ms** — `election_loop` wakes up. `now() >=
self.election_deadline` ✓. Calls `self.trigger_election()`:

```python
def trigger_election(self):
    self.state = State.CANDIDATE
    self.term += 1
    self.voted_for = self.node_id     # = None at this point
    self.votes = {self.node_id}       # = {None}
    self.request_vote()
```

`request_vote` iterates `self.neighbors` (empty list) and calls
`send` with — depending on the codebase — either nothing (no
neighbors means no messages, the iteration body is skipped) or a
`src` argument that is `None`. If there's an `assert self.node_id
is not None` somewhere in `send` or in `request_vote`, the assertion
fires.

**T=700ms+ε** — Process exits with `AssertionError` or
`TypeError`. Maelstrom logs "node failed to start" or similar.

## What Makes It Hard To Reproduce

The default election timeout range in this codebase is comfortably
larger than typical `INIT` delivery latency. On a quiet test rig,
`INIT` arrives within a few milliseconds; the race window is
nonexistent.

The bug surfaces when:

- The test harness is under load (many concurrent Maelstrom runs).
- The host has scheduling pressure (CI runners, oversubscribed
  cgroups).
- The user has shortened the election timeout for faster testing
  (which moves the lower bound below typical `INIT` latency).
- A debugger / profiler is attached and stalls the main thread at
  startup.

Each of those alone is unlikely; together they explain the
intermittent CI failures that look like "node crashed at startup
for no reason."

A side effect: even when the race doesn't crash the process, the
spurious election it triggers can pollute the cluster's term
history. A node that "elects itself" with `node_id = None` and
zero peers (because neighbors is empty pre-`INIT`) doesn't have a
useful concept of leadership, but it has bumped its term. Once
`INIT` arrives and proper peer messages start flowing, the node is
already at term ≥ 1 instead of term 0, which composes with other
term-confusion bugs to wedge things further.

## The Right Mental Model

A daemon thread that periodically does work is a *consumer* of
shared state. The state it consumes must be valid and fully initialized
before the thread is allowed to run.

In single-threaded code, "initialize, then run the event loop" is
the obvious enforcement: the event loop *is* the only consumer, so
nothing can consume uninitialized state.

In multi-threaded code, every thread is an independent consumer.
Starting them all at construction time forces every thread to
either (a) tolerate uninitialized state or (b) race with whoever
provides the initialization. Option (a) means every loop body has
a "wait until ready" guard at the top — the option-2 fix above.
Option (b) is the bug.

The cleanest enforcement is to make initialization a state
transition that *gates* loop creation. `handle_init` is the
transition; loop creation should sit at its tail. The threads
don't exist before `INIT` arrives, so they can't fire too early.

## A Subtler Variant

Even with the lazy-start fix, the loop bodies often *read* state
without locking, on the assumption that "the main thread has
already populated it." If `handle_init` populates `node_id`,
`neighbors`, etc. and *then* starts the threads, that's a happens-
before guarantee in the Python memory model (single-threaded write
followed by thread spawn). Fine.

If a later refactor moves the thread start earlier — "for
symmetry," "to share a helper" — and the loop bodies still read
the same state without locking, the happens-before relationship is
gone and the bug returns in a more subtle form (loops see partial
state). This is the kind of thing that decays over time as the
codebase grows; if you've ever seen a Python service that worked
fine for years and suddenly started racing after an innocuous
refactor, this is one of the failure modes.

The guard-each-loop-body option is more resilient against that
decay because the guard itself remains visible. The lazy-start
option is cleaner *now* and more fragile *later*. Both are
correct; the choice is a maintainability call.

## The General Lesson

Background threads, signal handlers, and other "passive" consumers
of state need an explicit happens-before with the initialization
that produces that state. The cheap way to establish that is to
defer thread creation until after initialization completes — which
is also what "initialization" *means* in most other contexts.

The same pattern shows up in:

- **Signal handlers in C.** Installing a handler before
  initializing the data it touches lets a signal arriving at the
  wrong moment read garbage memory.
- **gRPC servers that bind before initializing dependencies.** The
  first request arrives mid-init; the handler reads partial state.
- **JVM `static` initializer ordering.** Threads spawned from one
  class's static init can observe other classes mid-init,
  producing the same race.

The mechanical rule: pair every "I depend on X being initialized"
loop body with a corresponding "X is initialized" creation site.
The two should be syntactically adjacent, ideally with the
creation site as a method that *both* initializes X and starts the
loops that depend on it.
