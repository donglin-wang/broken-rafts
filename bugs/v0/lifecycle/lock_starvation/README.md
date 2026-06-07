# Lock Starvation from a Tight Election Loop

## Description

The bug is turning the election background thread into a tight loop that
releases `self.lock` and immediately tries to take it again:

```python
def election_loop(self):
    while True:
        with self.lock:
            if (
                datetime.now() > self.election_deadline
                and self.state != State.LEADER
            ):
                self.trigger_election()
```

The canonical loop sleeps after the `with self.lock` block:

```python
def election_loop(self):
    while True:
        with self.lock:
            if (
                datetime.now() > self.election_deadline
                and self.state != State.LEADER
            ):
                self.trigger_election()
        time.sleep(ELECTION_TICK_S)
```

That sleep is outside the lock on purpose. It yields the CPU between election
checks, giving message-handler threads a chance to acquire the same node lock.

Without the sleep, `election_loop` can win the lock again and again between
iterations. The lock is technically released, but in practice handlers such as
`handle_request_vote`, `handle_append_entries`, and `handle_request_vote_ok`
may wait behind the background loop long enough for the node to stop making
Raft progress.

## Example

Three-node cluster: `n0, n1, n2`. There is no leader yet, and each node's
election loop is spinning without `time.sleep(ELECTION_TICK_S)`.

```mermaid
sequenceDiagram
    participant N0 as n0 election_loop
    participant H0 as n0 request handler
    participant N1 as n1 candidate
    participant N2 as n2 candidate

    Note over N0: takes n0.lock
    Note over N0: election deadline has expired
    N0->>N1: RequestVote(term=1)
    N0->>N2: RequestVote(term=1)
    Note over N0: releases n0.lock
    Note over N0: immediately tries to reacquire n0.lock

    N1->>H0: RequestVote(term=1)
    Note over H0: handle_request_vote waits for n0.lock
    N2->>H0: RequestVote(term=1)
    Note over H0: still waiting for n0.lock

    Note over N0: reacquires n0.lock before handlers run
    Note over N0: repeats the loop without yielding
```

The stalled dependency is local to each node:

```mermaid
flowchart LR
    Loop["election_loop<br/>reacquires self.lock"]
    Handler["handle_request_vote<br/>needs self.lock"]
    Vote["peer candidate<br/>waits for vote response"]

    Loop -- starves --> Handler
    Handler -- cannot reply to --> Vote
    Vote -- times out and starts another election --> Loop
```

Randomized election deadlines do not solve this bug. Once a node's election
thread is spinning, incoming vote and heartbeat messages still need the same
lock to reset the deadline, grant votes, step down, or accept a leader. If
those handlers cannot run promptly, candidates keep timing out and starting new
terms instead of converging on a leader.

## Implementation Note

Background loops that hold a shared node lock should do short local work, leave
the critical section, and then wait outside the lock. The wait can be a sleep,
an event wait, or another blocking primitive, but it must happen after releasing
`self.lock`.

For this election loop, the correct yield is `time.sleep(ELECTION_TICK_S)`
outside the `with self.lock` block. Moving the sleep inside the critical
section would avoid the tight CPU loop but would still block message handlers
while the node is sleeping, so it is a different lock-liveness bug rather than
a fix.
