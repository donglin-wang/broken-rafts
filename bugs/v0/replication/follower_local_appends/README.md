# Followers Locally Append Forwarded Client Requests

## Description

The bug is letting a follower append a client operation to its own `record`
before forwarding the original client message to the known leader. In the
canonical implementation, only the leader appends in
`try_persist_or_forward_entry`:

```python
def try_persist_or_forward_entry(self, entry: LogEntry, message: Message[Any]):
    if self.state == State.LEADER:
        self.record.append(entry)
        self.pending_replies[self.record.last_index()] = message
        self.replication_signal.set()
    elif self.leader is not None:
        self.forward(self.leader, message)
    else:
        self.send(
            message["src"],
            {
                "type": MessageType.ERROR,
                "code": ErrorCode.TEMPORARILY_UNAVAILABLE,
                "in_reply_to": reply_id(message),
                "text": "No leader elected",
            },
        )
```

The buggy version changes the forwarding branch into:

```python
elif self.leader is not None:
    self.record.append(entry)  # BUG: follower forges a local log entry
    self.forward(self.leader, message)
```

That local append looks like a harmless latency optimization: the leader will
probably replicate the same request back to the follower later, so the follower
appears to be doing the work early. It is not harmless. A Raft follower may
change its log only when it handles an `APPEND_ENTRIES` message whose
`prev_log_index`, `prev_log_term`, `entries`, and `leader_commit` fields come
from the current leader. Appending in the forward path bypasses that handshake
and creates a log entry that no leader placed there.

The forged entry is dangerous because the client handlers build it with
`term: self.term`. Once appended, it raises `record.last_index()` and may also
raise or preserve `record.last_term()`. Those are exactly the values advertised
as `last_log_index` and `last_log_term` in a later `REQUEST_VOTE`. A follower
that only forwarded a client request can therefore make itself look more
up-to-date than nodes whose logs contain only leader-issued entries.

## Example

Start with three nodes: `n0`, `n1`, and `n2`. `n0` is leader in term 1.
All nodes have the same committed log:

```text
index: 1  2
log:   A  B
term:  1  1
```

The following execution shows a direct violation of Raft's log-matching
property. During the partition, messages between `n0` and `n1` are delayed or
dropped, but `n0` and `n2` can still communicate and form a majority.

```mermaid
sequenceDiagram
    participant C1 as c1
    participant C2 as c2
    participant N0 as n0 leader, term 1
    participant N1 as n1 follower, term 1
    participant N2 as n2 follower, term 1

    C1->>N1: write x=9
    Note over N1: buggy forward branch appends F at index 3<br/>F = {term: 1, op: write x=9}
    N1--xN0: n1 partitioned, forwarded write x=9 is dropped

    C2->>N0: write x=2
    Note over N0,N2: n0 replicates H at index 3 to n2, H is replicated to majority<br/>H = {term: 1, op: write x=2}
    N0-->>C2: write_ok for H

    Note over N0,N1: log-matching is now violated<br/>n0 has H at index 3, term 1<br/>n1 has F at index 3, term 1

    Note over N1: n1 wins term 2 election<br/>last_log_index=3, last_log_term=1
    C1->>N1: read x
    Note over N1: leader appends R at index 4<br/>R = {term: 2, op: read x}
    N1->>N0: APPEND_ENTRIES prev_log_index=3, prev_log_term=1, entries=[R]
    N0-->>N1: APPEND_ENTRIES_OK success=true, match_index=4
    Note over N1: commit_and_reply_if_applicable commits index 4<br/>n1 applies F, then R
    N1-->>C1: read_ok value=9

    Note over N0: n0 wins term 3 election<br/>its log also has R at index 4, term 2
    C2->>N0: read x
    Note over N0: leader appends S at index 5<br/>S = {term: 3, op: read x}
    N0->>N1: APPEND_ENTRIES prev_log_index=4, prev_log_term=2, entries=[S]
    N1-->>N0: APPEND_ENTRIES_OK success=true, match_index=5
    Note over N0: commit_and_reply_if_applicable commits index 5<br/>n0 applies S after H and R
    N0-->>C2: read_ok value=2
```

The violation happens before the election for term 2:

```text
n0: [A, B, H]   H = {term: 1, op: write x=2}
n1: [A, B, F]   F = {term: 1, op: write x=9}
n2: [A, B, H]   H = {term: 1, op: write x=2}
```

Because reads are implemented as log entries, the bad prefix can affect reads even
when the read entries themselves are leader-owned and replicated normally. `c1`
reads from `n1` after `R` commits and gets `9`, because `n1` applies `R` after
`F`. Later, `c2` reads from `n0` after `S` commits and gets `2`, because `n0`
applies `S` after `H`. The two reads disagree about the state produced by the
index-3, term-1 log position: `n1` has that position as `F`, while `n0`
has it as `H`.

## Implementation Note

Keep the producer/consumer split strict: the leader produces log entries, and
followers consume them through `handle_append_entries`. For
`try_persist_or_forward_entry`, that means:

- `State.LEADER` appends to `record`, stores the original client `message` in
  `pending_replies`, and wakes replication.
- A follower with `self.leader is not None` only calls
  `forward(self.leader, message)`.
- A node without a known leader returns `MessageType.ERROR` with
  `ErrorCode.TEMPORARILY_UNAVAILABLE`.

Do not compensate by adding follower-side `pending_replies` state. A follower
has no authority to commit the entry and no reliable event that can make a
locally queued client request safe to answer. The original client `src` and
`msg_id` already survive `forward`, so the leader can append the operation at
the leader's next index and reply when the committed entry is applied.
