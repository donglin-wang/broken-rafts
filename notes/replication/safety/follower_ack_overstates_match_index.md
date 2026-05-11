# Follower Acks Must Report The Accepted Range

## The Bug

`handle_append_entries` reports the follower's whole local tail as the
successful `match_index`:

```python
self.record.apply_entries(message["body"]["entries"], prev_log_index + 1)

...
self.send(
    message["src"],
    {
        "type": MessageType.APPEND_ENTRIES_OK,
        "term": self.term,
        "success": True,
        "match_index": self.record.last_index(),
    },
)
```

That is too strong. A successful `AppendEntries` proves only that the
follower accepted the range described by this request:

```python
acked_through = prev_log_index + len(message["body"]["entries"])
```

It does not prove that any pre-existing suffix on the follower is also
present in the leader's log.

## Why It Looks Reasonable

After the consistency check passes, `apply_entries` has reconciled the
incoming entries against the local log. Reading `record.last_index()`
looks like the natural summary of where the follower now stands.

That is only safe when the follower's log cannot contain extra entries
past the request's end. Raft does allow that situation temporarily:
a follower may have uncommitted entries from an old leader, and a new
leader may first contact it with a heartbeat or a shorter append that
matches some earlier prefix.

In that case, the follower's local suffix is not evidence of replication
from the current leader. Reporting it as `match_index` lets the leader
believe it has replicated entries it may not even have.

## The Asynchrony You Forgot

`AppendEntries` is both a consistency probe and a replication message.
An empty heartbeat with `prev_log_index = N` and `entries = []` asks:

> Does your log match mine at index N?

If the answer is yes, the follower can safely say "I match you through
N." It cannot safely say "I match you through my own last index."

The follower's entries after N might be:

- entries from an old leader,
- entries from a candidate that later lost,
- entries the current leader has not yet checked,
- entries the current leader does not have at all.

The current request has not validated any of them.

## A Trace That Goes Unsafe

Three-node cluster: L is the new leader, F is a follower.

**T=0** - L's log is `[1, 2, 3]`.

**T=1ms** - F's log is `[1, 2, 3, X, Y]`, where `X` and `Y` are
uncommitted entries from an older leader.

**T=10ms** - L sends a heartbeat:

```text
AppendEntries(prev_log_index=3, prev_log_term=term(3), entries=[])
```

The consistency check passes. F does match L at index 3.

**T=11ms** - The buggy reply says:

```text
AppendEntriesOk(success=true, match_index=5)
```

That reply claims F matches L through index 5. It does not. L's log
does not even have index 5.

**T=12ms** - L handles the reply:

```python
self.follower_match_indexes["F"] = max(5, ...)
self.follower_next_indexes["F"] = max(6, ...)
```

Now L believes F has entries through 5 and starts the next replication
attempt from index 6.

**T=100ms** - The replication loop computes:

```python
prev_log_index = follower_next_index - 1   # 5
prev_entry = self.record.at(prev_log_index)
```

Since L's own log ends at 3, `record.at(5)` raises `IndexError`. The
replication loop thread dies, or if the leader's log has grown since
then, the leader can make commit decisions using a follower match index
that was never established by a real append.

## Why The Cluster Doesn't Always Catch You

In the happy path, followers usually do not have extra suffixes:
heartbeats report the same last index the leader expects, and the bug
is invisible.

The bad shape needs leader churn or delayed replication:

- an old leader leaves uncommitted entries on a follower,
- a new leader contacts that follower with a shorter heartbeat,
- the follower reports its local tail instead of the request's end.

Partition tests create exactly that stale-suffix shape.

## The Right Mental Model

A successful `AppendEntries` acknowledges the request's checked range,
not the follower's full log. The response should be tied to the request:

```python
acked_through = prev_log_index + len(message["body"]["entries"])
```

Then the leader can safely do its existing monotonic update:

```python
match_index[src] = max(match_index[src], acked_through)
next_index[src] = max(next_index[src], acked_through + 1)
```

The follower may keep extra suffix entries briefly, but it must not
advertise them as entries accepted from this leader. They become real
only after the leader sends an `AppendEntries` whose range actually
covers them, or they are overwritten by the leader's log.

## Related

- [match_index_from_acked_range](match_index_from_acked_range.md) -
  the leader-side version of the same rule: `match_index` must come
  from the request/response range, not from a moving or unrelated log
  tail.
- [handle_append_entries_ordering](handle_append_entries_ordering.md) -
  another `AppendEntries` receiver bug where side effects are allowed
  to escape before the handler has established what the message proves.
