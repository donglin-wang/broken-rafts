# Lock Starvation from a Tight Loop

A thread that re-acquires a lock immediately after releasing it can starve other waiting threads, even though the lock is technically "released" between iterations.

## Minimal Example

```python
import threading
import time

lock = threading.Lock()
handled = False

def tight_loop():
    while True:
        with lock:
            pass  # releases lock, but immediately re-acquires it

def handler():
    global handled
    with lock:      # starved — can rarely win the race
        handled = True

t1 = threading.Thread(target=tight_loop, daemon=True)
t1.start()

time.sleep(0.01)

t2 = threading.Thread(target=handler)
t2.start()
t2.join(timeout=1)

print("handled:", handled)  # often False
```

## Why It Happens

When `tight_loop` exits the `with` block, CPython releases the lock and immediately tries to re-acquire it in the next iteration. The OS may not get a chance to schedule `handler` before `tight_loop` wins the lock again. `handler` is left waiting indefinitely.

## Fix

Release the lock *and* yield the CPU between iterations:

```python
def loop_with_sleep():
    while True:
        with lock:
            pass
        time.sleep(0.01)  # outside the lock — lets other threads run
```

## In the Raft Context

`election_loop` was the tight loop. `handle_request_vote` was the starved handler. Even with randomized election timeouts, nodes triggered elections faster than vote requests could be processed, so every node voted for itself before seeing any peer's `REQUEST_VOTE`.
