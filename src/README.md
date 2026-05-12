# Canonical Implementations

Each directory under `src/` is a versioned canonical implementation.

- `v1/` is the current baseline.
- Future versions should use the same shape (`raft.py`, `node.py`) so the
  shared root `main.py` can dispatch with `--version vN`.

Run a version directly through the shared wrapper:

```bash
./main.py --version v1
```
