# broken-rafts

A gallery of Raft implementation bugs, each tied to a canonical Raft KV-store
implementation and explained as a concrete failure mode. The goal is to make
wrong implementations legible: what changed, why the code looked reasonable,
and how it breaks under Maelstrom's `lin-kv` workload.

## Get Started

Prerequisites:

- [uv](https://docs.astral.sh/uv/) on `PATH`.
- [Maelstrom](https://github.com/jepsen-io/maelstrom) on `PATH` for distributed
  correctness tests. See Maelstrom's
  [Getting Ready guide](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md)
  or the [release downloads](https://github.com/jepsen-io/maelstrom/releases).
- Python 3.14 free-threading build (`3.14t`). The shebang on `main.py` asks
  `uv` to resolve it.

Run the v0 Maelstrom baseline:

```bash
maelstrom test \
  -w lin-kv \
  --bin './main.py --version v0' \
  --time-limit 60 \
  --node-count 3 \
  --concurrency 4n \
  --rate 30 \
  --nemesis partition
```

Browse the `bugs/` folder for writeups.

For patch-backed bugs, apply the patch to the matching canonical version,
run Maelstrom, then reverse the patch:

```bash
git apply <path-to-bug>/bug.patch
maelstrom test -w lin-kv --bin './main.py --version v0' --time-limit 60 \
  --node-count 3 --concurrency 4n --rate 30 --nemesis partition
git apply -R <path-to-bug>/bug.patch
```

## Layout

- `main.py` - shared Maelstrom entrypoint. It defaults to `v0` and accepts
  `--version vN` for future canonical implementations.
- `src/v0/` - canonical v0 implementation.
  - `raft.py` contains the Raft node and KV state machine.
  - `node.py` contains the Maelstrom JSON protocol plumbing.
- `bugs/v0/` - v0 bug writeups and, for patch-backed bugs, patch artifacts.
  Bugs are organized by subsystem. Browse the folder for the current set.
- `src/README.md` - short note on canonical implementation versioning.
- `pyproject.toml` / `uv.lock` - Python project metadata and locked tooling.

Patch files are versioned by directory. A patch under `bugs/v0/` is expected to
apply to `src/v0/`; a future `bugs/v1/` patch should apply to `src/v1/`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution and agent guidance.

## Roadmap And Scope

v0 has one narrow goal: pass Maelstrom `lin-kv` under the partition nemesis
with a compact, readable Raft implementation.

In scope for v0:

- Fixed membership from Maelstrom `init`.
- In-memory Raft state.
- Leader election, log replication, commit advancement, and a KV state machine.
- Linearizable `read`, `write`, and `cas` behavior for the target workload.
- Real OS threads for election and replication loops.

Out of scope for v0:

- **Persistence.** `term`, `voted_for`, and the log live in memory only.
- **Membership change.** No joint consensus, learners, or add/remove flow.
- **Crash recovery.** A killed or restarted node is not expected to rejoin.
- **Snapshots and log compaction.**
- **Production read optimizations.** No ReadIndex or lease reads.
- **Operational hardening.** No pre-vote, CheckQuorum, leadership transfer, or
  full observability story.

Future canonical versions can add those features without rewriting history:
add `src/v1/`, place matching bug specimens under `bugs/v1/`, and keep old v0
patches tied to `src/v0/`.
