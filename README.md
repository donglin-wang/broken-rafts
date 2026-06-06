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

New bugs should be added as patch-backed specimens against a specific canonical
version.

1. Choose the canonical version the bug applies to, such as `v0`.
2. Add a directory under `bugs/<version>/<subsystem>/<short_name>/`.
3. Include:
   - `README.md` - the explanatory writeup.
   - `bug.patch` - the minimal patch that turns the canonical implementation
     into the buggy implementation.
   - `meta.toml` - metadata such as canonical version, category, title, and
     default Maelstrom settings.
4. Keep the patch to one logical mistake. If it touches multiple concerns,
   split it into multiple bug specimens.
5. Verify the patch applies:

```bash
git apply --check bugs/<version>/<subsystem>/<short_name>/bug.patch
```

6. Verify the bug reliably reproduces with Maelstrom. The default target is:

```bash
maelstrom test -w lin-kv --bin './main.py --version <version>' \
  --time-limit 60 --node-count 3 --concurrency 4n --rate 30 \
  --nemesis partition
```

A bug writeup should be concrete and reproducible. Use this section structure:

1. `## Description` - define the bug, show the buggy code or logic, and
   explain why it is wrong.
2. `## Examples` - give elaborate concrete executions that expose the bug.
3. `### Example <number>` - one numbered subsection per elaborate example.
4. `## Additional issues` - document related problems that can arise but do
   not need full worked examples.
5. `## Implementation note` - explain the correct implementation approach and
   the mental model to use.

Prefer Mermaid diagrams for executions and dependency cycles. Sequence diagrams
work well for message traces; flowcharts work well for wait-for or causality
cycles. Keep diagrams tied to concrete node names, message types, and state
fields from the matching canonical implementation.

Code conventions:

- Keep each canonical implementation under `src/vN/`.
- Keep v0 standard-library-only.
- Guard shared Raft state with `self.lock`.
- Keep `Record` internal to `raft.py`.
- Reuse existing `MessageType` values unless the bug genuinely needs a new
  protocol message.

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
