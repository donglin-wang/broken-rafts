# broken-rafts

A gallery of Raft implementation bugs. Each branch presents one specific bug
in an otherwise-working Raft KV store, with a Maelstrom test that reliably
reproduces it and a note explaining the underlying mistake.

## Why this exists

Raft is well-specified but easy to misimplement. The paper's pseudocode reads
like RPC, the safety invariants read like obvious code, and most bugs only
surface under network partition with leader churn — conditions that casual
testing does not produce. This repo is the record of mistakes made while
implementing Raft from scratch against Maelstrom's `lin-kv` workload, each
isolated as a runnable demonstration.

The interesting artifact is the *notes*, not the code. Anyone can copy a
working Raft. The point of this repo is to make the wrong shapes legible.

## Layout

- `main` — working implementation. Passes Maelstrom `lin-kv` under the
  partition nemesis.
- `bug/<short-name>` — one branch per bug. Each branch:
  - Reverts exactly the lines on `main` that fix the bug (minimal diff).
  - Adds `notes/<short-name>.md` explaining the mistake.

`git diff main -- key_value_raft.py` on any bug branch shows the one
logical change at issue.

## Implementation

- **Python 3.14, free-threading build (`3.14t`)**. The shebang on
  `key_value_raft.py` is `#!/usr/bin/env -S uv run --python 3.14t`, so
  invoking the file directly is enough — no manual environment setup.
- **[uv](https://docs.astral.sh/uv/)** is the only required tool besides
  Maelstrom itself. It resolves the interpreter via the shebang.
- **Standard library only.** `node.py` is a thin Maelstrom protocol layer
  (newline-delimited JSON over stdin/stdout). No third-party dependencies.
- The node runs real OS threads for the replication and election loops.
  Free-threading lets them run without the GIL serializing them, which makes
  the asynchrony bugs more reliably visible.

## Running a bug demo

Prerequisites:

- [Maelstrom](https://github.com/jepsen-io/maelstrom) on `PATH` (the install
  doc covers the JDK + Graphviz dependencies).
- [uv](https://docs.astral.sh/uv/) on `PATH`.

Then:

```bash
git checkout bug/<short-name>
cat notes/<short-name>.md     # read the note first

maelstrom test \
  -w lin-kv \
  --bin ./key_value_raft.py \
  --time-limit 60 \
  --node-count 3 \
  --concurrency 4n \
  --rate 30 \
  --nemesis partition
```

Knossos rejects the resulting history. The diff back to `main` is the fix:

```bash
git diff main -- key_value_raft.py
```

To see the working baseline pass:

```bash
git checkout main
maelstrom test -w lin-kv --bin ./key_value_raft.py --time-limit 60 \
  --node-count 3 --concurrency 4n --rate 30 --nemesis partition
```

---

## For future contributors (humans and LLM agents)

This section is the spec for adding new bugs to this repo. Treat it as
authoritative.

### Each bug is one branch

1. Branch from latest `main` as `bug/<short-name>`.
2. Revert exactly the lines on `main` that fix the bug. Smaller diff = clearer
   demo. If the diff exceeds ~10 lines or touches multiple concerns, the bug
   isn't isolated — split it.
3. Add `notes/<short-name>.md`. Filename uses `_` (matches existing notes);
   branch name uses `-`.
4. Verify the Maelstrom command in the "Running a bug demo" block reliably
   triggers a Knossos rejection within ~2 minutes. If reproduction is under
   ~80% of runs, raise `--time-limit`, increase `--rate`, or add another
   nemesis (e.g. `--nemesis partition,kill`) until it's reliable. Document
   any deviation from the default command at the top of the note.

### Note structure

Every note follows the same skeleton. The existing roster lives at
`.idea/notes/` (gitignored local reference material from the originating
playground repo) — read several before writing a new one to internalize the
voice. The skeleton:

1. **The Bug** — the buggy code, 5–10 lines max. Show it, don't describe it.
2. **Why It Looks Reasonable** — the misreading of the spec or the mental
   model that makes the bug feel correct. This is the heart of the note.
3. **The Asynchrony You Forgot** (or analogous "what makes the bug visible")
   — the runtime conditions under which the bug manifests.
4. **A Trace That Goes Unsafe** — concrete timeline (`T=0`, `T=10ms`, …)
   showing one execution that violates linearizability or another invariant.
5. **Why The Cluster Doesn't Always Catch You** — the steady-state masking
   that hides the bug in healthy networks. Explains why partition testing
   was needed to find it.
6. **The Right Mental Model** — the correct framing, with the alternative
   shapes spelled out (usually two: "stamp the request" and "echo it back",
   or analogous pairs).
7. **The General Lesson** — generalize the pattern beyond Raft, with two or
   three examples from other systems (TCP, HTTP, two-phase commit, etc.).
8. **Related** — cross-links to other notes with shared root cause.

Tone: explanatory, careful with `§`-references to the Raft paper, concrete
over abstract, no hedging without reason. Code blocks for code; prose for
intuition.

### Code conventions

- The Raft node lives in a single file, `key_value_raft.py`. Don't split it
  unless `main` itself does.
- `Record` (the log abstraction) is internal to that file. The note may refer
  to its methods (`last_index()`, `at(i)`, `slice_from(i)`, `last_term()`).
- All shared state on the node is guarded by `self.lock`. Background loops
  (`replication_loop`, `election_loop`) reacquire it on each tick.
- Message types are in the `MessageType` enum. Reuse existing types where
  possible; only add new ones if the bug genuinely requires it.

### Before opening a bug branch

- [ ] Bug reproduces in ≥80% of Maelstrom runs at the documented command.
- [ ] Diff vs `main` is one logical change.
- [ ] Note exists at `notes/<short-name>.md` and follows the skeleton.
- [ ] Note cross-links existing related notes (avoid duplicating root-cause
      coverage — extend instead).
- [ ] No new third-party dependencies introduced.
- [ ] The note is written so a reader who has never seen the codebase can
      understand both the bug and why it matters.
