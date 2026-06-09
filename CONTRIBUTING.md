# Contributing

New bugs should be added as patch-backed specimens against a specific canonical
version.

1. Choose the canonical version the bug applies to, such as `v0`.
2. Add a directory under `bugs/<version>/<subsystem>/<short_name>/`.
3. Include:
   - `README.md` - the explanatory writeup.
   - `bug.patch` - the minimal patch that turns the canonical implementation
     into the buggy implementation.
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

## Bug Writeups

A bug writeup should be concrete and reproducible. Use this section structure:

1. `## Description` - define the bug, show the buggy code or logic, and
   explain why it is wrong.
2. `## Example` or `## Examples` - give concrete executions that expose the
   bug. Use `## Example` when there is only one example. Use `## Examples` and
   `### Example <number>` subsections only when there are multiple examples.
3. `## Additional Issues` - optional. Include this only when it adds related
   failure modes or consequences that are not already covered by the examples.
4. `## Implementation Note` - optional. Include this only when it adds useful
   implementation guidance or a mental model that is not already clear from the
   description.

Prefer Mermaid diagrams for executions and dependency cycles. Sequence diagrams
work well for message traces; flowcharts work well for wait-for or causality
cycles. Keep diagrams tied to concrete node names, message types, and state
fields from the matching canonical implementation.

Bug writeups already live under versioned directories such as `bugs/v0/`.
Avoid repeating the version in prose with phrases like "the v0 implementation"
or "the v0 commit path" unless the version distinction is essential to the
point being made.

## Code Conventions

- Keep each canonical implementation under `src/vN/`.
- Keep v0 standard-library-only.
- Guard shared Raft state with `self.lock`.
- Keep `Record` internal to `raft.py`.
- Reuse existing `MessageType` values unless the bug genuinely needs a new
  protocol message.

## For Agents

- Treat this document as the repository's agent guidance.
- Before creating or updating a bug patch, read the bug's `README.md` and make
  the canonical implementation temporarily match that description.
- Generate `bug.patch` with `git diff` from the canonical implementation change,
  then revert the canonical implementation so the patch is the only artifact
  left behind.
- When reviewing a bug trace, first check that the execution can actually occur
  assuming every implementation detail outside the described bug is correct.
  For Raft traces, validate election eligibility, log freshness, term changes,
  append consistency checks, quorum calculations, and commit guards before
  accepting the trace as evidence.
- Also review writeups for consistency and coherence: the description,
  diagram, example prose, message fields, terms, indexes, node states, and
  claimed invariant violation should agree with each other and within the trace
  itself.
- Do not overwrite unrelated user changes in the worktree.
