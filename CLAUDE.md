@AGENTS.md

# Claude Code notes — WorkTablesIndex

The import above is binding: [`AGENTS.md`](AGENTS.md) is the working agreement
for this repository. One source of truth, no drift — only genuinely
Claude-specific wiring belongs below.

- This repo has no `.claude/` guardrail hooks (unlike WorkTable). Apply the
  same discipline manually: ask before pushing, publishing, or anything
  destructive.
- The most common task here is syncing with upstream or shipping a fix that
  WorkTable needs; read the "Invariants" section before touching inherited
  code, and remember every version bump cascades into `worktable` and
  `data_bucket` exact pins.
