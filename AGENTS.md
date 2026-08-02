# Working agreement — WorkTablesIndex

The operating contract for **any** coding agent working in this repository,
mirroring the standard used in [pathscale/WorkTable](https://github.com/pathscale/WorkTable)
and applying to Codex, Cursor, Gemini CLI (native `AGENTS.md` readers) and
Claude Code (via the `@AGENTS.md` import in `CLAUDE.md`). **Never fork these
rules into a per-vendor file.**

## What this crate is

pathscale's maintained downstream package of
[indexset](https://github.com/lucidarium-systems/indexset), published on
crates.io as `WorkTablesIndex`. It was created to ship the `NodeLike::halve()`
`len()/2` fix before upstream adopted it. Upstream now carries that fix and its
regression tests; this package remains the coordinated, exactly pinned source
shared by WorkTable and DataBucket.

Consumers (`worktable`, `data_bucket`) depend on it via a **package alias**:

```toml
indexset = { package = "WorkTablesIndex", version = "=0.0.1", features = ["concurrent", "cdc", "multimap"] }
```

so every `use indexset::` path in their code keeps working unchanged.

## Invariants (don't break these)

- **Minimal diff to upstream.** No reformatting of inherited code, no lint-fix
  churn, no gratuitous refactors: the smaller our diff, the easier it is to
  exchange patches with upstream and to rebase onto their releases. The
  `[lints]` tables in `Cargo.toml` freeze the lint classes already present in
  inherited code; CI denies everything else. Do not add allows for new code.
- **Type identity is load-bearing.** `worktable` and `data_bucket` must always
  pin the *same exact version* of this crate — two source crates providing the
  same `Pair`/`ChangeEvent` types cannot coexist in one dependency tree. Any
  version bump here requires bumping both consumers together.
- **On-disk geometry coupling.** Changes to node split/merge behaviour change
  WorkTable's space-index golden fixtures
  (`tests/data/expected/space_index/indexset/…` in the WorkTable repo). Ship
  such changes together with regenerated fixtures there.
- **Doc examples import `indexset::` on purpose.** That is correct for aliased
  consumers; it just cannot resolve under this crate name locally, so CI runs
  `cargo test --all-features --lib --tests` (no doctests). Don't "fix" the
  examples.
- **Publishing to crates.io is irreversible.** Versions can never be reused;
  yanking does not delete. `cargo publish --dry-run` first, publish from the
  default branch, and remember the exact-pin consumers.

## Testing tiers

Automatic CI runs the fast tier only: tests that take too long (the
multi-threaded stress tests, which spawn 8 OS threads over spin-retry
structures and can livelock on 2-core runners) are reserved for special
testing via the manually triggered `Heavy tests` workflow
(`gh workflow run heavy.yml`). Run it whenever concurrency-relevant code
changes. Keep this split honest: a test belongs in the fast tier unless it
demonstrably cannot run quickly everywhere.

## Build & test

```bash
cargo build --all-targets --all-features
cargo test --all-features --lib --tests
cargo clippy --all-targets --all-features -- -D warnings   # allow-list lives in Cargo.toml [lints]
```

## Syncing with upstream

Add `upstream` as a remote (`git remote add upstream
https://github.com/lucidarium-systems/indexset.git`), then rebase or
cherry-pick upstream changes in order. Preserve the package rename and CI
policy, and re-run the WorkTable suite (including fixture checks) before
publishing. Keep fork-only commits clearly labelled and the source diff from
upstream minimal.

## Git workflow

- Default branch: `master`. Never force-push it.
- Branch naming for changes: `fix/…` or `feat/…`; PRs preferred for anything
  beyond upstream-sync mechanics.

## No AI attribution

Never add AI attribution to anything in this repo or leaving it: no "Generated with
Claude Code" / robot-emoji footers, no `Co-Authored-By: Claude` (or any AI) trailers,
and no AI credit in commit messages, PR or issue titles/bodies, changelogs, release
notes, or code comments. Applies to every agent and every vendor. Work product should
be indistinguishable from a human teammate's.
