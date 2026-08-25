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

- **No Python.** Not a script, not `python3 -c`, not a heredoc. Reaching for it is the
  tell that a step is being solved by parsing when the tool that owns the answer could
  just be asked. Do not swap it for another parser either, and do not assume `jq` is
  present: it does not ship with macOS. A fixed-shape field is one `sed -nE` line;
  anything needing real parsing belongs in this repo's own language, where it can be
  tested. If a task seems to need Python, the approach is wrong.

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
- **Never create merge commits — this is a hard ban.** Not locally, not to refresh a
  branch, not to land a pull request. If your branch has fallen behind, **rebase** it onto
  the moved base (`git rebase origin/master`, then `--force-with-lease`). `git merge master`
  into a feature branch is not an acceptable shortcut: it adds a commit whose only content
  is the fact that you were behind, and it turns a readable line of work into a diamond.
- **Rebase is the default everywhere** — refreshing a branch, and landing a pull request.
  Individual commits carry information: what was tried, in what order, and why. A rebase
  keeps that granularity on the base branch, so write commits worth keeping and land them
  intact.
- **Landing a pull request means rebase, then fast-forward.** `git rebase origin/master`
  on the branch, then `git merge --ff-only <branch>` on the base, then push. Those two
  commands are the whole job, so don't reach for `gh pr merge`: its default writes a
  merge commit. Rebasing rewrites the commit SHAs, so GitHub cannot always detect that
  a branch landed — close such pull requests explicitly and say why.
- **Don't delete remote branches by hand.** Once the work is on the default branch it is
  reaped automatically. Deleting your own local copy is fine.
- **Squash is acceptable** where it genuinely makes things easier or is the more
  appropriate shape for the branch — one logical change scattered across fixup commits, or
  a long branch whose intermediate states aren't worth preserving. It is a judgement call,
  not a violation. Merging is the only thing that is never allowed.
- **Delete what is deprecated.** A superseded file, flag, branch or code path gets removed
  in the change that supersedes it, not left behind with a deprecation note.

## No AI attribution

Never add AI attribution to anything in this repo or leaving it: no "Generated with
Claude Code" / robot-emoji footers, no `Co-Authored-By: Claude` (or any AI) trailers,
and no AI credit in commit messages, PR or issue titles/bodies, changelogs, release
notes, or code comments. Applies to every agent and every vendor. Work product should
be indistinguishable from a human teammate's.
