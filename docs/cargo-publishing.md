# Cargo publishing contract

This repository is the reference for manifest-driven Cargo publishing. The
`Publish crate` workflow is deliberately separate from Rust CI and runs only
when manually dispatched on `master`.

1. Change `[package].version` in `Cargo.toml` explicitly. That is the version to
   publish, not a value inferred from commits, a tag, or a second version file.
   Workspace packages may inherit `[workspace.package].version`; independently
   versioned packages retain their own versions. A workflow-only edit does not
   require a version bump.
2. Review the source and package contents, complete the affected local checks,
   and run `cargo publish --dry-run` locally. Record the exact reviewed commit.
   No remote test suite is run as part of publishing.
3. Dispatch `Publish crate` from `master`, provide that full commit SHA, and
   attest that local checks passed. The manifest must explicitly include
   `crates-io` in `package.publish`. The helper verifies the checkout matches
   the supplied commit and refuses dirty or untracked files.
4. The publisher reads the package name and resolved version from Cargo's
   manifest metadata. It checks that exact version on crates.io. Only HTTP 404
   means absent; an existing version is a no-op, and timeouts, authentication
   failures, server errors, and every other response fail closed.
5. The workflow serializes publishing runs within this repository. The helper
   uses Cargo's normal publish verification and keeps the token in the
   environment. The dry-run gate is performed locally before dispatch.
6. Verify the published version is available. Process dependencies before their
   consumers and pace crates.io publication across the fleet; the workflow's
   concurrency group does not coordinate different repositories.

The contract continues to support crates.vip through the existing `cargo vip
publish` path and its existing configuration. This crates.io workflow neither
creates nor enables crates.vip configuration, and one registry's success does
not imply success at another. Private packages must exclude crates.io from
their manifest allowlist.

The standard does not require synchronized versions across repositories,
automatic dependency bumps, application deployments, or GitHub Releases.
Compatible dependency requirements remain unchanged unless the new code needs
a newer minimum or compatibility changes. Existing lockfile policies remain in
force.
