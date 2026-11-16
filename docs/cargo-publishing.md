# Cargo publishing

The `Publish crate` workflow is separate from Rust CI. It runs only when
manually dispatched from `master`.

Cargo reads the package name and version from `Cargo.toml`; its `package.publish`
entry allows publishing to crates.io. Review the source and package contents,
run the affected local checks and `cargo publish --dry-run`, then manually
dispatch `Publish crate` from `master`. The workflow runs
`cargo publish --registry crates-io` with the existing registry token.

This workflow publishes only to crates.io and does not create GitHub Releases.
