# TODO

## Open review items

- [x] Implement the [manifest-driven Cargo publishing contract](cargo-publishing.md)
  for this crate: manual-only, exact reviewed commit, explicit registry
  eligibility, and registry errors distinguished from an absent version.
  Static review and ten mocked success/refusal scenarios passed; no live
  publication or workflow dispatch was performed for this change.

- [ ] Define the compatible serde representation for tree invariants, then
  validate deserialized trees before exposing them to concurrent operations.
- [ ] Review registry publish action references, token scope, and release inputs;
  preserve the manual-only workflow trigger.
- [ ] Extend the source audit to iterators, node internals, range/split paths, and
  map operations. Keep static reasoning separate from tests and model evidence.

## Resolved queue findings

- [x] Final singleton deletion revalidates and unlinks under topology write,
  emitting the actual deleted member before concurrent refill can proceed.
  Ordinary non-final removals retain their read-lock path. Payload and
  deterministic refill-order regressions cover this correction.
- [x] Replace-in-place CDC removal carries the value actually stored before
  replacement, including its value payload; focused regression covers a map
  replacement within a multi-entry node.
- [x] Odd topology generations count toward point-reader retry fallback. The
  existing blocking fallback can then wait for the structural writer instead
  of spinning forever. Source-reviewed; no dedicated concurrency regression
  was added in this change.
- [x] Published-root retirement holds a reclamation-domain pin from before the
  root swap through `retire`, satisfying the dependency's documented caller
  contract. The unlink-to-retire fence is retained. This is source-reviewed;
  the focused ps-reclaim Loom gate passed for the matching domain protocol.

See [the security review plan](plans/security-review-2026-11-05.md) for the
dispositions, scope limits, and validation details.
