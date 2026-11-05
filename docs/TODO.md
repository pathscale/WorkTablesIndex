# TODO

## Open review items

- [ ] Define the compatible serde representation for tree invariants, then
  validate deserialized trees before exposing them to concurrent operations.
- [ ] Review registry publish action references, token scope, and release inputs;
  preserve the manual-only workflow trigger.
- [ ] Extend the source audit to iterators, node internals, range/split paths, and
  map operations. Keep static reasoning separate from tests and model evidence.

## Resolved queue findings

- [x] CDC refill of a still-linked empty node now emits removal of the stale
  persisted node followed by creation of the refilled node. This avoids taking
  the global topology write lock for every removal. A focused regression covers
  the event sequence.
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
