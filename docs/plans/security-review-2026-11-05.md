# Security review follow-up

This records dispositions from the 2026-11-05 source review. Older review
claims were leads, not established vulnerabilities. The focused checks below
cover only named behavior; they are not a full concurrency or security proof.

## Reviewed findings

### Concurrent drain/refill CDC event sequence

The queued fix serialized every removal with the topology write lock. That
would put a global exclusive lock on the ordinary remove path, including
removals that leave their node nonempty. It was not integrated as written.

The intermediate refill event fix was superseded in 0.0.16: a routing pair can
retain an obsolete payload after singleton replacement. Final singleton deletion
now reroutes under topology write, emits the actual deleted member, and unlinks
before refill can proceed. Non-final deletion keeps its ordinary read-lock path.
A deterministic regression checks payload identity, refill exclusion and event
ordering. Legacy split-phase fixtures remain test-only. A downstream cold-reopen
regression covers empty-table storage reuse and both deletion APIs.

### Replace-in-place CDC payload

The replacement path now clones the stored entry while holding the node lock,
adopts its identity into the incoming value, and reports the stored entry in
`RemoveAt`. The prior packet's early `None` return for a missing locked slot
was not retained: the slot was selected under the same lock, so absence is an
invariant violation and should remain explicit. A focused map test replaces a
value while another row keeps the node multi-entry and checks old/new payloads.

### Odd topology generation progress

Both point-read loops now count odd-generation retries before deciding whether
to use the existing topology-lock fallback. Previously they continued before
incrementing retries, so this state could never reach the bounded fallback.
The fallback may still wait on a stalled writer; the change prevents unbounded
reader spin when the writer eventually completes. This change has source review
but no dedicated concurrency regression test yet.

### Published-root retirement contract

`ps-reclaim::Domain::retire` documents that callers invoke it while holding the
guard used to unlink the pointer. The topology writer now holds a publication
domain pin across root replacement and the `retire` call, and preserves a
sequentially consistent fence after publication. This satisfies the dependency
contract at this call site. The current dependency source also has a queued
generic retirement-ordering correction; WorkTablesIndex does not assume that
correction has already been released.

## Open policy and review items

- Derived serde deserialization can accept invalid internal tree layouts. A
  compatible validated encoding is not yet selected; do not silently change
  persisted-format compatibility.
- The publishing workflow's immutable action references, credential scope, and
  locked release inputs still need policy review. Keep `workflow_dispatch` as
  the default and do not run CI on pushes.
- Iterators, node internals, ranges/splits, and broader map paths still need a
  separate source audit. Do not generalize these findings into a full security
  assurance.

## Focused validation

The intended local gates for the current source changes are:

- `cargo fmt --all -- --check`
- `cargo test --offline --features cdc refill_of_drained_node_emits_remove_and_create_for_cdc`
- `cargo test --offline --features cdc replacing_a_value_reports_the_stored_value_in_cdc_removal`

These gates do not exercise the odd-generation fallback. No full suite,
benchmark, or cross-target check is implied by this plan.
