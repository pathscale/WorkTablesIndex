# Security review follow-up plan

The prior static review had unverified findings. Its conclusions are leads
for review, not established vulnerabilities. Reassess each item against the
current source and record evidence before making a security claim or changing
code. Detailed exploit narratives are not retained in this public plan.

## Review scope

- Revisit concurrent CDC removal/reinsert event sequences and replace-in-place
  event payloads.
- Review point-reader retry behavior while a generation is odd.
- Check deserialization validation for internal tree invariants.
- Review publish-job credential exposure, dependency locking, and action
  references.
- Reassess the reclamation ordering argument against the current retire
  contract and supported targets.
- Check that readers eventually stop retrying when a structural writer holds
  the generation odd.
- Extend source coverage to iterators, node internals, map operations, range and
  split operations, and the reclamation implementation.
- Review CDC event completeness for concurrent drain/refill and replacement
  operations, plus validation of deserialized tree state.

These are questions for a source review. This plan does not assert that any
reported issue is present, exploitable, or fixed.

## Completion notes

- Record the files and paths reviewed, along with a concise rationale for each
  disposition.
- Keep static reasoning distinct from model, runtime, and build validation.
- If a concrete issue is confirmed, document the minimal reproduction or
  reasoning and route any implementation separately.
- Consider whether consumers deserialize untrusted data, whether concurrent
  CDC writes occur in production, which target architectures are supported,
  and whether trusted publishing is desired.
