# Internal Validation Contracts

Validate required pointers, ranges, request shape, and configuration once at
the highest external boundary. Internal functions assume their documented
preconditions and must not repeat caller-guaranteed null, range, or identity
checks.

Keep checks only when they represent runtime state or external uncertainty:
allocation failure, resource exhaustion, I/O or publish failure, concurrent or
state transitions, stale or duplicate identities, and wire-data integrity.

Violations of internal preconditions are programming errors, not recoverable
fallback paths. Document strict preconditions in internal headers. Do not add
defensive error returns or fallback branches unless a real caller can receive
and correctly handle that outcome.

When simplifying an existing path, prove the caller contract across every
production call site before removing a check. Preserve checks that enforce
resource ownership, protocol integrity, or state-machine correctness.
