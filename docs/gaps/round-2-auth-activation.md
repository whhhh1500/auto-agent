# Round 2 - Authentication Activation

## Scope

Account identity, pending-token authorization, activation, password bounds,
console gating, account administration, and OpenAPI parity.

## Findings

1. A platform administrator could set a pending account directly to `active`,
   bypassing the required password change.
2. Storage callers could create contradictory status/change-password states.
3. Password minimums mixed byte and character counts; bcrypt's 72-byte maximum
   was not enforced before hashing.
4. Ordinary users could list tenant accounts and saw the console account menu.
5. Account and activation payloads were incompletely described by OpenAPI.

## Fixes

1. Pending accounts reject generic status changes and activate only through the
   transactional password path.
2. Account creation enforces the pending/change-password invariant; auth gates
   fail closed on either marker.
3. Activation uses Unicode character minimums and all passwords enforce the
   bcrypt byte maximum.
4. Account listing now requires tenant-operator authority; console navigation
   matches it.
5. Added explicit account, activation, password, status, and list schemas plus
   bypass and ordinary-user denial tests.

## Verification Evidence

- `go test ./pkg/server ./pkg/storage ./pkg/console ./scripts/verify-openapi -count=1` passed.
- `go vet ./pkg/server ./pkg/storage ./pkg/console` passed.
- OpenAPI verifier passed with 93 registered `/v1` operations.

## Remaining Risks

Rendered browser interaction and full process restart behavior remain for round
3 runtime verification.

## Conclusion

Authentication, activation, console, and API closure passed after repairs.
