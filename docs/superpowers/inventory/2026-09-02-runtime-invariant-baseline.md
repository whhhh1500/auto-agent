# Phase 0 Runtime Invariant Baseline

Date: 2026-09-02  
Scope: public `core.Runtime` and `server` paths, with their durable SQLite
adapters. This is a behavior-preserving regression baseline; it does not
introduce runtime, storage, or wire-contract changes.

## Coverage map

| Invariant | Public-path regression evidence | Durable-adapter evidence |
| --- | --- | --- |
| An approval pauses before the guarded provider call, then approved continuation resumes the same Run exactly once | `pkg/server:TestAsyncApprovalReleasesWorkerAndResumesSameRun`; `pkg/server:TestApprovalDecisionRouteIsIdempotentAndResumesOnlyOnce`; `pkg/core:TestDurableApprovalPausesWithoutTerminalAndResumesSameRun` | `pkg/storage:TestSQLApprovalDecisionAtomicallyResumesPausedRun` |
| A tenant cannot decide another tenant's approval; a cancellation/expiry/revocation cannot bypass continuation state | `pkg/server:TestApprovalDecisionRoutesAreTenantScoped`; `TestWaitingApprovalCancellationClosesControlAndSession`; `TestApprovalExpiryLoopResumesDeniedPath`; `TestApprovedRunRevokedBeforeResumeFailsBothLedgers` | `pkg/storage:TestSQLApprovalExpiryRequeuesRunAsDeniedDecision` |
| The synchronous HTTP Run stream emits durable run and tool events through the SSE writer | `pkg/server_test:TestServerPreservesMultiTurnSessionAndOwnership`; `pkg/server:TestStatusWriterPreservesFlush` | Session event validation and replay are covered by `pkg/core:TestRestoreSessionRejectsCorruptCoreEvents` |
| A Run claim has a monotonic generation fence; an expired old claim cannot renew, retry, or finish the new claim | `pkg/server:TestRunWorkerClaimLossCancelsExecutionWithoutTerminalOverwrite`; `TestRunClaimRecoveryLoopRequeuesExpiredWorkerWithoutRestart` | `pkg/storage:TestSQLRunQueueFencesExpiredWorkerAndDeletesTerminalPayload`; `pkg/extensions/runner:TestMemoryStoreClaimRenewCompleteUsesGenerationFence`; `pkg/storage:TestSQLRunnerStoreRecoveryCancellationAndGeneration` |
| Session lease holder identity is run-scoped; only its owner may renew/release; loss or renewal error cancels the live run | `pkg/server:TestServerLeaseIdentityIsStableRandomUUIDAndRunScoped`; `TestRunLeaseRenewsAndCancelsRunWhenOwnershipIsLost`; `TestRunLeaseRenewalErrorCancelsRun`; `TestRunLeaseConflictDoesNotStartRenewal` | `pkg/storage:TestSessionLeaseAcquireConflictExpireRelease` |
| A completed tool side effect is replayed from the journal; unknown non-idempotent outcomes are fenced; idempotent calls get a stable provider key | `pkg/core:TestToolJournalReplaysCompletedSideEffectAfterSessionWriteFailure`; `TestToolJournalBlocksUnknownNonIdempotentOutcome`; `TestIdempotentManifestPropagatesStableProviderKey`; `TestToolJournalCompletionFailureBecomesPermanentUnknownFence` | `pkg/integration:TestSQLToolJournalPreventsSideEffectReplayAfterLostSessionSuffix`; `pkg/storage:TestSQLToolInvocationJournalFencesReplaysAndConflicts`; `TestSQLToolInvocationJournalRetriesOnlyIdempotentCallsAndKeepsCanonicalResult` |

## New black-box boundary

`TestApprovalDecisionRouteIsIdempotentAndResumesOnlyOnce` exercises the
operator HTTP endpoint twice against one durable approval. It asserts that the
second decision preserves the first durable decision, produces no second
claimable continuation, records exactly one `run/resume`, preserves the
second-generation fence, and invokes the approved provider exactly once.

## Phase status

The critical HTTP/SSE/approval/queue/lease/journal runtime-invariant baseline
is now evidenced by the tests above. Phase 0 subsequently accepted the
separate SQLite historical fixture matrix, recorded PostgreSQL as an explicit
environment skip, and completed the current workspace consumer review. The
skip is not PostgreSQL migration validation. This file does not claim that
later architectural phases are complete.
