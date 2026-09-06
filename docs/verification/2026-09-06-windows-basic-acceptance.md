# Windows current-user Basic acceptance

**Date:** 2026-09-06  
**Scope:** the direct, unelevated Windows provider. This is not evidence for the retired dedicated-account/WFP/helper path, elevated callers, network isolation, or a general release claim.

## Runtime boundary

The default Windows provider starts only for an unelevated current-user process. It creates a restricted token with a session capability SID, applies the capability ACL only to a newly created session root and its owned children, and starts the target in a non-breakaway Job with KILL_ON_JOB_CLOSE.

The target receives anonymous standard handles through an exact Windows handle list. The provider binds the Job atomically at process creation, terminates and waits for the Job tree on normal completion, timeout, cancellation, and Close, then drains bounded output. It uses a private desktop in the current window station by default. A trusted process can explicitly set HARNESS_WINDOWS_SANDBOX_PRIVATE_DESKTOP=false for Winsta0\Default compatibility.

The implementation calls ordinary Win32 APIs through golang.org/x/sys/windows, including CreateRestrictedToken, CreateProcessAsUserW, Job Object APIs, and process-thread attributes. It has no .NET, PowerShell 7, Go runtime-side executable, RunAs, helper process, or host-fallback dependency. Missing native Job or process-attribute support fails closed.

NetworkHost is the only advertised network capability and NetworkIsolation=false. Offline proxy and package-manager environment hints are informational; they are not network isolation.

A normal private file outside the session root was denied write access. The acceptance test also verifies that explicitly Everyone- or logon-SID-writable files remain writable, which is the expected WRITE_RESTRICTED exception.

## Native Medium regression evidence

The following command completed successfully in a true Medium child created by the build-tagged development launcher:

    go test -count=1 -tags sandboxacceptance -v -run '^TestAcceptanceCurrentUserLaunchFromMedium$' ./pkg/execution/sandbox

It ran and passed these native cases:

1. Capability-root launch: a restricted target wrote its owned workspace and was non-Full, below High integrity, and without an enabled Administrators group.
2. Post-CreateProcessAsUserW child-handle-close failure: the admitted Job was synchronously terminated and drained.
3. Two sequential public Registry.Start -> Exec -> Close lifecycles.
4. A started target exceeding a five-second execution budget: ErrExecTimeout with TimedOut=true, after Job cleanup.
5. A started target cancelled by its caller: context.Canceled with the bounded stdout result retained after cleanup.

The command passed in 5.17 seconds. The timeout case itself took 5.02 seconds; the cancellation case took 0.06 seconds.

## Full Basic command evidence

An independently built acceptance executable ran from a D-backed owned directory:

- SHA-256: 70E7DBD78FB1D3F98375068710427959934C08AA50C2585698289E07CBA2D54C
- Exit code: 0
- Elapsed time: 5,619 ms
- Standard output: BASIC_UNELEVATED status=passed network=host network_isolation=false offline_hint=informational loopback=reachable desktop_default=true desktop_compatibility=true
- Standard error: empty

The command completed each of these checks before emitting its passed status:

1. Rejects a strict network-isolation request rather than overstating the Basic provider.
2. Writes and reads a file inside the owned workspace.
3. Confirms the target has the same user SID, is restricted, and is not elevated.
4. Denies writing an ordinary outside file while allowing explicitly Everyone- and logon-writable exception files through the exact FILE_WRITE_DATA|SYNCHRONIZE request.
5. Runs System32\cmd.exe /d /c exit 0 through the public Registry/Start/Exec path.
6. Creates a parent loopback listener; the restricted target dials it and writes an exact short marker while offline hints remain present.
7. Runs the default private-desktop path and the explicit trusted Winsta0\Default compatibility path.
8. Enforces a 1 KiB combined output cap against an 8 KiB target write.
9. Terminates a target that sleeps longer than its five-second execution budget.
10. Cancels a target that has spawned a descendant, then proves the recorded PID/creation-time process is gone.
11. Lets a target exit normally after spawning a descendant, then proves that same recorded PID/creation-time process is gone before Session.Close.

The acceptance executable creates a fresh D:\cc\auto_agent\.codex-tmp\harness-windows-sandbox-basic-* fixture root. The candidate-root count matching that prefix was 58 before and after the successful run, for a delta of zero. This is not a claim that the machine has no pre-existing fixture residue. A read-only provenance scan found all 58 matching directories were created and last written before this agent's first owned Basic log at 2026-09-05T15:23:21Z; the newest candidate was last written at 2026-09-05T15:18:54Z.

One pre-log candidate, `harness-windows-sandbox-basic-f2c0408b440c3a3182ff137435037b7d`, was later attributed to an early attempt in this task. It contains only the expected `sandbox\\sessions` empty-directory shape and `outside-current-user.txt`, with no active acceptance-executable process. A final owner-recovery check stopped before mutation: every native sentinel open, including zero desired access and the individual control and cleanup access masks, returned access denied. A separate descriptor query also showed that the sentinel no longer matched the task-created recovery precondition of the current owner and exactly two protected owner ACEs. Without a pre-existing bound handle, the helper could not prove file identity, link count, or digest, so it did not change the DACL or delete any path. The root remains deliberately retained; no broad or recursive removal was attempted. The other 57 candidates remain unowned/untouched by this run. An exact-process scan found zero active processes for the acceptance executable after exit; it does not make a claim about unrelated processes.

Its final stdout and stderr logs are retained in the D-backed build directory used for this run. They are test evidence only and are not part of the repository.

## Source closure and limits

A production-source scan found no imports or references to the retired sandboxrunner, sandboxsetup, sandboxlock, broker, helper-fence, or old basic-command runtime paths. The only retained Medium launcher is internal/sandboxacceptance, guarded by windows && sandboxacceptance, and it is used only for development acceptance tests.

The retired sandboxctl, windows-medium-launcher, and windows-sandbox-basic-smoke development CLIs were removed. Their internal helper, setup, and lock packages were not supported external APIs.

The provider supports one active Windows session per process. It does not provide dedicated-account isolation, WFP network isolation, arbitrary input materialization, administrator-caller execution, elevation, or a compatibility fallback. Those omissions are intentional capability boundaries.
