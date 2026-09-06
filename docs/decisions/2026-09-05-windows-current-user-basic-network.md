# ADR: Windows current-user basic network contract

## Status

Accepted for the current-user Basic slice. The real ordinary Medium native
acceptance passed on 2026-09-06; the detailed evidence is recorded in
[`docs/verification/2026-09-06-windows-basic-acceptance.md`](../verification/2026-09-06-windows-basic-acceptance.md).

## Decision

Use the existing `sandbox.NetworkHost` value for the Windows current-user basic
configuration. Its assurance report must expose `NetworkIsolation=false` and
must not list `NetworkDisabled` or `NetworkIsolated` as supported modes unless
the provider can enforce those modes.

The trusted `sandboxexec.Config` selects the mode. Tool and model input cannot
change it. An empty configuration retains the historical
`NetworkDisabled`/isolated-assurance default for compatibility. A strict
disabled or isolated request sent to a Host-only provider fails admission with
an assurance error; it is never silently converted to Host.

The basic acceptance may inject a fixed environment notice saying that the
test is running in an offline environment. That notice is evidence about the
test setup and has no security meaning. The acceptance result must report the
weak Host mode and the absence of a network-isolation guarantee. It may use a
controlled loopback check to demonstrate that Host mode remains connected, but
it must not claim strong IPv4, IPv6, or WFP denial.

`MountsEnforced` describes the fixed workspace/artifacts mount contract. It is
not a universal deny-read or deny-write assertion for every host path. The
current-user WRITE_RESTRICTED token and ACL contract retains its documented
Everyone/logon writable exceptions, so outside-write evidence remains bounded
to the named acceptance fixture.

## Context

The public contract already distinguishes `NetworkHost`, `NetworkDisabled`,
and `NetworkIsolated`, while `Assurance.NetworkIsolation` records whether
isolation is actually guaranteed. The Windows provider previously advertised
only Disabled, and the sandbox execution adapter hardcoded Disabled for every
tool call. That made a current-user weak mode impossible to express without
silently weakening a strict request.

The existing server composition registers one local provider and the
`sandbox.exec` capability. Network choice is therefore a trusted composition
decision, not a model-facing field. The current-user basic path deliberately
does not depend on a sandbox account, WFP objects, administrator installation,
PowerShell 7, .NET, or a runtime Go toolchain.

## Consequences

- Basic Windows server composition can choose Host while other platforms keep
  the empty compatibility configuration and therefore retain Disabled.
- `ExecRequest` accepts the existing legal network values, while session and
  provider admission still enforce exact policy and assurance matching.
- The former dedicated-account/WFP path is archived historical work. It is not
  a prerequisite, runtime dependency, or acceptance result for current-user
  Basic.
- A basic pass requires real evidence for token identity, workspace writes,
  outside-write denial, direct argv execution, output bounds, timeout,
  cancellation, descendant cleanup, and the weak network report. An offline
  notice alone cannot establish isolation.
