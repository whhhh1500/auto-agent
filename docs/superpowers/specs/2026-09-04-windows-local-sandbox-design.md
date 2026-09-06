# Historical record — superseded

This design is retained for historical context only. It is not a current
runtime or deployment specification and contains no operational procedure. The
current Windows Basic contract is documented in [`docs/runner.md`](../../runner.md),
[`docs/deployment.md`](../../deployment.md), and the
[current-user network ADR](../../decisions/2026-09-05-windows-current-user-basic-network.md).

The superseded design investigated a dedicated account, NTFS ACLs, restricted
tokens, Job Objects, Secondary Logon, a broker/helper boundary, and WFP
network rules. Those choices were not carried forward as the current Basic
runtime. Linux's existing bwrap/prlimit path is independent of this historical
record. Any old setup, repair, uninstall, elevation, or acceptance steps that
were once described here are intentionally omitted; they are not supported
operational guidance.