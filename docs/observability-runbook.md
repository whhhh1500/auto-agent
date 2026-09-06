# Operational metrics and alerts

Deployers choose thresholds based on their SLO and lease duration. Alert on a
non-zero sustained `harness.queue.oldest.age`, an increasing
`harness.queue.recoveries` terminal outcome, pending approvals older than the
approval policy, and run/tool failure counters divided by their corresponding
call counters. Metrics use only bounded status/outcome/capability dimensions;
never add tenant, task, run, request, or payload labels. Investigate queue age
by checking worker availability and leases first; investigate approvals through
the approval inbox; investigate error rate through traces and redacted logs.
