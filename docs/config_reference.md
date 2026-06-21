# Scheduler config reference

BSIM-109/E21 split the single flat `SchedulerConfig` into a Pydantic discriminated
union over three schemas, defined in
[`batch_sim/core/schemas.py`](../batch_sim/core/schemas.py). `load_scheduler_config`
returns the concrete subclass for a given YAML file — its type *is* the scheduler
(BSIM-123), so there is no separate `--scheduler` argument and a config can no
longer carry a field its scheduler doesn't read.

```
BaseSchedulerConfig            # cross-cutting: panic/SLA/warmup/retry timings,
│                               # scale-out polling, storage cost model
├── BatchConfig                # + allowed_instance_types (BSIM-115)
└── K8SConfig                  # + os_overhead_gb, time_window_policy, tiers
    └── K8SPlusConfig          # + provisioner (Karpenter-style)
```

A new field goes on the narrowest class that consumes it — `BaseSchedulerConfig` only
if every scheduler reads it. `extra="forbid"` makes a misplaced field (e.g. `tiers` on
a `BatchConfig`) a load-time `ValidationError`, not a silent no-op.

## BatchConfig

AWS Batch has no K8S/tier concepts. `allowed_instance_types` (optional) scopes
`BatchScheduler`'s instance selection to a named subset of the registry; `None`
searches the whole registry (the original behaviour).

## K8SConfig

Adds `os_overhead_gb`, the legacy `time_window_policy` calendar-based queue model, and
`tiers` — the BSIM-104 tier-compatibility registry (see
[epic E20](jira/epic_E20_multi_tier_boost_provisioning.md)).

**Caveat (E20): Batch ignores tiers entirely, including their admission control.**
`compatible_tiers` on a centroid and the BSIM-108 admission check (`min_spike` vs. a
tier's `spike_max_gb`) are read only by the K8S/K8S+ schedulers. A job that would be
`ADMISSION_REJECTED` against every declared tier still runs normally under
`BatchConfig` — Batch has no field to even express the constraint, so there is nothing
for it to reject.

## K8SPlusConfig

Extends `K8SConfig` with `provisioner` (Karpenter-style workload-reactive scale-out and
TTL-based lifecycle). When `tiers` is non-empty, scale-out routes through the joint
tier provisioner instead of `provisioner.allowed_instance_types` — see BSIM-112's
load-time warning for that combination.
