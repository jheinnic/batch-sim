# BSIM-E21 — Scheduler Config Composition

Tidies `SchedulerConfig` so its *shape* reveals which fields each scheduler actually
consumes, and removes one field that no scheduler reads at all.

## Problem

`SchedulerConfig` is a flat bag of ~20 fields where K8S-family-only configuration sits
at the same level as fields every scheduler reads. Nothing in the type tells a config
author (or a new reader) that the Batch scheduler silently ignores, say,
`k8s_os_overhead_gb` while honouring `scale_out_threshold_s`. The `provisioner`,
`storage`, and `tiers` fields are already composed sub-models, so the codebase has
started down the composition path unevenly; the loose K8S scalars never followed.

A read-site audit (which scheduler actually references each field) produced this matrix:

| Field | Batch | K8S | K8S+ | Verdict |
|---|:---:|:---:|:---:|---|
| `scheduler_type`, `panic_threshold_seconds`, `sla_target_seconds`, `warmup_delay_seconds`, `max_retries`, `replay_delay_seconds` | ✓ | ✓ | ✓ | cross-cutting |
| `idle_timeout_seconds` | ✓ | ✓ | ✓ | cross-cutting |
| `scale_out_threshold_s`, `scale_out_poll_s` | ✓ | ✓ | ✓ | cross-cutting (Batch has its own scale-out monitor) |
| `storage` | ✓ | ✓ | ✓ | cross-cutting (already a sub-model) |
| `k8s_os_overhead_gb` | — | ✓ | ✓ | K8S-family |
| `time_window_policy` | — | ✓ | ✓ | K8S-family |
| `tiers` / `queues` | — | ✓ | ✓ | K8S-family (already sub-models) |
| `provisioner` | — | — | ✓ | K8S+-only |
| `idle_check_interval_seconds` | — | — | — | **dead — no scheduler reads it** |

Two findings shaped this epic:

1. **The refactor is small.** Storage, scale-out, and idle-timeout turn out to be
   genuinely shared (Batch has its own scale-out monitor and storage pools), and
   `provisioner` / `storage` / `tiers` are already composed sub-models. The only
   genuinely-K8S-only loose scalar is `k8s_os_overhead_gb`; `time_window_policy` is the
   only other K8S-family field still sitting flat at top level.
2. **One field is dead.** `idle_check_interval_seconds` is declared and set in configs
   and fixtures but read by no scheduler.

## Solution

Group the two loose K8S-family fields under a `k8s: K8SConfig` sub-model
(`k8s.os_overhead_gb`, `k8s.time_window_policy`). Leave the already-typed sub-models
(`tiers`, `provisioner`, `storage`) and all cross-cutting fields at the top level —
this is a light grouping of orphaned scalars, not a wholesale restructure. Migrate all
affected config files and test fixtures directly to the nested shape; no flat→nested
backward-compat shim (the flat keys become unknown fields). Delete the dead
`idle_check_interval_seconds`. Capture the support matrix above in a discoverable,
maintained location so the "what does each scheduler read" question never needs another
grep.

Depends on: BSIM-E20 (tier registry — the `tiers` cross-reference validator must keep
working unchanged)

---

## BSIM-109 — Introduce `K8SConfig` sub-model for K8S-family fields

**Type:** Task | **Priority:** High | **Status:** To Do

**Description:**
Add a `K8SConfig` Pydantic model that holds the two K8S-family-only fields, and relocate
them off the flat `SchedulerConfig`:

```yaml
# before
scheduler_type: k8s
k8s_os_overhead_gb: 2.0
time_window_policy: [...]

# after
scheduler_type: k8s
k8s:
  os_overhead_gb: 2.0
  time_window_policy: [...]
```

`K8SConfig` fields:
- `os_overhead_gb: NonNegativeFloat = 2.0`
- `time_window_policy: list[TimeWindowPolicy] | None = None`

`SchedulerConfig.k8s: K8SConfig | None = None`. The already-typed sub-models (`tiers`,
`provisioner`, `storage`) and every cross-cutting field stay where they are.

Update all read-sites — `k8s_scheduler.py`, `k8s_plus_scheduler.py`,
`k8s_plus_two_queue.py` — from `cfg.k8s_os_overhead_gb` / `cfg.time_window_policy` to
`cfg.k8s.os_overhead_gb` / `cfg.k8s.time_window_policy` (guarding for `cfg.k8s is None`).
The Batch scheduler must not reference `cfg.k8s` at all.

No backward-compat shim: a flat `k8s_os_overhead_gb` or `time_window_policy` key is an
unknown field at the `SchedulerConfig` level. Migrate the 7 config files that set these
keys and the conftest fixtures to the nested shape in the same change.

The BSIM-104 tier cross-reference validator (named-tier references in
`time_window_policy` must exist in `tiers`) must continue to work — it now reads
`self.k8s.time_window_policy` and `self.tiers`.

**Acceptance Criteria:**
- `K8SConfig` model validates `os_overhead_gb` and `time_window_policy`
- `SchedulerConfig.k8s: K8SConfig | None`; the cross-tier reference validator still
  rejects a window referencing an undeclared tier
- All three K8S schedulers read `cfg.k8s.*`; Batch never references `cfg.k8s`
- A scheduler with `cfg.k8s is None` behaves as today's "no policy, default overhead"
- All 7 affected config files and the conftest fixtures migrated to the nested shape
- Full test suite green

---

## BSIM-110 — Remove dead `idle_check_interval_seconds`

**Type:** Task | **Priority:** Medium | **Status:** To Do

**Description:**
No scheduler reads `idle_check_interval_seconds` (confirmed by read-site audit). Remove
the field from `SchedulerConfig` and strip it from every config file and test fixture
that sets it.

**Acceptance Criteria:**
- Field removed from `SchedulerConfig`
- Removed from every config file and conftest fixture that set it
- A config still setting `idle_check_interval_seconds` is handled per Pydantic's default
  for unknown fields; a test documents the resulting behaviour
- Full test suite green

---

## BSIM-111 — Document the scheduler-config support matrix

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-109, BSIM-110

**Description:**
Capture the per-scheduler field support matrix (which of Batch / K8S / K8S+ reads each
`SchedulerConfig` field) in a discoverable, maintained location so config authors can
see at a glance what a given scheduler honours versus ignores. Reflect the post-E21
shape: the `k8s:` sub-model and the removed `idle_check_interval_seconds`.

Target locations:
- The `SchedulerConfig` (and `K8SConfig`) class docstring — the matrix lives next to the
  fields it describes
- A config reference section under `docs/` linked from the configs directory, holding
  the full table plus the "Batch silently ignores K8S/tier fields, including admission
  control" caveat established in BSIM-E20

**Acceptance Criteria:**
- Support matrix present in the `SchedulerConfig` docstring, grouped by verdict
  (cross-cutting / K8S-family / K8S+-only)
- A `docs/` config reference contains the full table and the Batch-ignores-tiers caveat
- Matrix reflects the nested `k8s:` shape and omits the removed dead field
- The doc notes that adding a new `SchedulerConfig` field requires updating the matrix

---

## BSIM-112 — Warn when `allowed_instance_types` is set alongside `tiers`

**Type:** Task | **Priority:** Medium | **Status:** To Do

**Description:**
When both `tiers` and `provisioner` are configured on a K8S+ scheduler, the provisioner's
`allowed_instance_types` has no effect on instance selection: scale-out runs through the
joint tier provisioner, which picks instances solely from each tier's
`spawn_instance_class`. Both read paths for `allowed_instance_types`
(`_select_instance_for_overflow` and the `provisioner` branch of
`_cheapest_fitting_for_job`) are unreachable once `tiers` is non-empty. The field is
silently inert — a foot-gun for anyone who carefully curates it expecting it to constrain
launches.

Emit a load-time warning when `tiers` is non-empty **and**
`provisioner.allowed_instance_types` is non-empty, stating that `allowed_instance_types`
is ignored for instance selection in tier mode (tier `spawn_instance_class` governs), and
that the provisioner still drives scale-in lifecycle (TTLs and consolidation). Do not
reject the config: the provisioner remains meaningful for lifecycle, so this is a warning,
not an error — consistent with the `queues→tiers` deprecation style.

Out of scope: changing selection semantics (tiers correctly own instance choice); this is
purely surfacing the dead-config condition.

**Acceptance Criteria:**
- A `SchedulerConfig` with non-empty `tiers` and a `provisioner` whose
  `allowed_instance_types` is non-empty emits a warning at load time
- The warning names `allowed_instance_types` and states it is ignored for selection while
  the provisioner still governs scale-in lifecycle
- No warning when only one of `tiers` / `provisioner.allowed_instance_types` is set
- Config is not rejected; the provisioner's lifecycle fields remain in effect
- A test asserts the warning fires (and does not fire in the single-feature cases)
