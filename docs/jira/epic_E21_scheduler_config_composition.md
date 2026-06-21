# BSIM-E21 — Per-Scheduler Config Schemas

Splits the single, unified `SchedulerConfig` into a discriminated union of per-scheduler
schemas — `BatchConfig` / `K8SConfig` / `K8SPlusConfig` over a shared
`BaseSchedulerConfig` — so each scheduler's config carries *only* the fields it consumes,
and the scheduler a config targets is intrinsic to the config rather than a separate
argument.

## Problem

Today one flat `SchedulerConfig` holds every field for every scheduler. Two consequences:

1. **Silent field bleed.** A Batch config can carry `k8s_os_overhead_gb`, `tiers`,
   `provisioner` — all silently ignored. Nothing in the type tells an author what a given
   scheduler honours. (Confirmed by a read-site audit — see the support matrix below.)

2. **Redundant, drift-prone scheduler argument.** The config already has a
   `scheduler_type` field, but the CLI *also* requires `--scheduler`, and `simulate`
   trusts the flag while ignoring the config:
   ```python
   cfg = load_scheduler_config(scheduler_config)          # cfg.scheduler_type exists…
   run_one(..., scheduler_type=SchedulerType(scheduler))  # …but the --scheduler flag wins
   ```
   This is not hypothetical: a batch of runs labelled `so*-k8splus` were actually produced
   by the plain K8S scheduler because `--scheduler k8s` overrode a `k8splus` config, and
   the mislabel went unnoticed until crash counts were eyeballed.

**Prior art.** BSIM-E17 already implemented exactly this separation — a standalone
`K8SPlusSchedulerConfig` (with `pools`, `daemonset_headroom_gb`, `age_cordon_s`) distinct
from the Batch/K8S configs. That work was merged to `origin/main` but bypassed when later
development branched from a pre-E17 base and collapsed back to the unified config. This
epic reclaims that boundary, generalised to all three schedulers, and the E17 schemas on
`origin/main` are a reference.

### Support matrix (the field-assignment spec)

| Field | Batch | K8S | K8S+ | Home |
|---|:---:|:---:|:---:|---|
| `scheduler_type` (discriminator), `panic_threshold_seconds`, `sla_target_seconds`, `warmup_delay_seconds`, `max_retries`, `replay_delay_seconds`, `idle_timeout_seconds`, `scale_out_threshold_s`, `scale_out_poll_s`, `storage` | ✓ | ✓ | ✓ | `BaseSchedulerConfig` |
| `allowed_instance_types` | ✓ | — | — | `BatchConfig` |
| `k8s_os_overhead_gb`, `time_window_policy`, `tiers` | — | ✓ | ✓ | `K8SConfig` |
| `provisioner` | — | — | ✓ | `K8SPlusConfig` |
| `idle_check_interval_seconds` | — | — | — | **dead — removed (BSIM-110)** |

## Solution

```
BaseSchedulerConfig            # cross-cutting fields; scheduler_type discriminator
├── BatchConfig                # + allowed_instance_types
└── K8SConfig                  # + k8s_os_overhead_gb, time_window_policy, tiers
    └── K8SPlusConfig          # + provisioner
```

`SchedulerConfig` becomes a Pydantic **discriminated union** on `scheduler_type`
(`Literal[...]` per subclass), so `load_scheduler_config` returns the concrete subclass —
its type *is* the scheduler. The CLI `--scheduler` flag is removed; the type is derived
from the loaded config. A Batch config carrying a K8S field is now a hard validation error
(unknown field), not a silent no-op. Configs and fixtures hard-migrate to the new shape;
no flat→nested back-compat shim.

Depends on: BSIM-E20 (tier model lives on `K8SConfig`); references BSIM-E17 (prior art).

---

## BSIM-109 — Discriminated-union per-scheduler config schemas

**Type:** Task | **Priority:** High | **Status:** Done

**Description:**
Introduce `BaseSchedulerConfig` holding the cross-cutting fields, and the subclasses
`BatchConfig`, `K8SConfig`, `K8SPlusConfig` per the support matrix (inheritance:
`K8SPlusConfig(K8SConfig)` adds `provisioner`; `K8SConfig` adds `os_overhead_gb` /
`time_window_policy` / `tiers`; `BatchConfig` adds `allowed_instance_types` per BSIM-115).
Each subclass pins `scheduler_type: Literal[...]`. Expose
`SchedulerConfig = Annotated[Union[...], Field(discriminator="scheduler_type")]` and have
`load_scheduler_config` return the union (concrete subclass). Update every read-site to
the typed attribute paths; Batch code must not reference K8S attributes (and now can't —
they don't exist on `BatchConfig`). Hard-migrate all config files and conftest fixtures.

**Acceptance Criteria:**
- `BaseSchedulerConfig` + `BatchConfig` / `K8SConfig` / `K8SPlusConfig` with field homes
  per the support matrix; `scheduler_type` is a per-subclass `Literal`
- `load_scheduler_config` returns the discriminated subclass; the BSIM-104 tier
  cross-reference validator still works on `K8SConfig`/`K8SPlusConfig`
- A Batch config containing a K8S field (`tiers`, `k8s_os_overhead_gb`, …) raises
  `ValidationError`
- All schedulers read their typed config; no scheduler reads another's fields
- All config files + conftest fixtures migrated; no flat-`SchedulerConfig` left
- Full test suite green

---

## BSIM-123 — Derive scheduler type from config; remove the `--scheduler` flag

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-109 (delivers it structurally; the fix itself is also viable on the
current unified `scheduler_type` if landed first)

**Description:**
Make the loaded scheduler config the single source of truth for which scheduler runs.
Remove `--scheduler` from `simulate`, `compare`, and `scripts/generate_node_timelines.py`;
derive the type from the config (`cfg.scheduler_type`, or the discriminated subclass after
BSIM-109). This eliminates the redundancy that produced the `so*-k8splus` mislabel — the
name and the run can no longer disagree because there is only one input.

**Acceptance Criteria:**
- `simulate` / `compare` / `generate_node_timelines` no longer accept `--scheduler`
- The scheduler is taken from the config; running a `k8splus` config runs K8S+ (the so4
  drift is structurally impossible)
- `run_one` is called with the type derived from the config, not a separate argument
- Help text / docs updated; a test asserts the derived type matches the config
- Any wrapper/script passing `--scheduler` is updated

---

## BSIM-115 — `allowed_instance_types` on `BatchConfig`

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-109

**Description:**
Batch has no way to restrict its instance set — `cheapest_fitting` searches the whole
registry, so scoping it to a subset requires forking the registry. Add
`allowed_instance_types: list[str] | None` to `BatchConfig` (None = whole registry,
today's behaviour) scoping `BatchScheduler.cheapest_fitting`.

Deliberately Batch-only: the K8S side derives its instance set differently — the K8S+
`provisioner.allowed_instance_types` applies only when no tiers are defined, and in tier
mode the effective set is the union of *referenced* tiers' `spawn_instance_class`. A shared
field would misrepresent that asymmetry.

**Acceptance Criteria:**
- `BatchConfig.allowed_instance_types` scopes Batch instance selection; None = whole
  registry (current behaviour preserved)
- A restricted run never launches an excluded type even when it is the cheapest fit; falls
  to the cheapest *allowed* fit, or no-fit if none qualifies
- The field exists only on `BatchConfig`
- Tests cover restricted and unrestricted paths

---

## BSIM-110 — Remove dead `idle_check_interval_seconds`

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-109

**Description:**
No scheduler reads `idle_check_interval_seconds` (read-site audit). It simply does not
appear on any of the new config classes. Strip it from every config file and fixture that
sets it.

**Acceptance Criteria:**
- Field absent from `BaseSchedulerConfig` and all subclasses
- Removed from every config file and conftest fixture that set it
- A config still setting it raises `ValidationError` (unknown field) — verified by test
- Full test suite green

---

## BSIM-112 — Warn when `allowed_instance_types` is set alongside `tiers`

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-109

**Description:**
On `K8SPlusConfig`, `provisioner.allowed_instance_types` has no effect on instance
selection when `tiers` is non-empty — scale-out runs through the joint tier provisioner,
which picks instances from each tier's `spawn_instance_class`. The field is silently inert,
a foot-gun for anyone who curates it. Emit a load-time warning when both are set; do not
reject (the provisioner still drives scale-in lifecycle), consistent with deprecation-style
warnings.

**Acceptance Criteria:**
- A `K8SPlusConfig` with non-empty `tiers` and a `provisioner` whose
  `allowed_instance_types` is non-empty warns at load time
- The warning states the field is ignored for selection while the provisioner still governs
  scale-in lifecycle
- No warning when only one is set; config is not rejected
- A test asserts the warning fires (and stays silent in single-feature cases)

---

## BSIM-114 — Preflight tier-compatibility validation (centroids ↔ scheduler tiers)

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-104 (E20 tier model); BSIM-109

**Description:**
`compatible_tiers` on a centroid references tier names defined in a *separate* scheduler
config file, so Pydantic can't validate the reference at load time — a typo'd or
physically-impossible tier name surfaces only at run time. Hand-maintaining long
semicolon-delimited tier strings across two files produced, in one editing session, three
error classes: missing tier names, a `162`/`192` naming drift, and c-family tiers whose
`spike_max_gb` exceeded the node's RAM. Add a `validate_config_pair(sim_config,
scheduler_config)` preflight (invoked by the runner / orchestrator), asserting:

1. **Reference integrity (error)** — every tier named in any centroid `compatible_tiers`
   and `TimeWindowOverride.compatible_tiers` exists in the scheduler's `tiers`.
2. **Physical validity (error)** — every `TierProfile` has
   `spike_max_gb < instance.ram_gb - os_overhead`, leaving a positive schedulable zone.
   (May also run as a standalone `K8SConfig` validator; needs only the registry.)
3. **Burst reachability (warning)** — for each centroid bin, at least one listed tier can
   host the bin's `min_spike`, so no bin is dead-on-arrival.

**Acceptance Criteria:**
- Rejects a centroid `compatible_tiers` naming an undeclared tier (names centroid + bin)
- Rejects a `TierProfile` whose `spike_max_gb >= instance.ram_gb − os_overhead`
- Warns when a centroid bin has no burst-viable tier among its listed tiers
- Passes cleanly for the corrected `jch_centroids_v04B.yaml` × `demo_k8splus_schedulerC.yaml`
  pair (regression fixture: 12/12 bins, 36 tiers)
- The runner / orchestrator calls the check before simulating; failure aborts clearly

---

## BSIM-111 — Document the per-scheduler config model

**Type:** Task | **Priority:** Low | **Status:** Done
**Depends on:** BSIM-109, BSIM-110

**Description:**
With per-scheduler schemas, the "what does each scheduler read" question is answered by
the type hierarchy itself — so this shrinks to a short orientation: a class-level docstring
on `BaseSchedulerConfig` summarising the cross-cutting/per-scheduler split, and a brief
`docs/` config-reference pointing at the three subclasses plus the "Batch ignores tiers,
including their admission control" caveat from E20. No standalone support-matrix table to
maintain — the schema is the matrix.

**Acceptance Criteria:**
- `BaseSchedulerConfig` (and subclasses) carry docstrings describing their field scope
- A short `docs/` config reference names the three schemas and links the E20 caveat
- The doc notes that a new scheduler field goes on the class that consumes it
