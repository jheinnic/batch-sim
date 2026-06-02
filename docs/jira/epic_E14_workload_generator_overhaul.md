# BSIM-E14 — Workload Generator Overhaul: Discrete Size Bins & Time-Varying Arrivals

Replaces the Pareto-multiplier job-sizing model with a discrete size-bin design that
matches how developers actually submit jobs (declared memory tiers, not continuous
multipliers). Adds time-window overrides for arrival rate and bin weights so that
peak/off-peak workload shape differences can be modelled. Also introduces a generator
mode parameter to preserve memoryless Poisson burstiness for production workloads while
keeping deterministic arrival counts for test configs.

Depends on: BSIM-E2 (generator foundation), BSIM-68 (burst arrival model)

---

## BSIM-74 — Generator mode parameter: poisson vs approximate

**Type:** Task | **Priority:** High | **Status:** To Do

**Description:**
Add an `arrival_spacing` field to `CentroidConfig` (and/or `generate_arrivals()`) that
selects the inter-arrival draw strategy:

- `poisson` (default): `rng.expovariate(1/λ)` — memoryless, exact Poisson, preserves
  real clustering behavior. Use for production workload datasets.
- `approximate`: average of 5000 draws (current behavior for tiny/teeny) — reduced
  variance, predictable arrival counts per window. Use for test configs where a known
  number of arrivals is needed to validate scheduler logic.

Both are correct under different requirements; the parameter makes the choice explicit
rather than relying on which code path is active.

**Acceptance Criteria:**
- `arrival_spacing: poisson` produces memoryless exponential inter-arrival draws
- `arrival_spacing: approximate` produces the avg-5000 behavior currently hard-coded
- Default (field absent) is `poisson`
- Existing tiny/teeny configs updated to declare `approximate` explicitly
- Unit test: same seed + `poisson` shows high inter-arrival variance; `approximate` shows
  variance ≈ (1/λ)/√5000

---

## BSIM-75 — Schema: centroid_bin_weights and per-bin sizing arrays

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** none (schema only)

**Description:**
Replace per-centroid `pareto_multiplier_min/max` with a discrete bin model in
`CentroidConfig`. The number of bins is authoritative from `len(centroid_bin_weights)`;
all per-bin arrays must match this length.

**New fields (all optional — absent means fall back to Pareto path):**
```yaml
centroid_bin_weights: [12, 5, 19, 14]     # unnormalized; runtime normalizes to CDF

# Per-bin sizing (one value or [lo, hi] pair per bin):
download_size_gb: [1.0, 2.0, 4.0, 8.0]
upload_size_gb: [0.5, 1.0, 2.0, 4.0]
preprocess_duration_s: [30, 60, 120, 240]
workhorse_stage_duration_s: [300, 600, 1200, 2400]  # scale factor per bin
preloader_hard_limit_gb: [8, 16, 32, 32]
preloader_actual_gb: [[4,7], [9,15], [18,24], [24,30]]   # [lo, hi] uniform draw
steady_state_hard_limit_gb: [4, 8, 16, 16]
steady_state_actual_gb: [[2,4], [5,8], [9,15], [10,15]]
```

Thread counts remain fixed per centroid (not bin-scaled). Memory limits are declared
hard limits as the developer submits them, not Pareto-perturbed actuals.

**Validation:**
- If `centroid_bin_weights` present, all per-bin arrays must have equal length
- `pareto_multiplier_min/max` and `centroid_bin_weights` are mutually exclusive
- Each `[lo, hi]` pair must satisfy lo < hi and lo > 0

**Acceptance Criteria:**
- Existing Pareto configs validate and run unchanged (no `centroid_bin_weights`)
- New bin configs validated: array length consistency, mutual exclusivity
- Pydantic model reflects new fields with correct types and validators
- `inspect_workload.py` shows bin weights and per-bin sizing ranges

---

## BSIM-76 — Generator: CDF-indexed bin sampling replaces Pareto

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-75

**Description:**
Update `generate_arrivals()` (or the sampler) to use bin-indexed sampling when
`centroid_bin_weights` is present:

1. At generator init, normalize `centroid_bin_weights` to a CDF array
2. For each arriving job, draw `U ~ Uniform(0, 1)`, find bin index via CDF lookup
3. Sample each per-bin parameter:
   - Scalar: use value directly
   - `[lo, hi]` pair: draw `rng.uniform(lo, hi)`
4. Pass sampled values to `build_phase_profile()`

Falls back to existing Pareto path when `centroid_bin_weights` is absent.

**Acceptance Criteria:**
- Bin selection frequencies match normalized weights within statistical tolerance
  (chi-squared test, p > 0.05, over 10k samples)
- Per-bin `[lo, hi]` draws are uniform within bounds and never outside
- Pareto path untouched: same seed + Pareto config produces identical output before/after
- Integration test: 4-bin config produces job size distribution matching declared weights

---

## BSIM-77 — Schema: time-window overrides for arrival rate and bin weights

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-75

**Description:**
Add optional `time_windows` list to `CentroidConfig`. Each entry covers a
`[start_time_s, end_time_s)` range and may override `burst_rate` and/or
`centroid_bin_weights` for that window. Unspecified parameters inherit the centroid
baseline.

```yaml
centroids:
  - id: heavy_batch
    burst_rate: 0.5
    centroid_bin_weights: [20, 15, 10, 5]
    time_windows:
      - start_time_s: 36000   # 10AM
        end_time_s:   50400   # 2PM
        burst_rate: 3.0
        centroid_bin_weights: [5, 10, 20, 30]
```

**Validation:**
- Windows must be non-overlapping
- Windows must not extend beyond the simulation horizon
- Gaps between windows are allowed (centroid baseline applies in gaps)
- `centroid_bin_weights` override, if present, must have same length as centroid baseline

**Acceptance Criteria:**
- Pydantic model for `TimeWindowOverride` with correct fields and validators
- Overlap detection raises a clear validation error naming the conflicting windows
- Window with only `burst_rate` override leaves bin weights at baseline
- `inspect_workload.py` shows per-window overrides alongside baseline

---

## BSIM-92 — Schema: discriminated union for Pareto vs bin sizing model

**Type:** Task | **Priority:** Low | **Status:** To Do
**Depends on:** BSIM-75, BSIM-76

**Background:**
After BSIM-75/76 shipped, `CentroidConfig` supports two mutually exclusive
sizing paths — Pareto multiplier and discrete bins — but expresses them as a
single flat struct with all fields present simultaneously.  When `centroid_bin_weights`
is set, the Pareto fields (`pareto_alpha`, `download_gb`,
`preprocess_memory_exponent_a/b`, `preprocess_duration_seconds`, `upload_gb`)
are required by the schema but never read by the sampler.  Authors using the
bin model must supply plausible-looking placeholder values to satisfy the
validator, which is misleading and a maintenance hazard.

A second naming inconsistency: the base centroid field is `arrival_rate_per_hour`
but its per-window override is `burst_rate` on `TimeWindowOverride`.  They are
the same quantity (burst events per hour) and should share a name.

**Design:**

Replace the flat `CentroidConfig` with a discriminated union:

```python
class CentroidBase(BaseModel):
    """Fields shared by both sizing models."""
    id: str
    label: str
    description: str = ""
    sizing_model: Literal["pareto", "bins"]   # discriminator
    arrival_rate_per_hour: PositiveFloat
    burst_size_min: int = 1
    burst_size_max: int = 1
    arrival_spacing: Literal["poisson", "approximate"] = "poisson"
    workhorse_cpu_stages: list[PositiveFloat]
    workhorse_soft_vcpu: list[int] | None = None
    workhorse_hard_vcpu: list[int]
    workhorse_io_wait_per_stage: list[Fraction] | None = None
    io_wait_fraction: Fraction
    time_windows: list[TimeWindowOverride] | None = None

class ParetoCentroidConfig(CentroidBase):
    sizing_model: Literal["pareto"] = "pareto"
    pareto_alpha: PositiveFloat
    pareto_multiplier_min: float = 0.25
    pareto_multiplier_max: float = 4.0
    download_gb: PositiveFloat
    preprocess_memory_exponent_a: PositiveFloat
    preprocess_memory_exponent_b: PositiveFloat
    preprocess_duration_seconds: PositiveFloat
    upload_gb: PositiveFloat

class BinCentroidConfig(CentroidBase):
    sizing_model: Literal["bins"] = "bins"
    centroid_bin_weights: list[PositiveFloat]
    bin_download_gb: list[PositiveFloat] | None = None
    bin_upload_gb: list[PositiveFloat] | None = None
    bin_preprocess_duration_s: list[PositiveFloat] | None = None
    bin_preloader_hard_limit_gb: list[PositiveFloat] | None = None
    bin_preloader_actual_gb: list[list[float]] | None = None
    bin_steady_state_hard_limit_gb: list[PositiveFloat] | None = None
    bin_steady_state_actual_gb: list[list[float]] | None = None
    bin_workhorse_scale: list[PositiveFloat] | None = None

CentroidConfig = Annotated[
    ParetoCentroidConfig | BinCentroidConfig,
    Field(discriminator="sizing_model"),
]
```

Also rename `TimeWindowOverride.burst_rate` → `arrival_rate_per_hour` to match
the base centroid field.

**Backward compatibility:**
- Existing YAML configs omitting `sizing_model` should default to `"pareto"` so
  no existing file breaks.  Pydantic v2 supports a default discriminator value.
- `burst_rate` on `TimeWindowOverride` should be accepted as a deprecated alias
  for one release cycle, with a logged warning on load.

**Migration:**
- All configs under `configs/` updated to declare `sizing_model: bins` or
  `sizing_model: pareto` explicitly.
- `demo_centroids.yaml` updated to drop placeholder Pareto fields from the
  `heavy` centroid.

**Acceptance Criteria:**
- `BinCentroidConfig` rejects `pareto_alpha` and accepts no placeholder Pareto
  fields; validation error names the offending field
- `ParetoCentroidConfig` rejects `centroid_bin_weights`
- Existing configs without `sizing_model` load as `ParetoCentroidConfig`
- `TimeWindowOverride.arrival_rate_per_hour` accepted; `burst_rate` logs a
  deprecation warning and is treated as an alias
- All existing config files in `configs/` validate under the new schema
- Sampler dispatch (`sample_job`) uses `isinstance` check or the discriminator
  field rather than `centroid.centroid_bin_weights is not None`

---

## BSIM-93 — Schema + sampler: per-bin workhorse stage definitions

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-92 (discriminated union)

**Background:**
The original bin model design called for fully independent workhorse stage
definitions per bin — each bin specifying its own stage count, CPU-seconds per
stage, thread ceilings, and I/O wait fractions.  What shipped in BSIM-75/76
was a weaker approximation: a single `bin_workhorse_scale` scalar that
proportionally stretches a shared `workhorse_cpu_stages` template, with thread
counts fixed per centroid and not binnable at all.  This prevents modelling
workloads where different declared memory sizes have meaningfully different
compute shapes (e.g. a small bin with one short parallel stage vs. a large bin
with two long parallel stages with different parallelism ceilings).

**Intended YAML format:**

```yaml
sizing_model: bins
centroid_bin_weights: [3, 2]   # 2 bins

# Per-bin list-of-lists; outer index = bin, inner list = stage definitions
workhorse_cpu_stages:        [[1200, 50], [800, 60, 1400, 40]]
workhorse_soft_vcpu:         [[2],        [4, 4]]
workhorse_hard_vcpu:         [[16],       [8, 16]]
workhorse_io_wait_per_stage: [[0.15],     [0.05, 0.15]]
```

Each inner list follows the existing flat-list convention: `workhorse_cpu_stages`
alternates parallel and sequential stage CPU-seconds; `workhorse_soft_vcpu` and
`workhorse_hard_vcpu` have one entry per parallel stage (i.e. half the length
of the corresponding cpu_stages inner list).

**Schema changes:**

`io_wait_fraction` is eliminated entirely.  `workhorse_io_wait_per_stage`
becomes the single field for I/O wait on both paths, accepting a flexible
nested structure that broadcasts from coarser to finer resolution:

```
Pareto path — Fraction | list[Fraction]
  0.15                     → all stages: 15%
  [0.30, 0.10]             → stage 0: 30%, stage 1: 10%

Bin path — Fraction | list[Fraction | list[Fraction]]
  0.15                     → every stage in every bin: 15%
  [0.15, [0.05, 0.15]]     → bin 0 all stages: 15%;
                              bin 1 stage 0: 5%, stage 1: 15%
  [[0.10, 0.20],            → bin 0 stage 0: 10%, stage 1: 20%;
   [0.05, 0.15]]               bin 1 stage 0: 5%, stage 1: 15%
```

Broadcast rules applied at sample time:
- If the top-level value is a scalar: broadcast to all bins × all stages.
- If the top-level value is a list (length = num bins): resolve each element:
    - Scalar element: broadcast to all stages in that bin.
    - List element: one value per parallel stage in that bin (length must match).

`BinCentroidConfig` workhorse fields (list-of-lists, outer = bin):

```python
workhorse_cpu_stages:        list[list[PositiveFloat]]
workhorse_soft_vcpu:         list[list[int]] | None = None
workhorse_hard_vcpu:         list[list[int]]
workhorse_io_wait_per_stage: Fraction | list[Fraction | list[Fraction]]
```

`CentroidBase` retains flat-list versions of the workhorse fields for the
Pareto path, with `workhorse_io_wait_per_stage` typed as
`Fraction | list[Fraction]`.  `io_wait_fraction` is removed from `CentroidBase`.

**Validation (BinCentroidConfig):**
- Outer length of all per-bin workhorse arrays must equal `len(centroid_bin_weights)`
- For each bin `i`: `len(workhorse_cpu_stages[i])` must be even (alternating
  parallel/sequential pairs)
- For each bin `i`: `len(workhorse_soft_vcpu[i])` and
  `len(workhorse_hard_vcpu[i])` must equal `len(workhorse_cpu_stages[i]) // 2`
- `soft_vcpu[i][j] <= hard_vcpu[i][j]` for all bins `i` and stages `j`
- If `workhorse_io_wait_per_stage` is a list (not scalar): length must equal
  `len(centroid_bin_weights)`; each list element that is itself a list must
  have length equal to `len(workhorse_cpu_stages[i]) // 2` for its bin `i`
- `bin_workhorse_scale` is removed from `BinCentroidConfig`; its presence in a
  config file is a validation error
- `io_wait_fraction` is removed from both config classes; its presence in a
  config file is a validation error

**Sampler changes:**
In `_sample_job_bin_mode`, replace:
```python
scale = _get(centroid.bin_workhorse_scale, 1.0)
cpu_stages = [s * scale for s in centroid.workhorse_cpu_stages]
hard_vcpu = list(centroid.workhorse_hard_vcpu)
```
with:
```python
cpu_stages = centroid.workhorse_cpu_stages[bin_idx]
hard_vcpu  = centroid.workhorse_hard_vcpu[bin_idx]
soft_vcpu  = (centroid.workhorse_soft_vcpu[bin_idx]
              if centroid.workhorse_soft_vcpu else None)
io_waits   = _resolve_io_wait(centroid.workhorse_io_wait_per_stage,
                              bin_idx, n_stages=len(cpu_stages) // 2)
```

Where `_resolve_io_wait` applies the broadcast rules:
```python
def _resolve_io_wait(spec, bin_idx, n_stages):
    if isinstance(spec, float):          # scalar → broadcast everywhere
        return [spec] * n_stages
    per_bin = spec[bin_idx]
    if isinstance(per_bin, float):       # per-bin scalar → broadcast within bin
        return [per_bin] * n_stages
    return list(per_bin)                 # explicit per-stage list
```

**Migration:**
- `demo_centroids.yaml` and any other bin-model configs updated to use
  list-of-lists form for the workhorse fields
- `bin_workhorse_scale` removed from those configs

**Acceptance Criteria:**
- Bin 0 and bin 1 produce jobs with the correct stage count, cpu-seconds,
  thread ceilings, and I/O wait fractions for their respective definitions
- All three `workhorse_io_wait_per_stage` forms (scalar, per-bin scalar list,
  per-bin per-stage list) produce the correct resolved value at sample time
- Validation catches inner-list length mismatches with a message naming the
  bin index and the offending field
- `soft_vcpu > hard_vcpu` rejected at validation time
- Pareto path (`ParetoCentroidConfig`) continues to use flat lists and is
  unaffected by this change; scalar `workhorse_io_wait_per_stage` also accepted
  on the Pareto path and broadcasts to all stages
- `bin_workhorse_scale` in a bin-model config raises a clear validation error
- `io_wait_fraction` in any config raises a clear validation error directing the
  author to use `workhorse_io_wait_per_stage` instead

---

## BSIM-78 — Generator: piecewise-constant Poisson with window boundary crossing

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-74, BSIM-77

**Description:**
Update `generate_arrivals()` to walk forward in sim time respecting time-window
boundaries. At each step, look up the window containing the current time, draw the
next inter-arrival interval, and apply the exact boundary crossing rule:

```
At time t in window ending at W_end with rate λ:
1. Draw τ ~ Exp(λ)
2. If t + τ < W_end: place arrival at t + τ, continue
3. If t + τ ≥ W_end: discard — no arrival in [t, W_end), restart at W_end with next rate
```

This is the exact piecewise-constant Poisson process. Do NOT use fraction-of-interval
acceptance (Δt/τ_drawn) — that overestimates arrival probability.

Bin weight lookup uses the same window resolution as the rate lookup.

**Acceptance Criteria:**
- Single-window workload (no time_windows): output identical to current behavior
  (same seed, same config → same arrival times)
- Two-window workload: no arrivals placed after W_end of first window until W_end
- Arrival counts per window match Poisson(λ·window_duration) distribution
  (KS test, p > 0.05, over 1000 simulated windows)
- Bin weight distribution switches correctly at window boundaries (verified by
  inspecting per-window job size histograms)

---

## BSIM-94 — K8S scheduler: configurable memory reservation fraction below workhorse hard limit

**Type:** Story | **Priority:** Medium | **Status:** To Do

**Background:**
The current baseline uses `bin_steady_state_hard_limit_gb` (the developer-declared
workhorse memory cap) as the K8S `resources.requests.memory` for scheduling placement.
Combined with the preload semaphore, this guarantees zero crash probability: even if
every job draws its maximum steady-state RAM simultaneously, total node consumption
cannot exceed physical capacity.

In practice, actual workhorse RAM draw (`bin_steady_state_actual_gb`) is typically well
below the declared hard limit (e.g. actual P95 ≈ 60-70% of the hard limit). This leaves
substantial schedulable headroom untapped. By reserving less than the full hard limit,
more jobs can be placed on each node, reducing cost — at the price of a small,
configurable crash probability.

**Goal:**
Add a `memory_reservation_fraction` parameter (float, default `1.0`) to
`SchedulerConfig` (and/or per-centroid pool config for K8S+). When less than 1.0, the
scheduler uses `reservation = memory_reservation_fraction × workhorse_hard_limit_gb`
as the memory request for bin-packing purposes, while retaining the full hard limit
for the actual pod spec `limits.memory`.

**Design sketch:**
```yaml
scheduler:
  memory_reservation_fraction: 0.7   # reserve 70% of hard limit → ~40% more jobs/node
```

The K8S+ placement logic in `_place_job` (and `compute_k8s_capacity`) should read this
fraction when computing `effective_schedulable_gb` and per-job RAM reservation. The
workhorse hard limit itself is unchanged in the job spec — only the scheduling signal
is discounted.

**Acceptance Criteria:**
- `memory_reservation_fraction = 1.0` reproduces current (BSIM-94 baseline) behavior
- Metrics include `crash_count` (node OOM events) alongside the usual cost/throughput
- Simulation output clearly annotates what fraction was configured
- At least one reference run at fraction = 0.7 with crash rate vs cost trade-off noted

**Risk:**
At fractions below ~0.6, crash probability rises sharply when actual draw clusters
near the hard limit. A follow-on story should derive the analytically safe fraction from
the `bin_steady_state_actual_gb` distribution (e.g. reserve at P99 of actual draw).
