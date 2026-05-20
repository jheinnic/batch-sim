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
