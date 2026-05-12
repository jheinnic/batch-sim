# BSIM-66 — Per-stage I/O wait fractions

**Type:** Enhancement | **Priority:** High | **Status:** To Do
**Epic:** BSIM-E11 (workload fidelity)

**Description:**
Allow each parallel workhorse stage to declare its own I/O wait fraction,
enabling accurate modelling of CPU-bound vs I/O-bound stage alternation
within a single job's workhorse phase.

**Motivation:**
Real batch jobs typically show a bathtub I/O profile across workhorse stages:
  - Early stages: high I/O wait (reading/decompressing input into memory)
  - Middle stages: low I/O wait (numerical computation on in-memory data)
  - Late stages: higher I/O wait again (writing results)

The current scalar `io_wait_fraction` applies uniformly across all parallel
stages, understating CPU utilisation during compute-heavy middle stages and
overstating it during I/O-heavy boundary stages.

**Schema change (backward-compatible):**
```yaml
# New optional field in CentroidConfig:
workhorse_io_wait_per_stage: [0.40, 0.10]   # one per parallel stage

# Existing scalar still works if per-stage array is absent:
io_wait_fraction: 0.25   # fallback, applied uniformly
```

If both are present, `workhorse_io_wait_per_stage` takes precedence.
If absent, existing behaviour is unchanged.

**Validation:**
- Length must equal `len(workhorse_cpu_stages) // 2`
- Each value in `[0.0, 1.0)`
- Clear error message stating which centroid failed and why

**Sampler change:**
Each per-stage value is independently Pareto-perturbed with the same
Normal(0, 0.05) jitter used for the scalar case, clamped to [0.05, 0.95].
This preserves the statistical variation already present in the scalar path.

**Downstream impact:**
- `Stage.effective_threads` already stores the per-stage value — no change
- Event list format unchanged — stages already serialise effective_threads
- Schedulers unchanged — they see declared_vcpu at job level, not per-stage
- Utilisation charts will naturally reflect per-stage variation since they
  read phase_ram from NodeModel slots which are updated per stage transition

**Acceptance Criteria:**
- Existing configs (scalar io_wait_fraction only) pass validation unchanged
- New per-stage configs validated correctly; length mismatch raises clear error
- Sampled jobs show distinct effective_threads across parallel stages
- `inspect_workload.py` shows per-stage effective thread counts in its output
- Unit test: centroid with [0.80, 0.05] produces effective_threads ≈ [1.6, 7.6]
  for 8-thread stages (heavy I/O first stage, near-full CPU second stage)
