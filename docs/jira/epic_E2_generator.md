# BSIM-E2 — Workload Generator

---

## BSIM-5 — Centroid parameter schema and phase model

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-2

**Description:**
Implement the full centroid parameter model. Each centroid defines: download GB,
pre-process memory scaling exponent, workhorse CPU stage array (alternating
parallel/serial durations and thread counts), I/O wait factor, and upload GB.
The phase model derives wall-clock durations and resource profiles from these inputs.

**Acceptance Criteria:**
- `PhaseProfile` dataclass produced per job with per-phase duration and RAM
- Download duration derived from GB and configurable network bandwidth (MB/s)
- Pre-process RAM = `a * download_GB ^ b` (super-linear, b > 1)
- Workhorse alternates parallel (even index) and serial (odd index) stages
- Effective CPU = declared * (1 - io_wait_fraction)
- Steady-state RAM = 8% of Phase 2 peak throughout Phases 3 and 4

---

## BSIM-6 — Pareto distribution sampler

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-5

**Description:**
Implement per-centroid Pareto sampling that varies individual job parameters around
the centroid's nominal values. The Pareto shape parameter (alpha) is a per-centroid
config input. Sampled values remain physically plausible (positive, within bounds).

**Acceptance Criteria:**
- `sample_job(centroid, rng)` returns a `JobSpec` with Pareto-perturbed parameters
- Download GB, RAM coefficient, and stage durations are sampled independently
- CPU stage array structure (length, parallel/serial pattern) is fixed per centroid
- Samples are reproducible given the same random seed
- All sampled values clamped to physically reasonable range [0.25×, 4×] nominal

---

## BSIM-7 — Arrival rate model

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-6

**Description:**
Implement Poisson-process job arrivals per centroid. Each centroid has an independent
arrival rate lambda (jobs/hour). Inter-arrival times drawn from exponential distribution.

**Acceptance Criteria:**
- `generate_arrivals(centroids, horizon_seconds, rng)` returns sorted (time, centroid_id) list
- Multiple centroids interleave correctly in time order
- With seed fixed, output is identical across runs

---

## BSIM-8 — Job spec assembly and event list construction

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-6, BSIM-7

**Description:**
Combine arrival times with sampled job parameters to produce a complete time-ordered
event list. Each event is a fully specified job: arrival time, centroid label, and
complete phase profile.

**Acceptance Criteria:**
- `EventList` is a sorted list of `JobArrivalEvent` objects
- Each event contains: job_id (UUID), arrival_time, centroid_id, and full PhaseProfile fields
- Events strictly sorted by arrival_time

---

## BSIM-9 — Event list serializer / deserializer

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-8

**Description:**
Persist the event list to a self-describing JSON file and reload it losslessly.
The file includes a header block (config metadata, seed, generation timestamp).

**Acceptance Criteria:**
- `save_event_list(event_list, path)` writes JSON with metadata header and event array
- `load_event_list(path)` reconstructs an identical EventList
- Round-trip test: save then load produces identical job specs

---

## BSIM-10 — Generator CLI integration and smoke test

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-3, BSIM-9

**Description:**
Wire the generator into the `generate` CLI subcommand. Validate end-to-end with
the reference centroid config producing 242 jobs over a 4-hour simulated window.

**Acceptance Criteria:**
- `python -m batch_sim generate --config configs/reference_centroids.yaml --output workloads/reference_4h.json` completes
- Output file passes load_event_list validation
- Summary printed to stdout: total jobs, per-centroid counts
- Reference config committed to `configs/`
