# BSIM-E23 — Declarative Experiment Orchestration

Replaces `reproduce_all.sh` — a stale, hand-wired shell script — with a declarative run
manifest and an idempotent orchestrator. The manifest names the combinatorial pieces
once (workloads, schedulers) and derives everything else, eliminating the brittle manual
chaining of generate-output → simulate-input → diagram-event-log that currently lets a
path mismatch silently produce wrong charts.

## Motivation

Running a comparison today means manually ensuring that:
- the `generate` command's output is the events file fed to each `simulate` run, and
- each node-timeline diagram is built from the `*_events.json` that matches *that*
  workload × scheduler combination.

`reproduce_all.sh` encodes this by hand across six runs, with repetition and no guard
against drift. Two of its runs are already dead (E22), its reference workloads use the
obsolete Pareto generation, and Run-04 infinite-spins. Rather than mechanically port it,
this epic first **rethinks what a batch series must produce** to support a defensible
comparative conclusion, then builds the orchestration to produce exactly that.

The naming convention is the crux: results are keyed by a `<workload>-<scheduler>` token
pair, with `_events` / `-nodes` suffixes derived automatically, so the token pairing
becomes the single source of truth and the suffixes cannot drift.

Depends on: BSIM-E22 (the manifest must not be born referencing dead schedulers).

---

## BSIM-118 — Spike: define the comparison and its required artifacts

**Type:** Spike | **Priority:** High | **Status:** To Do

**Description:**
Before building orchestration, decide what a batch series is *for*. Enumerate the claim a
comparison is meant to support (e.g. "K8S+ with tiered boost provisioning costs less than
Batch at equivalent SLA on realistic heavy-tailed workloads") and, working backward, the
raw artifacts that constitute the evidence: which workloads (and why those), which
scheduler configs, which scorecards / aggregates / charts, and what axes vary (scheduler,
panic threshold, tier configuration, workload). Identify what `reproduce_all.sh` produced
that is worth keeping versus what was noise.

Output: a short design note in `docs/` that the manifest schema (BSIM-119) and the
canonical manifest (BSIM-121) are built to satisfy. No code.

**Provenance requirement.** A real incident motivates this: a batch of runs labelled
`so*-k8splus` were in fact produced by the plain K8S scheduler (the `simulate` command
omitted the `plus`), and the mislabel went unnoticed until crash counts were eyeballed —
K8S crashes-and-retries by design, K8S+ cannot emit a `burst_collision` at all. Nothing
tied the output name to the scheduler that produced it. So treat run provenance as a
first-class artifact: every output (scorecard, event log, diagram set) must record the
scheduler type, scheduler config, workload, and seed that produced it, so a name/run
mismatch is detectable (ideally impossible) rather than silent.

**Acceptance Criteria:**
- A `docs/` note states the comparative claim(s) and the variables that must be swept
- Lists the required output artifacts per run (event log, scorecard, node timelines,
  any cross-run aggregate/comparison) and the directory layout
- Names the canonical workloads + scheduler configs the reproduction will use (current,
  not the obsolete Pareto `reference_4h_v*`)
- Specifies the provenance recorded with each run (scheduler type/config, workload, seed)
  and how the orchestrator detects a name↔run mismatch
- Explicitly lists what from the old `reproduce_all.sh` is dropped and why
- Reviewed/confirmed before BSIM-119 begins

---

## BSIM-119 — `ExperimentManifest` schema

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-118

**Description:**
A new Pydantic `ExperimentManifest` model (distinct from the narrow existing
`ExperimentConfig`) that names the combinatorial inputs:

```yaml
workloads:                       # name → generate-input tuple
  m20:   { config: configs/jch_centroids_v01.yaml, output: workloads/m20.json, seed: 8175 }
schedulers:                      # name → (scheduler config, type)
  k8splus: { config: configs/jch_k8splus_scheduler.yaml, type: k8splus }
  batch:   { config: configs/jch_batch_scheduler.yaml,   type: batch }
run:                             # grid to execute
  workloads:  [m20]
  schedulers: [k8splus, batch]
```

The schema owns the **output-naming convention**: a run is keyed `<workload>-<scheduler>`,
yielding `results/<set>/<workload>-<scheduler>_events.json` and
`…-<scheduler>-nodes/` deterministically. Validate that every name in `run` resolves to a
declared workload/scheduler, and (tie-in to BSIM-114) that scheduler tier references are
sound.

**Acceptance Criteria:**
- `ExperimentManifest` validates workload map, scheduler map, and run grid
- A `run` entry referencing an undeclared workload or scheduler name is rejected
- The schema exposes a single helper that, given a (workload, scheduler) pair, returns the
  canonical events path, node-diagram dir, and scorecard path
- Round-trips through YAML; documented with a worked example
- Unit tests cover name resolution and path derivation

---

## BSIM-120 — Idempotent orchestrator command

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-119

**Description:**
A command (`batch_sim orchestrate <manifest>`) that executes the manifest grid
idempotently:
1. **Generate** — for each named workload, produce its events file if absent (skip if
   present and inputs unchanged).
2. **Simulate** — for each workload × scheduler in `run`, produce the scorecard + event
   log if absent; skip if the event log already exists unless `--force`.
3. **Render** — for each combo, generate node-timeline diagrams from the *matching* event
   log, paths derived from the token pair (no manual wiring).

Each phase reports what it did vs skipped. Selective execution flags
(`--only generate|simulate|render`, `--workload`, `--scheduler`) allow partial runs.

**Acceptance Criteria:**
- A clean run produces all events, scorecards, and diagrams for the grid
- A second run with no changes is a near-no-op (everything skipped) and says so
- `--force` re-simulates; `--only` / `--workload` / `--scheduler` scope execution
- Diagrams are always built from the event log matching their (workload, scheduler) pair
  — a mismatch is impossible by construction
- Aborts with a clear message if a referenced config/workload is missing or invalid
  (uses the BSIM-114 preflight before simulating)

---

## BSIM-121 — Retire `reproduce_all.sh`; ship the canonical manifest

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-118, BSIM-119, BSIM-120

**Description:**
Delete `scripts/reproduce_all.sh` and replace it with a committed canonical manifest that
reproduces the artifact set defined in BSIM-118, driven by the single orchestrator
command. Document the one-command reproduction in the README / docs.

**Acceptance Criteria:**
- `scripts/reproduce_all.sh` removed
- A committed manifest (e.g. `configs/reproduce.manifest.yaml`) produces the BSIM-118
  artifact set via `batch_sim orchestrate`
- Docs describe the single command that reproduces all comparison material
- The manifest references only current schedulers/workloads (no dead schedulers, no
  obsolete Pareto `reference_4h_v*`)
- Running the orchestrator on the canonical manifest completes without error
