# BSIM-E1 — Project Scaffold & Tooling

---

## BSIM-1 — Initialize repository and package structure

**Type:** Task | **Priority:** Highest | **Status:** Done

**Description:**
Create the Git repository, Python package layout, and dependency manifest. The directory
structure must support clean separation of the generator, scheduler, core simulation,
registry, and metrics layers.

**Acceptance Criteria:**
- Git repo exists at /home/ionadmin/Git/batch-sim with main branch
- `pyproject.toml` defines all dependencies (simpy, numpy, scipy, pandas, matplotlib,
  seaborn, pydantic, rich, pytest)
- Package installs cleanly with `pip install -e .`
- `pytest` runs (zero tests, zero failures)

---

## BSIM-2 — Define configuration schema

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-1

**Description:**
Define and validate all configuration objects using Pydantic. Configuration covers:
centroid definitions, instance type registry, scheduler parameters (panic threshold,
warmup delay, SLA target), and experiment sweep parameters.

**Acceptance Criteria:**
- `CentroidConfig` validates all seven centroid parameters including CPU stage array structure
- `InstanceTypeConfig` covers RAM, vCPU, family, and hourly price
- `SchedulerConfig` covers panic_threshold, warmup_delay_seconds, sla_target_seconds, max_retries
- `ExperimentConfig` covers event_list_path, scheduler_type, and parameter sweep range
- Invalid configs raise clear validation errors with field-level messages
- All models serialize to/from JSON

---

## BSIM-3 — Logging and CLI harness

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-1

**Description:**
Set up a top-level CLI with subcommands for the two pipeline stages: `generate`,
`simulate`, `compare`, `experiment`, and `plot`.

**Acceptance Criteria:**
- `python -m batch_sim generate --config <path> --output <path>` invokes the generator
- `python -m batch_sim simulate --events <path> --scheduler <batch|k8s> ...` invokes scheduler
- `python -m batch_sim experiment ...` runs the full threshold sweep
- `rich` progress display shown during experiment runs

---

## BSIM-96 — Type annotations across batch_sim source

**Type:** Task | **Priority:** Low | **Status:** To Do

**Description:**
Add PEP 484 type annotations to all public function and method signatures in
`batch_sim/` so that PyLance/pyright operates without false-positive noise.

Current state: `collector.py` has zero return-type annotations (20 functions);
`engine.py` has one (23 functions); scheduler files are partial.  `pyproject.toml`
has no `[tool.pyright]` section, so PyLance runs in default mode with no
project-level strictness configured.

Scope:
- `batch_sim/core/engine.py` — `RunningJobSlot` fields (`object` → typed), all function signatures
- `batch_sim/metrics/collector.py` — all factory methods and aggregators
- `batch_sim/scheduler/cpu_boost_integration.py` — parameter and return types
- `batch_sim/scheduler/{batch,k8s,k8s_plus}_scheduler.py` — fill gaps
- `batch_sim/registry/instance_registry.py`, `generator/`, `core/schemas.py` — fill gaps
- `pyproject.toml` — add `[tool.pyright]` in `basic` mode

SimPy objects (`simpy.Environment`, `simpy.Event`, `simpy.Process`) should be typed
directly; generator/coroutine processes typed as `Generator[Any, None, None]`.
`from __future__ import annotations` already present in all non-`__init__` files.

**Out of scope:** `scripts/` directory, strict-mode zero-error target (basic mode only).

**Acceptance Criteria:**
- `[tool.pyright] pythonVersion = "3.11"` present in `pyproject.toml`; mode `basic`
- All functions in `engine.py`, `collector.py`, `cpu_boost_integration.py` have
  return type annotations
- No `object` typed fields on dataclasses where a concrete type is known
- PyLance "Problems" panel shows no `reportMissingParameterType` or
  `reportUnknownVariableType` errors in the files listed above
- No runtime behaviour changes (type-annotation-only diff)

---

## BSIM-4 — Pytest fixtures and test data

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-2

**Description:**
Create shared pytest fixtures covering a minimal valid centroid config, a small
synthetic event list (30-minute horizon, 2 centroids), and a minimal instance registry.

**Acceptance Criteria:**
- Fixtures defined in `tests/conftest.py`
- Synthetic event list covers all four job phases and two centroid types
- All test files import from conftest without duplication
