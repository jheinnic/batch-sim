# BSIM-E16 — Time-Based Scheduling Policy

Extends the K8S scheduler with a structured policy language that varies queue
definitions, memory bands, spawn rates, and drain aggressiveness by time of day.
The goal is to express realistic cluster behavior across off-peak, shoulder, and
peak windows — and ultimately to use the simulator to find the optimal policy
config before propagating it to production.

Depends on: BSIM-E6 (K8S scheduler), BSIM-E11 (two-queue scheduler)

---

## BSIM-83 — Schema: TimeWindowPolicy, QueuePolicy, DrainRule

**Type:** Task | **Priority:** High | **Status:** To Do

**Description:**
Define the policy schema objects in `schemas.py`. A scheduler config contains a
list of `TimeWindowPolicy` entries that partition [00:00, 24:00) with no gaps or
overlaps. Each window contains one or more `QueuePolicy` entries whose memory bands
partition [0, max_workload_memory] with no gaps or overlaps.

```python
class DrainRule(BaseModel):
    idle_vcpu: float          # threshold: node's idle vCPU must exceed this
    duration_s: float         # continuously for this long before DRAINING

class QueuePolicy(BaseModel):
    exclusive_min_gb: float   # jobs with preload > this GB are eligible
    inclusive_max_gb: float   # jobs with preload <= this GB are eligible
    spawn_instance_class: str # instance type to launch for this queue
    spawn_rate_per_min: float # new nodes/min while pods are unschedulable
    drain_rules: list[DrainRule]  # must be monotone: higher idle_vcpu → shorter duration

class TimeWindowPolicy(BaseModel):
    start_time_s: float
    end_time_s: float
    queues: list[QueuePolicy]
```

**Validation rules:**
- Time windows must collectively cover [0, 86400) exactly (no gaps, no overlaps)
- Memory bands per window must cover [0, max_preload_gb] with no gaps or overlaps
- Drain rules per queue must be monotone: higher `idle_vcpu` paired with strictly
  shorter `duration_s`
- `spawn_instance_class` must exist in the instance registry

**Acceptance Criteria:**
- All validation rules enforced with clear error messages naming the failing window
  or queue
- A reference policy YAML for the jch workload passes validation
- Existing single-queue K8S configs (without time windows) continue to work via a
  default single-window policy wrapping the existing fields

---

## BSIM-84 — K8S scheduler: active policy swapping at time boundaries

**Type:** Task | **Priority:** High | **Status:** To Do
**Depends on:** BSIM-83

**Description:**
Wire the time-window policy into `K8SScheduler` so that the active queue set and
spawn parameters update at window boundaries during the simulation.

At sim start, determine the first active `TimeWindowPolicy` from `env.now`. Schedule
a SimPy process that wakes at each boundary time and swaps in the next policy. Jobs
already running are unaffected; the new policy governs only new placements and spawns.

Queue-to-instance mapping changes at a boundary: nodes from the previous policy's
instance class are not forcibly terminated — they drain naturally. New spawns use the
new policy's `spawn_instance_class`.

**Acceptance Criteria:**
- Policy swap fires at the correct sim time (±1s tolerance)
- Jobs scheduled under the old policy run to completion unaffected
- New placements after the boundary use the new window's queue definitions
- Spawn rate and instance class update immediately at the boundary
- Sim log includes a `POLICY_SWAP` event at each boundary with old/new policy IDs
- Test: two-window policy, confirm different instance types spawned in each window

---

## BSIM-91 — Policy: document 24 h cycling and add multi-interval window support

**Type:** Task | **Priority:** Low | **Status:** To Do
**Depends on:** BSIM-83, BSIM-84

**Background:**
Two related gaps surfaced after BSIM-83/84 shipped:

**Gap 1 — undocumented cycling behaviour.**
The scheduler uses `env.now % 86400.0` throughout `_find_window_idx` and
`_policy_timer`.  For simulations longer than 24 hours (e.g. a 48 h stress
test) the policy therefore repeats automatically, which is the correct and
expected behaviour.  However, nothing in the schema docstring, the reference
YAML (`jch_policy_reference.yaml`), or the BSIM-83 acceptance criteria
mentions this.  Authors writing multi-day configs have no way to know the
policy cycles, and a careless reader might assume the last window's drain
rules remain in force for the rest of an extended run.

**Gap 2 — symmetric off-peak periods require repeated definitions.**
The current schema requires every `TimeWindowPolicy` to be a single
`[start_time_s, end_time_s)` span, and all spans must be contiguous with no
gaps.  When off-peak hours fall on both sides of a peak block (e.g. midnight–
8 am and 8 pm–midnight share the same queue config), operators must repeat the
identical queue definition twice:

```yaml
# Today: two definitions, same content
- start_time_s: 0       end_time_s: 28800    # midnight–8am
  queues: [...]                               # copy A
- start_time_s: 72000   end_time_s: 86400    # 8pm–midnight
  queues: [...]                               # copy B  ← identical to A
```

The original design intent was to allow a single window definition to cover
multiple non-contiguous intervals — eliminating copy-paste and the drift risk
when one copy is updated but not the other:

```yaml
# Proposed: one definition, multiple intervals
- intervals:
    - {start_time_s: 0,     end_time_s: 28800}   # midnight–8am
    - {start_time_s: 72000, end_time_s: 86400}   # 8pm–midnight
  queues: [...]
```

**Scope of this story:**

1. **Document cycling** — add a note to `TimeWindowPolicy` docstring and to
   `jch_policy_reference.yaml` / `demo_k8splus_scheduler.yaml` that the policy
   repeats every 86400 s for multi-day simulations.

2. **Multi-interval windows (schema)** — replace the single `start_time_s` /
   `end_time_s` pair on `TimeWindowPolicy` with an `intervals` list of
   `{start_time_s, end_time_s}` pairs.  A window with a single interval is
   unchanged in meaning.  Validation rules update to:
   - All intervals across all windows collectively cover [0, 86400) exactly
   - No overlaps between any two intervals (within or across windows)
   - Each interval must satisfy `end_time_s > start_time_s`

3. **Multi-interval windows (scheduler)** — `_policy_timer` currently walks a
   sorted list of single-boundary events.  Replace with a flat sorted list of
   `(boundary_time, entering_window_or_None, leaving_window_or_None)` events so
   the timer can handle non-contiguous windows correctly.  A gap between
   intervals (e.g. 28800–72000) means no `TimeWindowPolicy` is active; fall
   back to default instance selection for that period.

4. **Backward compatibility** — configs using `start_time_s` / `end_time_s`
   directly continue to parse.  During loading, a `TimeWindowPolicy` that has
   `start_time_s` and `end_time_s` at the top level is silently normalised to
   `intervals: [{start_time_s: ..., end_time_s: ...}]`.

**Acceptance Criteria:**
- Schema docstring and both reference YAMLs note 24 h cycling behaviour
- A config with two windows sharing the same queue definition via `intervals`
  passes validation and produces the same simulation output as the current
  two-entry equivalent
- Existing configs with flat `start_time_s` / `end_time_s` keys continue to
  load without error or deprecation warning
- `_policy_timer` handles a gap interval (no active window) without crashing
- Regression: jch workload with `jch_policy_reference.yaml` produces identical
  metrics before and after schema change

---

## BSIM-85 — K8S scheduler: drain rule enforcement with DRAINING node state

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-84

**Description:**
Implement the DRAINING node lifecycle state and the continuous idle monitoring
required by drain rules.

A node enters DRAINING when any of its queue's drain rules is satisfied: idle vCPU
(= `physical_vcpu - allocated_vcpu`) has continuously exceeded `drain_rule.idle_vcpu`
for `drain_rule.duration_s` seconds. Multiple drain rules per queue are evaluated
independently; the first satisfied rule triggers DRAINING.

DRAINING semantics:
- No new pods accepted (scheduler skips DRAINING nodes during placement)
- Existing jobs run to completion
- Node shuts down when the last job exits (`NODE_TERMINATED` event emitted)

Idle monitoring: a per-node SimPy process checks idle vCPU at each job arrival/departure.
When idle vCPU crosses a threshold, start a timer. If idle vCPU falls below the threshold
before the timer expires, cancel and reset. If the timer fires, transition to DRAINING.

**Acceptance Criteria:**
- DRAINING nodes reject new job placement attempts
- Node shuts down exactly when the last job exits, not before
- Multiple drain rules evaluated: highest `idle_vcpu` threshold with shortest
  `duration_s` fires first when node is very lightly loaded
- Monotonicity violation in drain rules caught at validation time (BSIM-83), not runtime
- `NODE_DRAINING` event emitted when state transitions to DRAINING
- Test: node with two jobs, one finishes, idle vCPU crosses threshold — verify correct
  timer behavior and DRAINING transition
