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
