# BSIM-E3 — Simulation Core

---

## BSIM-11 — SimPy environment wrapper and clock model

**Type:** Task | **Priority:** Highest | **Status:** Done
**Depends on:** BSIM-4

**Description:**
Create the SimPy environment wrapper that drives the simulation clock, ingests job
arrival events from a loaded event list, and dispatches job processes. This is the
engine all scheduler implementations run inside.

**Acceptance Criteria:**
- `SimulationEngine` wraps `simpy.Environment` and exposes `run(event_list, scheduler)`
- Arrival events scheduled at exact arrival_time on the SimPy clock
- Scheduler interface: `scheduler.on_job_arrival(env, job, arrival_time)` called per arrival
- Simulated time unit is seconds throughout

---

## BSIM-12 — Node state model

**Type:** Task | **Priority:** Highest | **Status:** Done
**Depends on:** BSIM-11

**Description:**
Implement the per-node state tracker. A node knows its physical RAM and vCPU capacity,
the current set of jobs running on it, and the instantaneous aggregate resource
consumption at every simulated moment.

**Acceptance Criteria:**
- `NodeModel` tracks physical_ram_gb, physical_vcpu, per-job slots with current phase and resource draw
- `instantaneous_ram_gb()` returns sum of all jobs' current-phase RAM consumption
- `is_overloaded()` returns True when instantaneous_ram > physical_ram_gb
- Phase transitions update resource draw automatically
- `phase2_jobs()` returns jobs currently in pre-process (peak RAM) phase

---

## BSIM-13 — Overload detector and crash-and-replay dispatcher

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-12

**Description:**
When a node's instantaneous RAM exceeds physical capacity, one running job is selected
at random from Phase-2 jobs and killed. The victim is sent to the replay queue with
retry count incremented. After 3 retries the job is recorded as a terminal failure.

**Acceptance Criteria:**
- Overload check fires on every phase transition (only moments resource draw changes)
- Victim selection is uniformly random among Phase-2 jobs; fallback to highest-RAM job
- Killed job emits JobCrashEvent to metrics collector before requeue
- Replay queue re-inserts job with arrival_time = now + replay_delay
- At retry == max_retries, JobTerminalFailureEvent emitted instead
- Two jobs with combined peak ≤ physical RAM → no crash (verified by test)
- Two jobs with combined peak > physical RAM → exactly one crash (verified by test)

---

## BSIM-14 — Instance lifecycle model (warmup and teardown)

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-12

**Description:**
Model the delay between a scheduler's decision to launch a new instance and that
instance becoming available. Track idle time from last job completion to termination.

**Acceptance Criteria:**
- `launch_instance` is a SimPy process that yields for warmup_delay_seconds
- Node transitions: LAUNCHING → READY → IDLE → TERMINATED
- Idle time begins accumulating when last job on a node completes
- Node terminated after idle_timeout_seconds with no new job placed
- Cost accrual starts at LAUNCHING and stops at TERMINATED

---

## BSIM-15 — Priority queue and panic-mode trigger

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-14

**Description:**
Implement the shared job queue with priority elevation. When a job's wait time crosses
the panic threshold, it is elevated to URGENT, a new instance launch is guaranteed if
no slot is currently available, and it cannot be superseded by later arrivals.

**Acceptance Criteria:**
- Queue is a min-heap ordered by (priority, arrival_time, seq)
- Two priority levels: NORMAL and URGENT
- A SimPy process monitors each queued job's age; at panic_threshold → upgrade to URGENT
- `guarantee_capacity()` launched immediately if no node can accommodate the job
- URGENT jobs placed before NORMAL jobs regardless of arrival order
- PanicTriggerEvent emitted to metrics with job_id and wait_time_at_panic

---

## BSIM-16 — Job process and phase execution engine

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-12, BSIM-15

**Description:**
Implement the SimPy process that executes a single job through its four phases
sequentially on an assigned node. Each phase yields for its duration while holding
the appropriate resource profile on the node.

**Acceptance Criteria:**
- `run_job_process(env, job, node, ...)` is a SimPy generator
- Phase transitions update NodeModel at exact simulated times
- Workhorse phase iterates through CPU stage array, yielding per-stage duration
- Job emits JobStartEvent, PhaseTransitionEvent (×3), and JobCompleteEvent
- If interrupted (crash), process terminates and JobCrashEvent is emitted
- Total elapsed time recorded from queue-entry to JobCompleteEvent
