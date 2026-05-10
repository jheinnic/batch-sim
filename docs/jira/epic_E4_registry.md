# BSIM-E4 — Instance Registry

---

## BSIM-17 — EC2 instance type definitions

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-2

**Description:**
Define the menu of EC2 instance types available to schedulers across three families:
general purpose (m7i), memory-optimized (r7i), and compute-optimized (c7i).
At least three sizes per family. Prices reflect us-east-1 on-demand rates.

**Acceptance Criteria:**
- 15 instance types defined (5 per family) in `configs/instance_registry.yaml`
- Each type has: name, family, ram_gb, vcpu, hourly_price_usd
- `InstanceRegistry.cheapest_fitting(min_ram_gb, min_vcpu)` returns lowest-cost fitting type
- `InstanceRegistry.candidates(min_ram_gb, min_vcpu)` returns all fitting types sorted by price
- Edge cases tested: no fit, exact fit, multiple fits

---

## BSIM-18 — Tier-local MM and K8S effective capacity calculator

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-17

**Description:**
For each instance type, compute the tier-local MM (largest job peak RAM that physically
fits on the node), derive spike headroom (0.92 × MM), and report the effective
schedulable RAM for K8S bin-packing.

**Formulas:**
- tier_local_MM = max peak RAM among all centroid peaks that fit within (node_RAM - os_overhead)
- spike_headroom = 0.92 × MM
- effective_schedulable = node_RAM - os_overhead - spike_headroom
- soft_limit = 0.08 × MM
- max_schedulable_jobs = floor(effective_schedulable / soft_limit)

**Acceptance Criteria:**
- `compute_k8s_capacity(instance, centroid_peak_rams, os_overhead_gb)` returns K8SCapacityProfile
- Worked example verified: 128 GB node, 64 GB peak → headroom ~46%, ~13 jobs vs 2 for Batch
- Jobs too large for a node tier excluded from that tier's MM calculation
- `batch_max_jobs(instance, peak_ram_gb, declared_vcpu)` returns floor by both RAM and CPU

---

## BSIM-19 — Cost accrual model

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-17, BSIM-14 (engine lifecycle)

**Description:**
Implement per-node cost accrual from LAUNCHING through TERMINATED. Cost is billed
in whole seconds (pro-rated from hourly rate). Aggregate pool cost is the sum across
all nodes, with a time-series sampled every 60 simulated seconds.

**Acceptance Criteria:**
- `NodeCostAccruer` tracks launch_time and termination_time per node
- cost = (termination_time - launch_time) / 3600 × hourly_price_usd
- `PoolCostSummary.from_accruers()` produces total cost, cost by family, cost-over-time series
- Unit test: single node, 1 hour, known price → exact expected cost
