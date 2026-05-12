# BSIM-68 — Burst arrival model: runs of N same-type jobs at deploy time

**Type:** Enhancement | **Priority:** High | **Status:** To Do
**Epic:** BSIM-E11 (workload fidelity)

**Description:**
The current generator models individual job arrivals via a Poisson process
(exponential inter-arrival gaps). Real workloads arrive in bursts: at deploy
time a list of N jobs of the same type is submitted together, all arriving
at the same simulated time. N is drawn uniformly from [burst_size_min,
burst_size_max] per burst event.

**Schema additions to CentroidConfig (all optional, backward-compatible):**
```yaml
burst_size_min: 1          # default=1 → existing single-job Poisson behaviour
burst_size_max: 1          # default=1 → same
# Set min=3, max=18 for the real-workload burst model
```

When burst_size_min == burst_size_max == 1, behaviour is identical to current.

**Arrival rate interpretation:**
arrival_rate_per_hour governs BURST events per hour (not individual jobs).
Total job volume = arrival_rate_per_hour × mean(burst_size) × horizon_hours.
This keeps the rate parameter meaningful at the centroid level and lets the
user control throughput by adjusting arrival_rate_per_hour independently of
burst size.

**Generator change (generate_arrivals in event_list.py):**
For each burst event time t drawn from the Poisson process:
  N = rng.integers(burst_size_min, burst_size_max + 1)
  emit N JobArrivalEvent records all with arrival_time = t

**Acceptance Criteria:**
- burst_size_min=1, burst_size_max=1: event list identical to current output
  (same seed, same config → same jobs, verified by test)
- burst_size_min=3, burst_size_max=18: inspect_workload shows multiple jobs
  with identical arrival_time within each centroid
- EventList.metadata records burst_size_min, burst_size_max per centroid
- inspect_workload.py shows burst size distribution in its output
- Unit test: centroid with burst 3-18, 100 burst events → mean N ≈ 10.5
