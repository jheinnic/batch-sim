# Node Disruption Detection: Event-Driven Sim vs. Polled Reality

This document records a known divergence between how the simulation decides
to disrupt (drain/terminate) a K8S+ provisioner-managed node and how a real
Karpenter-style controller does it. It is intended to be read independently
of the code when simulation conclusions about node lifecycle / disruption
aggressiveness are questioned, in the same spirit as `CPU_MODELING.md`.

---

## What real Karpenter does

Karpenter's disruption/consolidation logic is not part of `kube-scheduler` —
it runs in a separate controller, on its own reconciliation cadence. To
decide whether a node is eligible for empty-node or underutilization-based
consolidation, that controller inspects node/pod state at discrete points in
time (a watch-triggered reconcile, a scheduled requeue, or both, depending on
implementation details this doc does not claim certainty about).

This means the controller's view of "is this node empty / underutilized" is
a **sampled** view, not a continuous one. Any state transition that starts
and fully reverses between two samples — e.g. a node's job count going
1 → 0 → 1 within a single reconcile gap — is invisible to it. The "node is
empty" condition was never true at any instant the controller actually
inspected the node, so there is nothing for it to have caught. This is not a
delay or a missed deadline; it is a structural blind spot inherent to any
periodically-sampled controller, independent of how short or long the
sampling interval is.

## What the simulation does

The simulation's provisioner lifecycle logic (`k8s_plus_scheduler.py`) is
event-driven and exact:

- `on_job_complete` calls `_update_node_lifecycle`, which starts the
  `empty_ttl_s` timer the instant a node's last job exits.
- `_place_job` calls the same `_update_node_lifecycle`, which cancels that
  timer the instant a new job lands on the node — even if the node had been
  empty for only a simulated instant.

Both calls happen as ordinary synchronous Python within SimPy's
single-threaded event loop; control only passes to another process at a
`yield env.timeout(...)`, and there is none between these two calls. There is
no sampling interval for a transient empty window to fall through — every
job-count transition is observed and reacted to at its exact simulated
timestamp, no matter how brief.

## Bias direction

The simulation is **more reactive than the real system it models** with
respect to transient empty/underutilized windows:

- A node that briefly empties and immediately refills will have its
  `empty_ttl_s` / `underutilize_ttl_s` timers started and cancelled cleanly
  in the simulation, with no effect on its eventual disruption outcome
  either way (the timer cancellation prevents any erroneous termination).
- The open question is the opposite direction: whether a real Karpenter
  deployment's sampling resolution causes it to **miss** some genuinely
  empty/underutilized windows that the simulation's exact event tracking
  would register — i.e., whether the simulation's node count / utilization
  timelines run *colder* (more idle time correctly attributed, nodes
  disrupted closer to their true eligibility) than a real cluster would
  achieve in practice.
- If real Karpenter's disruption controller is less precise here than the
  simulation models it, real-world node-hours (and therefore real-world
  cost) would tend to run **higher** than this simulation predicts for K8S+,
  not lower — because some real nodes that the simulation would have
  disrupted promptly might instead survive a bit longer, having never been
  observed as eligible at a sample point.

**This is a one-directional, not yet quantified, distortion**: it does not
flip which scheduler wins a cost comparison, but it does mean any *absolute*
K8S+ node-hour or cost figure from this simulation should be read as a
plausible best case for disruption responsiveness, not a guaranteed match to
a real cluster's behavior.

## Open questions at time of writing

1. Whether Karpenter's actual disruption controller reconciles purely on a
   fixed interval, or also re-triggers on relevant pod add/remove watch
   events (which would partially close this gap for nodes it happens to be
   watching at the right moment). Not verified against the Karpenter
   implementation.
2. The magnitude of this distortion has not been measured — it depends on
   how frequently real workloads produce job-count oscillations shorter than
   Karpenter's actual reconcile interval, which is workload-specific.
3. Whether this is worth modeling explicitly (e.g. adding a configurable
   "controller sample interval" to the K8S+ provisioner so disruption
   decisions are only evaluated at discrete sampled points, matching real
   Karpenter's resolution) has not been scoped as a story.
