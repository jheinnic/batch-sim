# Executive Summary — Batch Compute Platform Evaluation

> **Stale reference numbers.** The cost table below predates two CPU-model bug
> fixes (`docs/CPU_MODELING.md`) and the removal of the panic-escalation
> mechanism described in an earlier version of this document (it modeled
> automatic priority/capacity escalation with no real analog in AWS Batch or
> Kubernetes/Karpenter). Re-run the reference scenario before citing these
> numbers in a real decision.

## The Question

Should we consider replacing AWS Batch with OKD (open-source Kubernetes) for our
batch compute workloads? And can we evaluate this without modifying our existing
container image or building a prototype first?

## The Method

We built a discrete-event simulation that faithfully models how both platforms
schedule jobs given our actual workload characteristics. The same reproducible
stream of 242 jobs — drawn from four workload profiles that represent our real
submission mix — was processed by both schedulers and the results compared.

The simulation captures the key structural difference between the two platforms:
AWS Batch provisions each job's server for peak RAM and peak CPU simultaneously,
even though those peaks occur in different phases and never overlap. Kubernetes
schedules based on a job's steady-state resource use (8% of peak RAM), reserving
headroom for the brief (&lt;1 minute) memory spike. This allows K8S to place
substantially more jobs per server.

## The Result

Over a 4-hour simulated window:

| Metric | AWS Batch | OKD / K8S |
|--------|-----------|-----------|
| Total EC2 cost | $402.61 | **$397.95** |
| Jobs completed | 242 | 242 |
| Mean queue wait | 390s | 390s |
| SLA breaches | 0 | 0 |
| Job crashes | 0 | 0 |

K8S is 1.2% cheaper with identical service quality. The savings are expected to
grow with workload density: the advantage of better bin-packing compounds as more
jobs compete for server capacity simultaneously.

## Key Assumptions

1. The container image is used as-is. No code changes are required or assumed.
2. Comparison is limited to EC2 compute cost. S3 and SQS charges are equal between
   platforms and excluded. OKD has no licensing fee (unlike Red Hat OpenShift).
3. The simulation models memory collision recovery via automatic job restart
   (up to 3 retries). Zero collisions occurred in the reference run.

## Recommendation

**Proceed to a small-scale prototype.** The simulation provides sufficient evidence
that K8S/OKD is cost-favorable without degrading service quality. A prototype would
validate real scheduling behavior under production load and quantify operational
overhead before any migration decision is made. OKD is free and open-source; a
test cluster can be created and dismantled without long-term commitment.

The ask is approval to build a prototype — not a migration decision.

---

*Simulation source: github.com/jheinnic/batch-sim · Reference run: seed=42, 4-hour
horizon, 242 jobs, 2 schedulers — see the staleness note above before citing.*
