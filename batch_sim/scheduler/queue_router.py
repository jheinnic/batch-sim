"""
BSIM-53: Queue assignment based on the advantage ratio formula.

The advantage ratio measures how much better K8S bin-packing is than Batch
for a specific job on a specific instance type:

    advantage_ratio = (M - M²/C) / S

where:
    M = job.preprocess_peak_ram_gb      (actual peak from Pareto sample)
    S = job.preprocess_steady_ram_gb    (actual steady-state; NOT assumed 0.08M)
    C = instance.ram_gb                 (candidate instance capacity)

Derivation:
    Batch max concurrency  = C / M
    K8S  max concurrency   = (C - M) / S   [one spike worth of headroom reserved]
    ratio                  = [(C-M)/S] / [C/M]
                           = M(C-M) / (SC)
                           = (M - M²/C) / S   ✓

Degenerate condition (K8S ≈ Batch):
    advantage_ratio → 1  when  C → M/0.92  (instance barely larger than peak)
    advantage_ratio < k  →  route to Queue 2 (near-capacity, longer patience)
    advantage_ratio >= k →  route to Queue 1 (bin-packing applies)

The queue split threshold k is a configuration variable, not a constant.
k ∈ {2, 3, 4} is the initial sweep range.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from batch_sim.core.schemas import InstanceTypeConfig
from batch_sim.registry.instance_registry import InstanceRegistry


class QueueClass(str, Enum):
    Q1 = "q1"   # comfortable — K8S bin-packing applies
    Q2 = "q2"   # near-capacity — longer wait, near-Batch economics


@dataclass
class QueueAssignment:
    queue: QueueClass
    instance: InstanceTypeConfig    # cheapest physically-fitting instance
    advantage_ratio: float          # computed value for this job/instance pair
    M: float                        # peak RAM GB
    S: float                        # steady-state RAM GB
    C: float                        # instance capacity GB


def compute_advantage_ratio(M: float, S: float, C: float) -> float:
    """
    Compute the K8S-to-Batch concurrency advantage ratio.

    Returns how many more jobs K8S can pack per node vs Batch (continuous approx).
    Values < k indicate near-capacity degenerate territory for a chosen k.

    Special cases:
      S <= 0: undefined (no steady-state consumption); returns infinity
      M >= C: job does not physically fit on this instance; returns 0
      M <= 0: invalid; returns 0
    """
    if M <= 0 or S <= 0:
        return 0.0
    if M >= C:
        return 0.0   # does not fit
    return (M - (M * M) / C) / S


def assign_queue(
    peak_ram_gb: float,
    steady_ram_gb: float,
    registry: InstanceRegistry,
    k: float,
) -> Optional[QueueAssignment]:
    """
    Assign a job to a queue and pre-select its cheapest fitting instance.

    Iterates the instance registry (already sorted by price ascending).
    The first instance where peak_ram_gb ≤ instance.ram_gb is the cheapest fit.
    The advantage ratio is computed for that instance.

    Returns None if no instance in the registry can physically fit the job.
    """
    M = peak_ram_gb
    S = steady_ram_gb

    cheapest = registry.cheapest_fitting(min_ram_gb=M, min_vcpu=1)
    if cheapest is None:
        return None   # job exceeds all available instances

    C = cheapest.ram_gb
    ratio = compute_advantage_ratio(M, S, C)

    queue = QueueClass.Q1 if ratio >= k else QueueClass.Q2

    return QueueAssignment(
        queue=queue,
        instance=cheapest,
        advantage_ratio=round(ratio, 3),
        M=M,
        S=S,
        C=C,
    )


def queue_summary(assignments: list[QueueAssignment]) -> dict:
    """Summarise a population of queue assignments for reporting."""
    if not assignments:
        return {}

    q1 = [a for a in assignments if a.queue == QueueClass.Q1]
    q2 = [a for a in assignments if a.queue == QueueClass.Q2]
    ratios = [a.advantage_ratio for a in assignments]

    import statistics
    return {
        "total": len(assignments),
        "q1_count": len(q1),
        "q2_count": len(q2),
        "q1_pct": round(len(q1) / len(assignments) * 100, 1),
        "q2_pct": round(len(q2) / len(assignments) * 100, 1),
        "advantage_ratio": {
            "min":    round(min(ratios), 3),
            "max":    round(max(ratios), 3),
            "mean":   round(statistics.mean(ratios), 3),
            "median": round(statistics.median(ratios), 3),
            "stddev": round(statistics.stdev(ratios), 3) if len(ratios) > 1 else 0.0,
        },
    }
