"""
BSIM-62: Utilization sampler process.

Runs as a SimPy process, waking every sample_interval_s seconds to snapshot
the live NodeModel states and emit a UTILIZATION_SAMPLE event containing:

    allocated   = sum(node.physical_ram_gb)              [net of OS overhead]
    reserved    = sum(job soft or hard limits) + headroom [fixed definitions]
    util1       = sum(job.current_phase_ram)              [instantaneous]
    util2       = util3_val + max_supportable_burst       [max theoretical]
    util3       = sum(min(phase_ram, soft_limit))         [min theoretical / floor]

See BSIM-61 for full definitions, OS overhead treatment, and ratio interpretations.
"""

from __future__ import annotations

import simpy
from batch_sim.metrics.collector import MetricsCollector, EventType, SimEvent


def _max_burst_contribution(slots, headroom_gb: float) -> float:
    """
    Greedy 0/1 knapsack: given the burst headroom available on this node,
    what is the largest possible combined burst we could support simultaneously?

    Selects jobs by peak_ram descending until headroom is exhausted.
    Returns the total peak RAM of the selected subset.
    """
    peaks = sorted(
        [s.phase_peak_ram_gb for s in slots if s.phase_peak_ram_gb > 0],
        reverse=True,
    )
    total = 0.0
    for p in peaks:
        if total + p <= headroom_gb:
            total += p
        else:
            break
    return total


def utilization_sampler(
    env: simpy.Environment,
    nodes: dict,            # node_id → NodeModel; live reference, updated by scheduler
    metrics: MetricsCollector,
    scheduler_type: str,    # "batch" or "k8s"
    sample_interval_s: float = 60.0,
):
    """
    SimPy generator process. Wakes every sample_interval_s to record
    UTILIZATION_SAMPLE events.

    Call via: env.process(utilization_sampler(env, scheduler._nodes, metrics, 'k8s'))
    The `nodes` dict is a live reference — changes made by the scheduler
    between samples are automatically reflected.
    """
    while True:
        yield env.timeout(sample_interval_s)

        active = {
            nid: node for nid, node in nodes.items()
            if node.state.value in ('ready', 'idle')
            or len(node._slots) > 0
        }

        if not active:
            metrics.record(SimEvent(EventType.COST_SAMPLE, env.now, {
                'type': 'utilization_sample', 't': round(env.now),
                'nodes': 0, 'allocated': 0,
                'reserved': 0, 'util1': 0, 'util2': 0, 'util3': 0,
            }))
            continue

        allocated = sum(node.physical_ram_gb for node in active.values())
        reserved  = 0.0
        util1     = 0.0
        util2_base = 0.0   # soft-cap floor (shared between util2 and util3)
        util2_burst = 0.0  # max-burst contribution per node

        for node in active.values():
            slots = list(node._slots.values())

            # ── Reserved ─────────────────────────────────────────────────
            if scheduler_type == 'batch':
                # Hard limits: each job reserves its declared peak for its
                # full tenure regardless of current phase
                reserved += sum(s.job.profile.preprocess_peak_ram_gb for s in slots)
            else:
                # Soft limits per job + fixed spike headroom per node
                reserved += sum(s.job.profile.soft_limit_ram_gb for s in slots)
                reserved += node.spike_headroom_gb_at_launch

            # ── Util1: instantaneous ──────────────────────────────────────
            util1 += sum(s.phase_peak_ram_gb for s in slots)

            # ── Util3 base: every job capped at soft limit ────────────────
            node_floor = sum(
                min(s.phase_peak_ram_gb, s.soft_limit_ram_gb or s.job.profile.soft_limit_ram_gb)
                for s in slots
            )
            util2_base += node_floor

            # ── Util2 burst contribution: largest supportable burst subset ─
            headroom = node.spike_headroom_gb_at_launch
            if headroom > 0 and slots:
                util2_burst += _max_burst_contribution(slots, headroom)

        util2 = util2_base + util2_burst
        util3 = util2_base   # floor only, no burst

        metrics.record(SimEvent(EventType.COST_SAMPLE, env.now, {
            'type': 'utilization_sample',
            't': round(env.now),
            'nodes': len(active),
            'allocated': round(allocated, 2),
            'reserved':  round(reserved, 2),
            'util1':     round(util1, 2),
            'util2':     round(util2, 2),
            'util3':     round(util3, 2),
        }))
