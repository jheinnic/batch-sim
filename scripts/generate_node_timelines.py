"""
scripts/generate_node_timelines.py

Produces Gantt-style node lifecycle charts for every node that ran in a
simulation, derived from a scorecard directory that contains the raw
event log (events.json).

Because the scorecard alone does not contain the event log, this script
re-runs the simulation from the saved event list and scheduler config to
reconstruct the full per-node job timeline.  All inputs are reproducible
from the configs and workload files committed to the repository.

Usage:
    python scripts/generate_node_timelines.py --scheduler batch
    python scripts/generate_node_timelines.py --scheduler k8s
    python scripts/generate_node_timelines.py --scheduler batch --events workloads/reference_4h_v2.json
    python scripts/generate_node_timelines.py --help

Paths:
    --event-log PATH    Read a saved *_events.json instead of re-running.
                        Re-run path: simulation is re-executed via run_one()
                        (same code path as the simulate command).

Output:
    results/node_timelines/<scheduler>/
        overview.png          — all nodes, one row each, compressed time axis
        overview.svg
        node_<id>.png         — one chart per node, full-resolution
        node_<id>.svg
        summary.json          — structured data for all node timelines

Phase colour coding (consistent with presentation):
    download    #888780  grey
    preprocess  #A32D2D  red   (peak RAM phase)
    workhorse   #185FA5  blue  (CPU phase)
    upload      #3B6D11  green
    warmup      #d0cec8  light grey (node not yet READY)
    idle        #ebe9e4  very light (no jobs running)
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import warnings
warnings.filterwarnings('ignore', message='.*tight_layout.*')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_sim.core.config_loader import load_simulation_config, load_scheduler_config
from batch_sim.registry.instance_registry import InstanceRegistry, compute_k8s_capacity
from batch_sim.generator.event_list import load_event_list
from batch_sim.core.schemas import SchedulerType
from batch_sim.metrics.collector import MetricsCollector, EventType


# ---------------------------------------------------------------------------
# Colour map
# ---------------------------------------------------------------------------

PHASE_COLOR = {
    'download':   '#888780',
    'preprocess': '#A32D2D',
    'workhorse':  '#185FA5',
    'upload':     '#3B6D11',
    'warmup':     '#d0cec8',
    'idle':       '#ebe9e4',
    'crash':      '#e74c3c',
}

# BSIM-82: stacked waste layer colours
WASTE_COLOR = {
    'effective':       '#2ecc71',   # green  — useful work
    'io_ineligible':   '#f1c40f',   # yellow — I/O blocked, cannot redistribute
    'thread_count':    '#e67e22',   # orange — above thread ceiling
    'hard_limit':      '#e74c3c',   # red    — CFS quota (K8S only)
}

CENTROID_HATCH = {
    'centroid_a': '',
    'centroid_b': '//',
    'centroid_c': '\\\\',
    'centroid_d': 'xx',
    'centroid_e': '..',
    'centroid_f': '++',
}

FONT = 'monospace'


# ---------------------------------------------------------------------------
# BSIM-79 helper: synthetic termination for nodes alive at sim-end
# ---------------------------------------------------------------------------

def _apply_synthetic_terminations(
    node_launch_time: dict, node_term_time: dict, log: list
) -> None:
    """Mutates node_term_time: adds synthetic entries for nodes still alive at sim end."""
    if not log:
        return
    sim_end_t = max(e.sim_time for e in log)
    for nid in node_launch_time:
        if nid not in node_term_time:
            node_term_time[nid] = sim_end_t


# ---------------------------------------------------------------------------
# Simulation re-run and timeline extraction
# ---------------------------------------------------------------------------

def run_and_extract(
    event_list_path: str,
    scheduler_type: str,
    cfg_path: str,
    registry_path: str,
    seed: int,
    event_log_path: str | None = None,
    os_overhead_gb: float = 0.0,
) -> tuple[dict, dict]:
    """
    Build node timelines from a simulation run.

    If event_log_path is provided (a *_events.json file saved by
    `python -m batch_sim simulate`), reads directly from that log.

    If event_log_path is absent, falls back to re-running the simulation
    via run_one() (same code path as the main simulate command).

    BSIM-79 fix: nodes that never received NODE_TERMINATED (because the
    sim ended before their idle timers fired) are given a synthetic
    termination time equal to the last event in the log.
    """
    import json as _json
    from batch_sim.metrics.collector import SimEvent

    el           = load_event_list(event_list_path)
    job_profiles = {e.job_id: e for e in el.events}

    if event_log_path:
        with open(event_log_path) as f:
            raw_events = _json.load(f)
        from batch_sim.metrics.collector import EventType as ET
        log = []
        for r in raw_events:
            try:
                et = ET(r['event_type'])
            except ValueError:
                continue
            log.append(SimEvent(event_type=et, sim_time=r['sim_time'],
                                data=r.get('data', {})))
        registry = InstanceRegistry.from_yaml(registry_path)
        print(f"  Reading {len(log)} events from saved log")
    else:
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.config_loader import load_scheduler_config
        from batch_sim.core.schemas import SchedulerType

        cfg      = load_scheduler_config(cfg_path)
        registry = InstanceRegistry.from_yaml(registry_path)
        cool_off  = el.metadata.get('cool_off_seconds', 0.0)

        sc, metrics = run_one(
            event_list=el,
            scheduler_type=SchedulerType(scheduler_type),
            cfg=cfg,
            registry=registry,
            event_list_path=event_list_path,
            seed=seed,
            return_metrics=True,
        )
        log = metrics.log
        print(f"  Re-ran simulation: {sc.job_stats.pool_job_count} jobs completed")

    # ── Node lifecycle from events ─────────────────────────────────────────
    node_instances   = {}
    node_launch_time = {}
    node_ready_time  = {}
    node_term_time   = {}
    node_idle_dur    = {}

    for e in log:
        nid = e.data.get('node_id')
        if not nid:
            continue
        t = e.sim_time
        if e.event_type == EventType.NODE_LAUNCHING:
            node_instances[nid]   = e.data.get('instance_name', '?')
            node_launch_time[nid] = t
        if e.event_type == EventType.NODE_READY:
            node_ready_time[nid]  = t
        if e.event_type == EventType.NODE_TERMINATED:
            node_term_time[nid]   = t
            node_idle_dur[nid]    = e.data.get('idle_duration_s', 0)

    # BSIM-79: synthetic termination for nodes alive at sim end
    _apply_synthetic_terminations(node_launch_time, node_term_time, log)

    # ── BSIM-82: per-node CPU_WASTE step functions ─────────────────────────
    # node_id → list of (t, effective_vcpu, io_ineligible, thread_count, hard_limit)
    # (node_id, job_id) → list of (t, effective_vcpu, current_vcpu, io_ineligible, thread_count, hard_limit, remaining_cpu_s)
    node_cpu_waste: dict[str, list] = {}
    job_cpu_waste: dict[tuple, list] = {}
    for e in log:
        if e.event_type == EventType.CPU_WASTE:
            nid = e.data.get('node_id')
            jid = e.data.get('job_id')
            if nid and jid:
                # Per-job event (has job_id)
                eff = e.data.get('effective_vcpu', 0.0)
                job_cpu_waste.setdefault((nid, jid), []).append((
                    e.sim_time,
                    eff,
                    e.data.get('current_vcpu', eff),
                    e.data.get('io_ineligible_waste', 0.0),
                    e.data.get('thread_count_waste', 0.0),
                    e.data.get('hard_limit_waste', 0.0),
                    e.data.get('remaining_cpu_s', 0.0),
                ))
            elif nid:
                # Node composite event (no job_id)
                node_cpu_waste.setdefault(nid, []).append((
                    e.sim_time,
                    e.data.get('effective_vcpu', 0.0),
                    e.data.get('io_ineligible_waste', 0.0),
                    e.data.get('thread_count_waste', 0.0),
                    e.data.get('hard_limit_waste', 0.0),
                ))

    # ── BSIM-94: per-node storage pool events ─────────────────────────────
    # pool_expanded[nid]    = [(t, old_gb, new_gb)]
    # pool_exhausted[nid]   = True
    # pool_gen_events[nid]  = [(event_type_str, t, gen_id, capacity_gb)]
    pool_expanded:   dict[str, list] = {}
    pool_exhausted:  dict[str, bool] = {}
    pool_gen_events: dict[str, list] = {}
    for e in log:
        nid = e.data.get('node_id')
        if not nid:
            continue
        if e.event_type == EventType.STORAGE_POOL_EXPANDED:
            pool_expanded.setdefault(nid, []).append(
                (e.sim_time, e.data.get('old_gb', 0.0), e.data.get('new_gb', 0.0)))
        elif e.event_type == EventType.STORAGE_EXHAUSTED:
            pool_exhausted[nid] = True
        elif e.event_type in (EventType.STORAGE_GEN_OPENED, EventType.STORAGE_GEN_RELEASED):
            pool_gen_events.setdefault(nid, []).append((
                e.event_type.value, e.sim_time,
                e.data.get('gen_id', 0), e.data.get('capacity_gb', 0.0)))

    inst_map = {i.name: i for i in registry.all_types}

    # ── Per-job phase windows ──────────────────────────────────────────────
    phase_starts  = {}
    phase_windows = collections.defaultdict(list)
    job_centroids = {}
    job_start_t   = {}
    job_status    = {}

    for e in log:
        nid = e.data.get('node_id')
        jid = e.data.get('job_id')
        if not nid or not jid:
            continue
        t = e.sim_time

        if e.event_type == EventType.JOB_START:
            job_start_t[(nid, jid)] = t
            job_centroids[jid]      = e.data.get('centroid_id', '?')

        if e.event_type == EventType.PHASE_TRANSITION:
            key = (nid, jid)
            ph  = e.data['phase']
            if key in phase_starts:
                prev_ph, prev_t = phase_starts[key]
                phase_windows[key].append((prev_ph, prev_t, t))
            phase_starts[key] = (ph, t)

        if e.event_type in (EventType.JOB_COMPLETE, EventType.JOB_CRASH):
            key = (nid, jid)
            if key in phase_starts:
                prev_ph, prev_t = phase_starts.pop(key)
                phase_windows[key].append((prev_ph, prev_t, t))
            job_status[(nid, jid)] = (
                'crash' if e.event_type == EventType.JOB_CRASH else 'complete'
            )

    # ── Assemble per-node structure ────────────────────────────────────────
    all_node_ids = set(node_launch_time.keys())
    node_timelines = {}

    for nid in all_node_ids:
        inst_name = node_instances.get(nid, '?')
        inst      = inst_map.get(inst_name)
        if inst is None:
            continue

        launch_t = node_launch_time[nid]
        term_t   = node_term_time[nid]
        cost     = (term_t - launch_t) / 3600.0 * inst.hourly_price_usd

        jobs_here = []
        for (n2, jid), windows in phase_windows.items():
            if n2 != nid:
                continue
            prof = job_profiles.get(jid)

            if scheduler_type == 'batch':
                res_gb = prof.preprocess_peak_ram_gb if prof else None
            else:
                res_gb = prof.workhorse_hard_limit_gb if prof else None

            j_soft = (prof.soft_cpu or prof.workhorse_declared_vcpu) if prof else None
            j_hard = (prof.hard_cpu or prof.workhorse_declared_vcpu) if prof else None

            # BSIM-81: per-phase RAM from actual job profile
            if prof:
                phase_ram = {
                    'download':   prof.download_ram_gb,
                    'preprocess': prof.preprocess_peak_ram_gb,
                    'workhorse':  prof.workhorse_ram_gb,
                    'upload':     prof.upload_ram_gb,
                }
                phase_vcpu = {
                    'download':   1.0,
                    'preprocess': float(prof.preprocess_vcpu),
                    'workhorse':  float(prof.workhorse_peak_vcpu),
                    'upload':     1.0,
                }
            else:
                peak = res_gb or 4.0
                phase_ram  = {'download': 0.5, 'preprocess': peak,
                              'workhorse': peak * 0.08, 'upload': 0.5}
                phase_vcpu = {'download': 1.0, 'preprocess': 1.0,
                              'workhorse': 4.0, 'upload': 1.0}

            jobs_here.append({
                'job_id':          jid,
                'centroid':        job_centroids.get(jid, '?'),
                'start_t':         job_start_t.get((nid, jid),
                                                    windows[0][1] if windows else 0),
                'end_t':           windows[-1][2] if windows else 0,
                'status':          job_status.get((nid, jid), 'complete'),
                'phases':          [(ph, t0, t1) for ph, t0, t1 in windows],
                'reserved_ram_gb': round(res_gb, 1) if res_gb else None,
                'soft_cpu':        j_soft,
                'hard_cpu':        j_hard,
                'phase_ram_gb':    phase_ram,
                'phase_vcpu':      phase_vcpu,
                # Per-job CPU_WASTE steps: (t, effective_vcpu, current_vcpu, io_ineligible, thread_count, hard_limit, remaining_cpu_s)
                'cpu_waste_steps': sorted(
                    job_cpu_waste.get((nid, jid), []), key=lambda x: x[0]
                ),
            })
        jobs_here.sort(key=lambda j: j['start_t'])

        # Compute effective_schedulable_gb from observed peak RAMs (k8s/k8splus only)
        eff_sch_gb = None
        spike_headroom_gb = None
        if os_overhead_gb > 0 and scheduler_type in ('k8s', 'k8splus') and inst:
            peak_rams_here = [
                j['phase_ram_gb']['preprocess']
                for j in jobs_here
                if j.get('phase_ram_gb') and j['phase_ram_gb'].get('preprocess', 0) > 0
            ]
            # BSIM-104: compute_k8s_capacity now takes a scalar spike_max_gb instead of
            # a peak-RAM list. Reproduce the legacy chart value — the spike reservation
            # is the largest observed preprocess peak that fits this instance.
            fitting = [r for r in peak_rams_here if r <= inst.ram_gb - os_overhead_gb]
            if fitting:
                _cap = compute_k8s_capacity(inst, max(fitting),
                                            os_overhead_gb=os_overhead_gb)
                eff_sch_gb      = round(_cap.effective_schedulable_gb, 2)
                spike_headroom_gb = round(_cap.spike_headroom_gb, 2)

        node_soft = max((j.get('soft_cpu') or 0 for j in jobs_here), default=0)
        node_hard = max((j.get('hard_cpu') or 0 for j in jobs_here), default=0)

        node_timelines[nid] = {
            'instance':               inst_name,
            'ram_gb':                 inst.ram_gb,
            'vcpu':                   inst.vcpu,           # BSIM-80
            'hourly_usd':             inst.hourly_price_usd,
            'launch_t':               launch_t,
            'ready_t':                node_ready_time.get(nid, launch_t),
            'term_t':                 term_t,
            'cost':                   round(cost, 4),
            'jobs':                   jobs_here,
            'soft_cpu':               node_soft if node_soft > 0 else None,
            'hard_cpu':               node_hard if node_hard > 0 else None,
            'reserved_ram_gb':        round(max(
                (j.get('reserved_ram_gb') or 0 for j in jobs_here), default=0
            ), 1),
            'effective_schedulable_gb': eff_sch_gb,          # None for batch
            'spike_headroom_gb':       spike_headroom_gb,   # None for batch
            'cpu_waste_steps':        sorted(             # BSIM-82
                node_cpu_waste.get(nid, []), key=lambda x: x[0]
            ),
            'pool_expanded':          pool_expanded.get(nid, []),     # BSIM-94
            'pool_exhausted':         pool_exhausted.get(nid, False), # BSIM-94
            'pool_gen_events':        pool_gen_events.get(nid, []),   # BSIM-94
        }

    metadata = {
        'scheduler':   scheduler_type,
        'event_list':  event_list_path,
        'total_nodes': len(node_timelines),
        'total_jobs':  sum(len(v['jobs']) for v in node_timelines.values()),
        'total_cost':  round(sum(v['cost'] for v in node_timelines.values()), 4),
        'seed':        seed,
    }

    return node_timelines, metadata


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

def _draw_node_row(
    ax, node: dict, y: float, row_h: float,
    t_origin: float, t_scale: float, width: float,
    scheduler_type: str = 'batch',
) -> None:
    launch = node['launch_t']
    ready  = node['ready_t']
    term   = node['term_t']

    def px(t):
        return (t - t_origin) * t_scale

    ax.barh(y, px(ready) - px(launch), row_h * 0.7,
            left=px(launch), color=PHASE_COLOR['warmup'], zorder=2)

    first_job_t = node['jobs'][0]['start_t'] if node['jobs'] else term
    last_job_t  = node['jobs'][-1]['end_t']  if node['jobs'] else ready
    if first_job_t > ready:
        ax.barh(y, px(first_job_t) - px(ready), row_h * 0.5,
                left=px(ready), color=PHASE_COLOR['idle'], zorder=2)
    if last_job_t < term:
        ax.barh(y, px(term) - px(last_job_t), row_h * 0.5,
                left=px(last_job_t), color=PHASE_COLOR['idle'], zorder=2)

    for job in node['jobs']:
        hatch = CENTROID_HATCH.get(job['centroid'], '')
        for ph, t0, t1 in job['phases']:
            if t1 <= t0:
                continue
            color = PHASE_COLOR.get(ph, '#aaa')
            ax.barh(y, px(t1) - px(t0), row_h * 0.75,
                    left=px(t0), color=color, hatch=hatch,
                    edgecolor='white', linewidth=0.3, zorder=3)
        if job['status'] == 'crash':
            ax.plot(px(job['end_t']), y, 'x',
                    color='#e74c3c', markersize=6, zorder=5, mew=1.5)

        res_gb = job.get('reserved_ram_gb')
        if res_gb is not None:
            tick_x = px(job['end_t'])
            ax.plot([tick_x, tick_x],
                    [y - row_h * 0.45, y + row_h * 0.45],
                    color='#222', linewidth=1.2, zorder=6)
            ax.text(tick_x + 1.5, y, f'{res_gb:.0f}G',
                    fontsize=6, va='center', color='#222',
                    fontfamily=FONT, zorder=7)


# ---------------------------------------------------------------------------
# BSIM-81: Event-driven pool-wide usage time series
# ---------------------------------------------------------------------------

def _build_usage_series(
    node_timelines: dict,
    sample_s: float = 60.0,
    t_start: float | None = None,
    t_end: float | None = None,
) -> list[dict]:
    """
    Build a time series of pool-wide RAM and CPU usage at sample_s intervals.

    t_start / t_end optionally constrain the sampling window (absolute sim
    seconds).  's['t']' values in the returned list are always relative to
    t_start so callers can multiply directly by t_scale without an offset.

    BSIM-81: RAM uses actual per-phase values from job profiles (phase_ram_gb).
    CPU uses per-node CPU_WASTE step functions (cpu_waste_steps) rather than
    hard-coded phase constants.

    Returns list of {t, alloc_ram, soft_reserved_ram, used_ram,
                        alloc_vcpu, soft_reserved_vcpu, used_vcpu, n_nodes}.
    All values are in physical units (GB, vCPU).
    soft_reserved_* tracks the scheduler's committed soft-limit reservations so
    charts can show: used → reserved (committed) → burst/headroom → provisioned.
    """
    if not node_timelines:
        return []

    t_min = t_start if t_start is not None else min(n['launch_t'] for n in node_timelines.values())
    t_max = t_end   if t_end   is not None else max(n['term_t']   for n in node_timelines.values())

    # Build per-job phase RAM lookup: (nid, jid) → [(phase, t0, t1, ram_gb)]
    phase_resource: dict[tuple, list] = {}
    for nid, node in node_timelines.items():
        for job in node['jobs']:
            phase_ram = job.get('phase_ram_gb', {})
            segs = []
            for ph, t0, t1 in job['phases']:
                ram = phase_ram.get(ph, 0.0)
                segs.append((ph, t0, t1, ram))
            phase_resource[(nid, job['job_id'])] = segs

    # Per-job soft-limit (= reserved_ram_gb) for spike-zone burst calculation:
    # burst_j = max(0, preprocess_peak_j - soft_limit_j) draws from the spike pool.
    job_soft_limit = {
        (nid, job['job_id']): (job.get('reserved_ram_gb') or 0.0)
        for nid, node in node_timelines.items()
        for job in node['jobs']
    }

    # Per-job soft-limit reservation windows: (nid, start_t, end_t, ram_gb, vcpu)
    job_reservations = [
        (nid,
         job['start_t'], job['end_t'],
         job.get('reserved_ram_gb') or 0.0,
         job.get('soft_cpu') or 0.0)
        for nid, node in node_timelines.items()
        for job in node['jobs']
        if job['start_t'] < job['end_t']
    ]

    last_job_end_by_node = {
        nid: max((j['end_t'] for j in node['jobs']), default=0.0)
        for nid, node in node_timelines.items()
    }

    def _node_effective_vcpu(nid: str, t: float) -> float:
        """Step-function lookup of effective_vcpu from cpu_waste_steps.
        Returns 0 once all jobs on the node have finished."""
        if t >= last_job_end_by_node.get(nid, 0.0):
            return 0.0
        steps = node_timelines[nid].get('cpu_waste_steps', [])
        val = 0.0
        for (st, eff, *_) in steps:
            if st <= t:
                val = eff
            else:
                break
        return val

    series = []
    t = t_min
    while t <= t_max + sample_s:
        active_nodes = [
            (nid, n) for nid, n in node_timelines.items()
            if n['launch_t'] <= t < n['term_t']
        ]
        alloc_ram  = sum(n['ram_gb'] for _, n in active_nodes)
        alloc_vcpu = sum(n['vcpu']   for _, n in active_nodes)
        pool_sch   = sum(
            (n.get('effective_schedulable_gb') or 0.0) for _, n in active_nodes
        )
        used_ram   = 0.0
        used_vcpu  = 0.0
        soft_res_ram  = 0.0
        soft_res_vcpu = 0.0

        active_set = {nid for nid, _ in active_nodes}
        for (nid, jid), segs in phase_resource.items():
            if nid not in active_set:
                continue
            for ph, t0, t1, ram in segs:
                if t0 <= t < t1:
                    used_ram += ram

        for nid in active_set:
            used_vcpu += _node_effective_vcpu(nid, t)

        for nid, jstart, jend, jram, jcpu in job_reservations:
            if nid in active_set and jstart <= t < jend:
                soft_res_ram  += jram
                soft_res_vcpu += jcpu

        # Spike pool consumption: sum of (preprocess_peak - soft_limit) for all
        # jobs currently in the preprocess phase.  These N GB are drawn from the
        # unreservable spike headroom zone, causing its bottom edge to rise.
        spike_consumed = 0.0
        for (nid, jid), segs in phase_resource.items():
            if nid not in active_set:
                continue
            for ph, t0, t1, ram in segs:
                if ph == 'preprocess' and t0 <= t < t1:
                    soft_lim = job_soft_limit.get((nid, jid), 0.0)
                    spike_consumed += max(0.0, ram - soft_lim)
                    break

        series.append({
            't':                  round(t - t_min),
            'alloc_ram':          round(alloc_ram, 1),
            'pool_schedulable':   round(pool_sch, 1),
            'soft_reserved_ram':  round(soft_res_ram, 1),
            'used_ram':           round(used_ram, 1),
            'spike_consumed':     round(spike_consumed, 1),
            'alloc_vcpu':         round(alloc_vcpu, 1),
            'soft_reserved_vcpu': round(soft_res_vcpu, 1),
            'used_vcpu':          round(used_vcpu, 1),
            'n_nodes':            len(active_nodes),
        })
        t += sample_s
    return series


# ---------------------------------------------------------------------------
# BSIM-82: Stacked CPU waste panel
# ---------------------------------------------------------------------------

def _draw_cpu_waste_panel(
    ax,
    node: dict,
    t_min: float,
    t_scale: float,
) -> None:
    """
    Draw a stacked step-function panel showing CPU decomposition for one node.

    Layers (bottom to top):
      effective     — cycles doing useful work           (green)
      io_ineligible — I/O-blocked cycles (not redistributable) (yellow)
      thread_count  — allocated above stage thread ceiling    (orange)
      hard_limit    — withheld by CFS quota (K8S only)        (red)

    Source: cpu_waste_steps = [(t, eff, io_inel, thread_cnt, hard_lim), ...]
    """
    steps = node.get('cpu_waste_steps', [])
    if not steps:
        ax.text(0.5, 0.5, 'no CPU_WASTE events', transform=ax.transAxes,
                ha='center', va='center', fontsize=8, color='#888',
                fontfamily=FONT)
        ax.set_ylim(0, 1)
        return

    node_vcpu = node.get('vcpu', 1)
    ts   = [(s[0] - t_min) * t_scale for s in steps]
    eff  = [s[1] for s in steps]
    io_w = [s[2] for s in steps]
    thrd = [s[3] for s in steps]
    hlim = [s[4] for s in steps]

    # Extend the step function to cover the full node lifetime, zeroing CPU
    # once all jobs have finished so idle tails don't ghost the last phase.
    t_last_job_end_val = (
        (max(j['end_t'] for j in node.get('jobs', [])) - t_min) * t_scale
        if node.get('jobs') else 0.0
    )
    t_term = (node['term_t'] - t_min) * t_scale
    # Insert a zero step at last-job-end.  With step='post' semantics the
    # preceding step's value naturally covers [prev_t, t_last_job_end).
    if ts and ts[-1] < t_last_job_end_val - 1e-6:
        ts.append(t_last_job_end_val)
        eff.append(0.0); io_w.append(0.0); thrd.append(0.0); hlim.append(0.0)
    # Extend to node termination (value is already 0 if there was an idle tail).
    if ts and ts[-1] < t_term - 1e-6:
        ts.append(t_term)
        eff.append(eff[-1]); io_w.append(io_w[-1])
        thrd.append(thrd[-1]); hlim.append(hlim[-1])

    # Soft-limit reservation step function derived from job start/end events.
    # Built from the *original* steps list (before the termination extension),
    # then extended to match ts length.
    job_events = sorted(
        [(job['start_t'], +(job.get('soft_cpu') or 0.0)) for job in node.get('jobs', [])]
        + [(job['end_t'],  -(job.get('soft_cpu') or 0.0)) for job in node.get('jobs', [])]
    )
    sr_running, ei = 0.0, 0
    sr_vals = []
    for st, *_ in steps:
        while ei < len(job_events) and job_events[ei][0] <= st:
            sr_running += job_events[ei][1]
            ei += 1
        sr_vals.append(max(0.0, sr_running))
    # Mirror the termination-extension applied to ts above.
    while len(sr_vals) < len(ts):
        sr_vals.append(sr_vals[-1] if sr_vals else 0.0)

    # Stacked fill_between (step='post')
    def _stack(ax, ts, bottom, top, color, label, alpha=0.8):
        ax.fill_between(ts, bottom, top,
                        step='post', color=color, alpha=alpha,
                        label=label)

    b0 = [0.0] * len(ts)
    b1 = eff
    b2 = [a + b for a, b in zip(b1, io_w)]
    b3 = [a + b for a, b in zip(b2, thrd)]
    b4 = [a + b for a, b in zip(b3, hlim)]

    _stack(ax, ts, b0, b1, WASTE_COLOR['effective'],     'effective vCPU', alpha=0.75)
    _stack(ax, ts, b1, b2, WASTE_COLOR['io_ineligible'], 'I/O-blocked waste')
    _stack(ax, ts, b2, b3, WASTE_COLOR['thread_count'],  'thread-count waste')
    _stack(ax, ts, b3, b4, WASTE_COLOR['hard_limit'],    'hard-limit waste (CFS)')

    # Soft-limit reservation floor: dashed line showing total committed soft_cpu
    if sr_vals:
        ax.step(ts, sr_vals, where='post', color='#444', linewidth=1.1,
                linestyle=':', zorder=6, label='soft-limit reservation')

    ax.axhline(node_vcpu, color='#555', linewidth=0.8, linestyle='--', alpha=0.5)
    ax.set_ylim(0, node_vcpu * 1.1)
    ax.set_ylabel('vCPU', fontfamily=FONT, fontsize=8)
    ax.tick_params(axis='y', labelsize=7)
    ax.set_facecolor('#fafaf9')
    ax.grid(True, axis='x', alpha=0.18)
    ax.legend(fontsize=6.5, loc='upper right', ncol=2, framealpha=0.9)


# ---------------------------------------------------------------------------
# Overview chart
# ---------------------------------------------------------------------------

def _render_overview_page(
    gantt_nodes: list,
    all_timelines: dict,
    metadata: dict,
    out: Path,
    scheduler_type: str,
    t_min: float,
    t_max: float,
    filename: str,
    title_suffix: str,
    png_only: bool = False,
) -> None:
    """Render one overview page.

    The Gantt panel shows only `gantt_nodes`; the pool-wide CPU/RAM panels
    always use `all_timelines` so every page shares consistent context.
    The x-axis is fixed to [t_min, t_max] so pages share the same scale.
    """
    t_span  = t_max - t_min
    n_nodes = len(gantt_nodes)
    row_h   = max(0.35, min(0.9, 36 / n_nodes))
    gantt_h = max(4, n_nodes * row_h * 1.35 + 1.5)
    ram_h   = 2.2
    cpu_h   = 1.6
    fig_w   = 18
    has_storage_overview = any(
        n.get('pool_expanded') or n.get('pool_gen_events')
        for _, n in gantt_nodes
    )
    storage_h = 1.2 if has_storage_overview else 0
    n_ov_panels = 3 + (1 if has_storage_overview else 0)
    h_ratios_ov = [gantt_h, ram_h, cpu_h] + ([storage_h] if has_storage_overview else [])
    total_ov_h  = gantt_h + ram_h + cpu_h + storage_h + 0.8

    fig = plt.figure(figsize=(fig_w, total_ov_h))
    gs  = fig.add_gridspec(n_ov_panels, 1, height_ratios=h_ratios_ov, hspace=0.06)
    ax_gantt   = fig.add_subplot(gs[0])
    ax_usage   = fig.add_subplot(gs[1], sharex=ax_gantt)
    ax_cpu     = fig.add_subplot(gs[2], sharex=ax_gantt)
    ax_ov_stor = fig.add_subplot(gs[3], sharex=ax_gantt) if has_storage_overview else None

    t_scale = (fig_w - 2) / t_span

    for i, (nid, node) in enumerate(gantt_nodes):
        y = n_nodes - 1 - i
        _draw_node_row(ax_gantt, node, y, row_h, t_min, t_scale,
                       fig_w, scheduler_type=scheduler_type)
        soft = node.get('soft_cpu', '?')
        hard = node.get('hard_cpu', '?')
        ram  = node.get('reserved_ram_gb', node['ram_gb'])
        label = (f"{nid}  {node['instance']}  ${node['cost']:.2f}"
                 f"  ({len(node['jobs'])}j)"
                 f"  cpu:{soft}/{hard}  ram:{ram:.0f}G")
        ax_gantt.text(-0.005, y, label,
                      transform=ax_gantt.get_yaxis_transform(),
                      ha='right', va='center', fontsize=6, fontfamily=FONT)

    ax_gantt.set_ylim(-0.8, n_nodes - 0.2)
    ax_gantt.set_yticks([])
    ax_gantt.set_xlim(0, t_span * t_scale)
    ax_gantt.tick_params(axis='x', labelbottom=False)
    ax_gantt.set_facecolor('#fafaf9')
    ax_gantt.grid(True, axis='x', alpha=0.15, zorder=1)
    ax_gantt.legend(handles=_legend_handles(), fontsize=7,
                    loc='upper right', ncol=3, framealpha=0.9)

    sched_label = ('AWS Batch' if scheduler_type == 'batch'
                   else 'OKD K8S' if scheduler_type == 'k8s'
                   else 'OKD K8S+')
    ax_gantt.set_title(
        f'{sched_label} — Node Lifecycles  '
        f'{metadata["total_jobs"]} jobs · ${metadata["total_cost"]:.2f}'
        f'{title_suffix}  '
        f'(left labels: instance  cost  jobs  cpu:soft/hard  ram:limit)',
        fontfamily=FONT, fontsize=9, loc='left',
    )

    # ── Bottom panels: CPU and RAM (pool-wide, BSIM-80 absolute units) ───
    # t_start/t_end constrain the series to this page's time window so the
    # usage panels align with the gantt and omit uncharted simulation time.
    series = _build_usage_series(all_timelines, sample_s=max(60, t_span / 80),
                                 t_start=t_min, t_end=t_max)
    if series:
        ts_px        = [s['t'] * t_scale          for s in series]
        used_ram     = [s['used_ram']              for s in series]
        soft_res_ram = [s['soft_reserved_ram']     for s in series]
        alloc_ram    = [s['alloc_ram']             for s in series]
        used_cpu     = [s['used_vcpu']             for s in series]
        soft_res_cpu = [s['soft_reserved_vcpu']    for s in series]
        alloc_cpu    = [s['alloc_vcpu']            for s in series]

        max_alloc_ram = max(alloc_ram) if alloc_ram else 1.0
        max_alloc_cpu = max(alloc_cpu) if alloc_cpu else 1.0

        pool_sch     = [s.get('pool_schedulable', 0.0) for s in series]
        has_pool_sch = any(v > 0 for v in pool_sch)

        # RAM overview stacking (bottom → top), variable-pool view:
        #   spike consumption | normal use | soft-limit reserved | headroom | provisioned
        ax_usage.fill_between(ts_px, alloc_ram,
                              alpha=0.15, color='#d0cec8', step='post',
                              label=f'RAM provisioned  (peak {max_alloc_ram:.0f} GB)')
        spike_consumed_o = [s.get('spike_consumed', 0.0) for s in series]
        used_sch_o = [max(0.0, u - sc) for u, sc in zip(used_ram, spike_consumed_o)]

        b1 = spike_consumed_o                                             # top of spike
        b2 = [sc + u  for sc, u  in zip(b1, used_sch_o)]                 # top of normal use
        b3 = [max(bot, sc + sr)                                           # top of reservation
              for bot, sc, sr in zip(b2, b1, soft_res_ram)]

        if has_pool_sch:
            b4 = [max(res_top, sc + ps)                                   # top of headroom
                  for res_top, sc, ps in zip(b3, b1, pool_sch)]
            zeros = [0.0] * len(ts_px)
            ax_usage.fill_between(ts_px, zeros, b1,
                                  alpha=0.75, color='#b05a00', step='post',
                                  label='spike pool consumed')
            ax_usage.fill_between(ts_px, b1, b2,
                                  alpha=0.80, color='#A32D2D', step='post',
                                  label='RAM in use (schedulable)')
            ax_usage.fill_between(ts_px, b2, b3,
                                  alpha=0.50, color='#c07070', step='post',
                                  label='RAM soft-limit reserved')
            ax_usage.fill_between(ts_px, b3, b4,
                                  alpha=0.40, color='#c87d10', step='post',
                                  label='schedulable headroom')
            ax_usage.step(ts_px, pool_sch, where='post', color='#7a4800',
                          linewidth=0.8, linestyle='--', alpha=0.55)
        else:
            ax_usage.fill_between(ts_px, b1, b2,
                                  alpha=0.80, color='#A32D2D', step='post',
                                  label='RAM in use')
            ax_usage.fill_between(ts_px, b2, b3,
                                  alpha=0.50, color='#c07070', step='post',
                                  label='RAM soft-limit reserved')
        ax_usage.set_ylabel('RAM (GB)', fontfamily=FONT, fontsize=8, color='#A32D2D')
        ax_usage.set_ylim(0, max_alloc_ram * 1.1)
        ax_usage.tick_params(axis='y', labelsize=7, colors='#A32D2D')

        from matplotlib.patches import Patch
        from matplotlib.lines import Line2D
        ram_handles = [
            Patch(color='#b05a00', alpha=0.80, label='spike pool consumed'),
            Patch(color='#A32D2D', alpha=0.80, label='RAM in use (schedulable)'),
            Patch(color='#c07070', alpha=0.55, label='RAM soft-limit reserved'),
            Patch(color='#c87d10', alpha=0.45, label='schedulable headroom'),
            Patch(color='#d0cec8', alpha=0.35,
                  label=f'RAM provisioned  (peak {max_alloc_ram:.0f} GB)'),
        ] + ([Line2D([0], [0], color='#7a4800', linewidth=1.0,
                     linestyle='--', alpha=0.7, label='schedulable ceiling')]
             if has_pool_sch else [])
        ax_usage.legend(handles=ram_handles, fontsize=7, loc='upper right',
                        framealpha=0.9, ncol=3)
        ax_usage.set_facecolor('#fafaf9')
        ax_usage.grid(True, axis='x', alpha=0.15)
        ax_usage.tick_params(axis='x', labelbottom=False)

        peak_cpu = max(used_cpu, default=0)
        # Provisioned ceiling drawn first (background) so the unused fraction
        # shows as light grey rather than white — mirrors the RAM panel layout.
        ax_cpu.fill_between(ts_px, alloc_cpu,
                            alpha=0.12, color='#888', step='post')
        ax_cpu.fill_between(ts_px, soft_res_cpu,
                            alpha=0.22, color='#185FA5', step='post',
                            label='CPU soft-limit reserved')
        ax_cpu.fill_between(ts_px, used_cpu,
                            alpha=0.70, color='#185FA5', step='post',
                            label=f'CPU effective  (peak {peak_cpu:.0f} vCPU)')
        ax_cpu.step(ts_px, alloc_cpu, color='#888', linewidth=0.7,
                    where='post', linestyle='--',
                    label=f'CPU provisioned  (peak {max_alloc_cpu:.0f} vCPU)')
        ax_cpu.set_ylabel('vCPU', fontfamily=FONT, fontsize=8, color='#185FA5')
        ax_cpu.set_ylim(0, max_alloc_cpu * 1.1)
        ax_cpu.tick_params(axis='y', labelsize=7, colors='#185FA5')
        ax_cpu.legend(fontsize=7, loc='upper right', framealpha=0.9, ncol=3)
        ax_cpu.set_facecolor('#fafaf9')
        ax_cpu.grid(True, axis='x', alpha=0.15)

    # ── Pool-wide storage capacity panel (BSIM-94) ────────────────────
    if ax_ov_stor is not None:
        ax_cpu.tick_params(axis='x', labelbottom=False)
        # Build Σ pool_capacity(t) step function from all nodes' expansion events
        # Collect all unique event times across all nodes
        all_stor_evts: list[tuple[float, float]] = []  # (t, delta_gb)
        for _, nd in all_timelines.items():
            expanded_nd = nd.get('pool_expanded', [])
            gen_evts_nd = nd.get('pool_gen_events', [])
            if gen_evts_nd:
                for ev_type, t, gen_id, cap_gb in gen_evts_nd:
                    if ev_type == 'storage_gen_opened':
                        all_stor_evts.append((t, +cap_gb))
                    elif ev_type == 'storage_gen_released':
                        all_stor_evts.append((t, -cap_gb))
            elif expanded_nd:
                # Initial capacity delta at launch_t
                init_cap  = expanded_nd[0][1]  # old_gb of first expansion
                final_cap = expanded_nd[-1][2]  # new_gb of last expansion
                launch = nd.get('launch_t', t_min)
                term   = nd.get('term_t',   t_max)
                all_stor_evts.append((launch, +init_cap))
                for t_exp, old_gb, new_gb in expanded_nd:
                    all_stor_evts.append((t_exp, +(new_gb - old_gb)))
                all_stor_evts.append((term, -final_cap))
        if all_stor_evts:
            all_stor_evts.sort(key=lambda x: x[0])
            ts_stor  = [t_min]
            cap_stor = [0.0]
            running  = 0.0
            for t_ev, delta in all_stor_evts:
                ts_stor.append(t_ev); cap_stor.append(running)
                running += delta
                ts_stor.append(t_ev); cap_stor.append(running)
            ts_stor.append(t_max); cap_stor.append(running)
            ts_stor_px = [(t - t_min) * t_scale for t in ts_stor]
            ax_ov_stor.fill_between(ts_stor_px, cap_stor, step='post',
                                    alpha=0.40, color='#00796b',
                                    label=f'Σ pool capacity  (peak {max(cap_stor):.0f} GB)')
            ax_ov_stor.step(ts_stor_px, cap_stor, where='post',
                            color='#00695c', linewidth=1.2)
            ax_ov_stor.set_ylabel('storage GB', fontfamily=FONT, fontsize=8, color='#00695c')
            ax_ov_stor.tick_params(axis='y', labelsize=7, colors='#00695c')
            ax_ov_stor.set_facecolor('#f0f8f5')
            ax_ov_stor.grid(True, axis='x', alpha=0.15)
            ax_ov_stor.legend(fontsize=7, loc='upper right', framealpha=0.9)

    ax_bottom_ov = ax_ov_stor if ax_ov_stor is not None else ax_cpu
    tick_s = _nice_tick_seconds(t_span)
    ticks  = np.arange(0, t_span + tick_s, tick_s)
    ax_bottom_ov.set_xticks(ticks * t_scale)
    ax_bottom_ov.set_xticklabels(
        [f'{t/60:.0f}m' for t in ticks], fontsize=8, fontfamily=FONT)
    ax_bottom_ov.set_xlabel('simulated time', fontfamily=FONT, fontsize=9)
    if ax_ov_stor is None:
        ax_cpu.set_xlabel('simulated time', fontfamily=FONT, fontsize=9)

    fig.align_ylabels([ax_usage, ax_cpu] + ([ax_ov_stor] if ax_ov_stor else []))
    exts = ('png',) if png_only else ('png', 'svg')
    for ext in exts:
        fig.savefig(out / f'{filename}.{ext}', dpi=130, bbox_inches='tight')
    plt.close(fig)
    ext_label = 'png' if png_only else 'png,svg'
    print(f'  {filename}.{{{ext_label}}}  ({n_nodes} nodes)')


def chart_overview(
    node_timelines: dict,
    metadata: dict,
    out: Path,
    scheduler_type: str = 'batch',
) -> None:
    """
    Overview chart: all nodes.
    Layout: Gantt (top) stacked above CPU/RAM usage (bottom).

    When more nodes exist than fit at MAX_GANTT_H:
      overview.png       — summary, top max_rows nodes by cost
      overview_p01..pN.png — full-coverage pages, all nodes by launch time,
                             max_rows per page, pool-wide metrics on every page
    """
    nodes_by_launch = sorted(node_timelines.items(), key=lambda x: x[1]['launch_t'])
    if not nodes_by_launch:
        return

    t_min = min(n['launch_t'] for _, n in nodes_by_launch)
    t_max = max(n['term_t']   for _, n in nodes_by_launch)
    if t_max == t_min:
        return

    n_total  = len(nodes_by_launch)
    row_h    = max(0.35, min(0.9, 36 / n_total))
    MAX_GANTT_H = 80.0
    max_rows = max(1, int((MAX_GANTT_H - 1.5) / (row_h * 1.35)))

    # Summary overview — top max_rows by cost, or all nodes when they fit
    if n_total <= max_rows:
        summary_nodes = nodes_by_launch
        title_suffix  = ''
    else:
        summary_nodes = sorted(nodes_by_launch,
                               key=lambda x: x[1]['cost'], reverse=True)[:max_rows]
        title_suffix  = f'  [top {max_rows} of {n_total} by cost]'

    _render_overview_page(
        gantt_nodes=summary_nodes, all_timelines=node_timelines,
        metadata=metadata, out=out, scheduler_type=scheduler_type,
        t_min=t_min, t_max=t_max,
        filename='overview', title_suffix=title_suffix,
        png_only=(n_total > 120),
    )

    # Paginated full-coverage overviews (only when truncation was needed)
    if n_total > max_rows:
        n_pages = (n_total + max_rows - 1) // max_rows
        for pi in range(n_pages):
            start      = pi * max_rows
            end        = min(start + max_rows, n_total)
            page_nodes = nodes_by_launch[start:end]
            # Time bounds for this page: earliest launch → latest termination
            # of the nodes shown.  The x-axis stretches to fill the gantt width
            # so shorter windows give wider bars and better readability.
            page_t_min = min(n['launch_t'] for _, n in page_nodes)
            page_t_max = max(n['term_t']   for _, n in page_nodes)
            _render_overview_page(
                gantt_nodes=page_nodes,
                all_timelines=node_timelines,
                metadata=metadata, out=out, scheduler_type=scheduler_type,
                t_min=page_t_min, t_max=page_t_max,
                filename=f'overview_p{pi + 1:02d}',
                title_suffix=(f'  [p{pi + 1}/{n_pages}'
                              f'  nodes {start + 1}–{end} of {n_total}]'),
                png_only=True,
            )
        print(f'  overview_p01..p{n_pages:02d}.png'
              f'  ({n_pages} pages × up to {max_rows} nodes/page)')


# ---------------------------------------------------------------------------
# BSIM-94: Storage band helpers
# ---------------------------------------------------------------------------

def _build_pool_capacity_steps(
    node: dict, t_min: float, t_max: float,
) -> tuple[list[float], list[float]]:
    """Return (times, capacity_gb) step series for a single node's pool.

    For the Batch single-pool model: starts at initial capacity (inferred from
    the first STORAGE_POOL_EXPANDED 'old_gb'), jumps at each expansion.
    For K8S generational: accumulated capacity across open generations.
    Uses STORAGE_GEN_OPENED to add capacity and STORAGE_GEN_RELEASED to subtract.
    Falls back to STORAGE_POOL_EXPANDED if no gen events.
    """
    gen_events = node.get('pool_gen_events', [])
    expanded   = node.get('pool_expanded', [])

    if gen_events:
        # K8S generational path
        ts: list[float]  = [t_min]
        cap_series: list[float] = [0.0]
        current_cap: dict[int, float] = {}  # gen_id → capacity
        for ev_type, t, gen_id, cap_gb in sorted(gen_events, key=lambda x: x[1]):
            total_before = sum(current_cap.values())
            if ev_type == 'storage_gen_opened':
                current_cap[gen_id] = cap_gb
            elif ev_type == 'storage_gen_released':
                current_cap.pop(gen_id, None)
            total_after = sum(current_cap.values())
            if total_before != total_after:
                ts.append(t); cap_series.append(total_before)  # step down first
                ts.append(t); cap_series.append(total_after)
        ts.append(t_max); cap_series.append(sum(current_cap.values()))
        return ts, cap_series

    # Batch / single-pool path
    if not expanded:
        return [], []
    # Infer initial capacity from first expansion's old_gb
    initial_cap = expanded[0][1]   # old_gb at first expansion
    ts    = [t_min, expanded[0][0]]
    caps  = [initial_cap, initial_cap]
    cur   = initial_cap
    for t, old_gb, new_gb in expanded:
        ts.append(t); caps.append(cur)
        ts.append(t); caps.append(new_gb)
        cur = new_gb
    ts.append(t_max); caps.append(cur)
    return ts, caps


def _draw_storage_panel(
    ax, node: dict, t_min: float, t_scale: float, scheduler_type: str,
) -> None:
    """Render pool capacity step function on ax."""
    ts_raw, caps = _build_pool_capacity_steps(node, t_min, node['term_t'])
    if not ts_raw:
        ax.text(0.5, 0.5, 'no storage events', transform=ax.transAxes,
                ha='center', va='center', fontsize=8, color='#888')
        ax.set_yticks([])
        return

    ts_px = [(t - t_min) * t_scale for t in ts_raw]
    gen_events = node.get('pool_gen_events', [])
    is_gen = bool(gen_events)

    if is_gen:
        # Shade alternating generations
        GEN_COLORS = ['#2e7d32', '#1565c0']
        gen_spans: dict[int, list] = {}
        for ev_type, t, gen_id, cap_gb in sorted(gen_events, key=lambda x: x[1]):
            if ev_type == 'storage_gen_opened':
                gen_spans[gen_id] = [t, None, cap_gb]
            elif ev_type == 'storage_gen_released' and gen_id in gen_spans:
                gen_spans[gen_id][1] = t
        for gen_id, (t0, t1, cap_gb) in gen_spans.items():
            if t1 is None:
                t1 = node['term_t']
            x0 = (t0 - t_min) * t_scale
            x1 = (t1 - t_min) * t_scale
            c  = GEN_COLORS[gen_id % len(GEN_COLORS)]
            ax.axvspan(x0, x1, ymin=0, ymax=1, alpha=0.12, color=c)
            ax.fill_between([x0, x1], [cap_gb, cap_gb], step='post',
                            alpha=0.35, color=c, label=f'gen {gen_id}  ({cap_gb:.0f} GB)')
            # Mark release with a vertical drop line
            if gen_spans[gen_id][1] is not None:
                rx = (gen_spans[gen_id][1] - t_min) * t_scale
                ax.axvline(rx, color=c, linewidth=1.0, alpha=0.7, linestyle=':')
    else:
        ax.fill_between(ts_px, caps, step='post', alpha=0.40, color='#00796b',
                        label=f'pool capacity  ({max(caps):.0f} GB peak)')
        # Mark expansion events with vertical lines
        for t, old_gb, new_gb in node.get('pool_expanded', []):
            ex = (t - t_min) * t_scale
            ax.axvline(ex, color='#00796b', linewidth=1.2, alpha=0.75)
            ax.text(ex + 0.5, new_gb * 0.5, f'+{new_gb - old_gb:.0f}',
                    fontsize=6, color='#00796b', va='center')

    ax.step(ts_px, caps, where='post', color='#00695c', linewidth=1.3)
    if node.get('pool_exhausted'):
        ax.set_title('STORAGE EXHAUSTED', color='red', fontsize=7)
    ax.set_ylabel('pool GB', fontfamily=FONT, fontsize=8, color='#00695c')
    ax.tick_params(axis='y', labelsize=7, colors='#00695c')
    ax.set_facecolor('#f0f8f5')
    ax.grid(True, axis='x', alpha=0.15)
    if not is_gen:
        ax.legend(fontsize=6.5, loc='upper right', framealpha=0.9)


# ---------------------------------------------------------------------------
# Per-node detail chart
# ---------------------------------------------------------------------------

def chart_per_node(
    node_id: str,
    node: dict,
    out: Path,
    scheduler_type: str = 'batch',
) -> None:
    """
    Per-node detail chart.

    Layout: Gantt (top) + CPU/RAM usage (middle) + stacked CPU waste (bottom).
    BSIM-80: absolute units on all y-axes.
    BSIM-82: stacked CPU waste panel.
    """
    jobs = node['jobs']
    if not jobs:
        return

    t_min  = node['launch_t']
    t_max  = node['term_t']
    t_span = t_max - t_min
    if t_span == 0:
        return

    node_ram_gb  = node['ram_gb']
    node_vcpu    = node.get('vcpu', 64)
    has_waste    = bool(node.get('cpu_waste_steps'))

    n_rows  = len(jobs) + 1
    row_h   = 0.7
    gantt_h = max(2.5, n_rows * row_h * 1.8 + 1.0)
    usage_h = 2.2
    waste_h = 1.8 if has_waste else 0
    fig_w   = 15
    has_storage = bool(node.get('pool_expanded') or node.get('pool_gen_events'))
    storage_h   = 1.4 if has_storage else 0

    n_panels    = 2 + (1 if has_waste else 0) + (1 if has_storage else 0)
    h_ratios    = [gantt_h, usage_h] + ([waste_h] if has_waste else []) + ([storage_h] if has_storage else [])
    total_h     = gantt_h + usage_h + waste_h + storage_h + 0.6

    fig = plt.figure(figsize=(fig_w, total_h))
    gs  = fig.add_gridspec(n_panels, 1, height_ratios=h_ratios, hspace=0.06)
    ax_gantt    = fig.add_subplot(gs[0])
    ax_usage    = fig.add_subplot(gs[1], sharex=ax_gantt)
    panel_idx   = 2
    ax_waste    = fig.add_subplot(gs[panel_idx], sharex=ax_gantt) if has_waste else None
    if has_waste: panel_idx += 1
    ax_storage  = fig.add_subplot(gs[panel_idx], sharex=ax_gantt) if has_storage else None

    t_scale = (fig_w - 2.5) / t_span

    # ── Node lifecycle row ─────────────────────────────────────────────
    _draw_node_row(ax_gantt, node, n_rows - 1, row_h,
                   t_min, t_scale, fig_w, scheduler_type=scheduler_type)
    soft = node.get('soft_cpu', '?')
    hard = node.get('hard_cpu', '?')
    ax_gantt.text(-0.005, n_rows - 1,
                  f"node  {node['instance']}  cpu:{soft}/{hard}  {node_ram_gb:.0f}GB",
                  transform=ax_gantt.get_yaxis_transform(),
                  ha='right', va='center', fontsize=8,
                  fontfamily=FONT, fontweight='bold')

    # ── Per-job rows ──────────────────────────────────────────────────
    for i, job in enumerate(jobs):
        y     = i
        hatch = CENTROID_HATCH.get(job['centroid'], '')
        for ph, t0, t1 in job['phases']:
            if t1 <= t0:
                continue
            ax_gantt.barh(
                y, (t1 - t0) * t_scale, row_h * 0.75,
                left=(t0 - t_min) * t_scale,
                color=PHASE_COLOR.get(ph, '#aaa'),
                hatch=hatch, edgecolor='white', linewidth=0.4, zorder=3,
            )
            if (t1 - t0) * t_scale > 22:
                ax_gantt.text(
                    (t0 - t_min + (t1 - t0) / 2) * t_scale, y,
                    ph[:4], ha='center', va='center',
                    fontsize=7, color='white', fontfamily=FONT, zorder=4,
                )

        res_gb = job.get('reserved_ram_gb')
        if res_gb is not None:
            tick_x = (job['end_t'] - t_min) * t_scale
            ax_gantt.plot([tick_x, tick_x],
                          [y - row_h * 0.45, y + row_h * 0.45],
                          color='#222', linewidth=1.4, zorder=6)
            ax_gantt.text(tick_x + 0.06, y, f'{res_gb:.0f}G',
                          fontsize=6.5, va='center', color='#222',
                          fontfamily=FONT, zorder=7)

        if job['status'] == 'crash':
            ax_gantt.plot(
                (job['end_t'] - t_min) * t_scale, y,
                'x', color='#e74c3c', markersize=7, mew=2, zorder=5,
            )

        res_str  = f'  ram:{res_gb:.0f}G' if res_gb is not None else ''
        job_soft = job.get('soft_cpu', '?')
        job_hard = job.get('hard_cpu', '?')
        label = (f"{job['centroid'].split('_')[-1].upper()}"
                 f"  {job['job_id'][:8]}"
                 f"  cpu:{job_soft}/{job_hard}{res_str}")
        ax_gantt.text(-0.005, y, label,
                      transform=ax_gantt.get_yaxis_transform(),
                      ha='right', va='center', fontsize=7, fontfamily=FONT)

    ax_gantt.set_ylim(-0.6, n_rows - 0.4)
    ax_gantt.set_yticks([])
    ax_gantt.set_xlim(0, t_span * t_scale)
    ax_gantt.tick_params(axis='x', labelbottom=False)
    ax_gantt.set_title(
        f'Node {node_id}  —  {node["instance"]}'
        f'  ({node_ram_gb:.0f} GB  {node_vcpu} vCPU  ${node["hourly_usd"]:.4f}/hr)'
        f'  lifespan {t_span:.0f}s  ·  {len(jobs)} jobs  ·  ${node["cost"]:.4f}',
        fontfamily=FONT, fontsize=9,
    )
    ax_gantt.legend(handles=_legend_handles(), fontsize=8,
                    loc='upper right', ncol=2, framealpha=0.9)
    ax_gantt.grid(True, axis='x', alpha=0.18, zorder=1)
    ax_gantt.set_facecolor('#fafaf9')

    # ── Middle panel: CPU/RAM (BSIM-80: absolute units) ──────────────
    node_tl = {node_id: node}
    series  = _build_usage_series(node_tl, sample_s=max(10, t_span / 60))

    if series:
        ts_px     = [s['t'] * t_scale          for s in series]
        used_ram  = [s['used_ram']              for s in series]
        soft_res  = [s['soft_reserved_ram']     for s in series]
        alloc_ram = [s['alloc_ram']             for s in series]
        eff_sch   = node.get('effective_schedulable_gb')   # None for batch

        # Layer 1: provisioned ceiling (background)
        ax_usage.fill_between(ts_px, alloc_ram,
                              alpha=0.18, color='#d0cec8', step='post',
                              label=f'RAM provisioned  ({node_ram_gb:.0f} GB)')
        spike_consumed_s = [s.get('spike_consumed', 0.0) for s in series]
        # Schedulable-zone usage = actual RAM minus the burst drawn from the spike
        # pool; keeps the dark-red fill below eff_sch and avoids double-counting
        # the burst in both the used_ram region and the spike-consumed region.
        used_sch = [max(0.0, u - sc) for u, sc in zip(used_ram, spike_consumed_s)]

        # Layer 2 (k8s/k8splus): schedulable headroom [soft_res, eff_sch].
        # Shows space still available for additional soft-limit reservations.
        if eff_sch is not None and eff_sch > 0:
            amber_bot = [min(s, eff_sch) for s in soft_res]
            ax_usage.fill_between(ts_px, amber_bot, [eff_sch] * len(ts_px),
                                  alpha=0.38, color='#c87d10', step='post',
                                  label='schedulable headroom')
        # Layer 3: soft-limit reservations [0, soft_res]
        ax_usage.fill_between(ts_px, soft_res,
                              alpha=0.45, color='#c07070', step='post',
                              label='soft-limit reserved')
        # Layer 4: schedulable-zone RAM in use (burst excluded — shown in spike zone)
        ax_usage.fill_between(ts_px, used_sch,
                              alpha=0.80, color='#A32D2D', step='post',
                              label='RAM in use (schedulable zone)')
        # Layers 5+6 (k8s/k8splus): spike headroom pool [eff_sch, node_ram_gb].
        # Layer 5: consumed portion [eff_sch, eff_sch + spike_consumed] — drawn
        #   in a warm amber so it reads as "pool in use, not yet returned."
        # Layer 6: available portion [eff_sch + spike_consumed, node_ram_gb] —
        #   hatched grey; its bottom edge rises as the semaphore is acquired and
        #   drops back when the preprocess burst is released.
        spike_gb = node.get('spike_headroom_gb')
        if eff_sch is not None and eff_sch > 0 and spike_gb is not None:
            spike_top = [min(eff_sch + sc, eff_sch + spike_gb)
                         for sc in spike_consumed_s]
            spike_ceil = eff_sch + spike_gb          # top of spike pool
            os_gb = node_ram_gb - spike_ceil         # actual OS overhead
            ax_usage.fill_between(ts_px,
                                  [eff_sch] * len(ts_px), spike_top,
                                  alpha=0.65, color='#b05a00', step='post',
                                  label=f'spike pool consumed (preprocess burst)')
            ax_usage.fill_between(ts_px,
                                  spike_top, [spike_ceil] * len(ts_px),
                                  alpha=0.28, color='#b8b8b8', step='post',
                                  hatch='///', edgecolor='#888888', linewidth=0.3,
                                  label=f'spike pool available  ({spike_gb:.0f} GB = max preload)')
            # OS overhead cap — always present, distinct from spike pool
            ax_usage.axhspan(spike_ceil, node_ram_gb,
                             facecolor='#909090', alpha=0.55,
                             label=f'OS overhead  ({os_gb:.0f} GB)')
            ax_usage.axhline(eff_sch, color='#555', linewidth=1.2,
                             linestyle='--', alpha=0.75)
        ax_usage.axhline(node_ram_gb, color='#888', linewidth=0.7,
                         linestyle='--', alpha=0.4)
        ax_usage.set_ylabel('RAM (GB)', fontfamily=FONT, fontsize=8, color='#A32D2D')
        ax_usage.set_ylim(0, node_ram_gb * 1.05)
        ax_usage.tick_params(axis='y', labelsize=7, colors='#A32D2D')
        ax_usage.legend(fontsize=7, loc='upper right', framealpha=0.9)
        ax_usage.set_facecolor('#fafaf9')
        ax_usage.grid(True, axis='x', alpha=0.18)

    # ── CPU waste panel (BSIM-82) ─────────────────────────────────────
    if ax_waste is not None:
        _draw_cpu_waste_panel(ax_waste, node, t_min, t_scale)
        ax_waste.tick_params(axis='x', labelbottom=False)

    # ── Storage band panel (BSIM-94) ──────────────────────────────────
    if ax_storage is not None:
        _draw_storage_panel(ax_storage, node, t_min, t_scale, scheduler_type)
        ax_storage.tick_params(axis='x', labelbottom=False)

    # Shared x-axis ticks on the lowest visible panel
    ax_bottom = ax_storage if ax_storage is not None else (ax_waste if ax_waste is not None else ax_usage)
    tick_s = _nice_tick_seconds(t_span)
    ticks  = np.arange(0, t_span + tick_s, tick_s)
    ax_bottom.set_xticks(ticks * t_scale)
    ax_bottom.set_xticklabels(
        [f'{t:.0f}s' if t_span < 600 else f'{t/60:.1f}m' for t in ticks],
        fontsize=8, fontfamily=FONT,
    )
    ax_bottom.tick_params(axis='x', labelbottom=True)
    ax_bottom.set_xlabel('time since node launch', fontfamily=FONT, fontsize=9)

    for ext in ('png', 'svg'):
        fig.savefig(out / f'node_{node_id}.{ext}', dpi=130, bbox_inches='tight')
    plt.close(fig)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nice_tick_seconds(span: float) -> float:
    targets = [30, 60, 120, 300, 600, 900, 1800, 3600]
    for t in targets:
        if span / t <= 10:
            return t
    return 3600


def _legend_handles():
    return [
        mpatches.Patch(color=PHASE_COLOR['warmup'],     label='warmup'),
        mpatches.Patch(color=PHASE_COLOR['idle'],       label='idle'),
        mpatches.Patch(color=PHASE_COLOR['download'],   label='download'),
        mpatches.Patch(color=PHASE_COLOR['preprocess'], label='preprocess (RAM peak)'),
        mpatches.Patch(color=PHASE_COLOR['workhorse'],  label='workhorse (CPU)'),
        mpatches.Patch(color=PHASE_COLOR['upload'],     label='upload'),
    ]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Generate per-node job timeline charts for a scheduler run.'
    )
    parser.add_argument('--scheduler', choices=['batch', 'k8s', 'k8splus'], required=True)
    parser.add_argument('--events',
                        default='workloads/reference_4h_v1.json',
                        help='Path to event list JSON')
    parser.add_argument('--scheduler-config',
                        default='configs/scheduler_reference.yaml')
    parser.add_argument('--registry',
                        default='configs/instance_registry.yaml')
    parser.add_argument('--output',
                        default=None,
                        help='Output directory (default: results/node_timelines/<scheduler>)')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--overview-only', action='store_true',
                        help='Generate only the overview chart, not per-node charts')
    parser.add_argument('--max-per-node', type=int, default=None,
                        help='Maximum number of per-node charts to generate')
    parser.add_argument(
        '--event-log', default=None,
        help=(
            'Path to *_events.json saved by `python -m batch_sim simulate`. '
            'When provided, charts are built from this log without re-running '
            'the simulation (--event-log path). '
            'When absent, re-runs the simulation via run_one() (re-run path).'
        )
    )
    args = parser.parse_args()

    out = Path(args.output or f'results/node_timelines/{args.scheduler}')
    out.mkdir(parents=True, exist_ok=True)

    if args.event_log:
        print(f'[--event-log path]  Reading saved event log: {args.event_log}')
    else:
        print(f'[re-run path]  Re-running {args.scheduler} via run_one()...')

    os_overhead_gb = 0.0
    if args.scheduler in ('k8s', 'k8splus'):
        _sch_cfg = load_scheduler_config(args.scheduler_config)
        os_overhead_gb = getattr(_sch_cfg, 'k8s_os_overhead_gb', 0.0)

    node_timelines, metadata = run_and_extract(
        event_list_path=args.events,
        scheduler_type=args.scheduler,
        cfg_path=args.scheduler_config,
        registry_path=args.registry,
        seed=args.seed,
        event_log_path=args.event_log,
        os_overhead_gb=os_overhead_gb,
    )
    print(f'  {metadata["total_nodes"]} nodes  '
          f'{metadata["total_jobs"]} jobs  '
          f'${metadata["total_cost"]:.2f} total')

    with open(out / 'summary.json', 'w') as f:
        serial = {nid: {k: v for k, v in nd.items()} for nid, nd in node_timelines.items()}
        json.dump({'metadata': metadata, 'nodes': serial}, f, indent=2)
    print(f'  summary.json')

    print(f'Writing charts to {out}')
    chart_overview(node_timelines, metadata, out, scheduler_type=args.scheduler)

    if not args.overview_only:
        nodes_sorted = sorted(node_timelines.items(), key=lambda x: x[1]['launch_t'])
        limit = args.max_per_node or len(nodes_sorted)
        for i, (nid, node) in enumerate(nodes_sorted[:limit]):
            chart_per_node(nid, node, out, scheduler_type=args.scheduler)
            if (i + 1) % 10 == 0 or (i + 1) == min(limit, len(nodes_sorted)):
                print(f'  per-node charts: {i+1}/{min(limit, len(nodes_sorted))}')

    print(f'Done.  {len(list(out.glob("*.png")))} PNG files in {out}')


if __name__ == '__main__':
    main()
