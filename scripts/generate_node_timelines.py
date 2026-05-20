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
from batch_sim.registry.instance_registry import InstanceRegistry
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
        cooloff  = el.metadata.get('cooloff_seconds', 0.0)

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
    node_cpu_waste: dict[str, list] = {}
    for e in log:
        if e.event_type == EventType.CPU_WASTE:
            nid = e.data.get('node_id')
            if nid:
                node_cpu_waste.setdefault(nid, []).append((
                    e.sim_time,
                    e.data.get('effective_vcpu', 0.0),
                    e.data.get('io_ineligible_waste', 0.0),
                    e.data.get('thread_count_waste', 0.0),
                    e.data.get('hard_limit_waste', 0.0),
                ))

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
                res_gb = prof.preprocess_steady_ram_gb if prof else None

            j_soft = prof.workhorse_declared_vcpu if prof else None
            j_hard = j_soft

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
                'phase_ram_gb':    phase_ram,   # BSIM-81
                'phase_vcpu':      phase_vcpu,  # BSIM-81
            })
        jobs_here.sort(key=lambda j: j['start_t'])

        node_soft = max((j.get('soft_cpu') or 0 for j in jobs_here), default=0)
        node_hard = max((j.get('hard_cpu') or 0 for j in jobs_here), default=0)

        node_timelines[nid] = {
            'instance':        inst_name,
            'ram_gb':          inst.ram_gb,
            'vcpu':            inst.vcpu,           # BSIM-80
            'hourly_usd':      inst.hourly_price_usd,
            'launch_t':        launch_t,
            'ready_t':         node_ready_time.get(nid, launch_t),
            'term_t':          term_t,
            'cost':            round(cost, 4),
            'jobs':            jobs_here,
            'soft_cpu':        node_soft if node_soft > 0 else None,
            'hard_cpu':        node_hard if node_hard > 0 else None,
            'reserved_ram_gb': round(max(
                (j.get('reserved_ram_gb') or 0 for j in jobs_here), default=0
            ), 1),
            'cpu_waste_steps': sorted(             # BSIM-82
                node_cpu_waste.get(nid, []), key=lambda x: x[0]
            ),
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

def _build_usage_series(node_timelines: dict, sample_s: float = 60.0) -> list[dict]:
    """
    Build a time series of pool-wide RAM and CPU usage at sample_s intervals.

    BSIM-81: RAM uses actual per-phase values from job profiles (phase_ram_gb).
    CPU uses per-node CPU_WASTE step functions (cpu_waste_steps) rather than
    hard-coded phase constants.

    Returns list of {t, alloc_ram, used_ram, alloc_vcpu, used_vcpu, n_nodes}.
    All values are in physical units (GB, vCPU).
    """
    if not node_timelines:
        return []

    t_min = min(n['launch_t'] for n in node_timelines.values())
    t_max = max(n['term_t']   for n in node_timelines.values())

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

    def _node_effective_vcpu(nid: str, t: float) -> float:
        """Step-function lookup of effective_vcpu from cpu_waste_steps."""
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
        used_ram   = 0.0
        used_vcpu  = 0.0

        active_set = {nid for nid, _ in active_nodes}
        for (nid, jid), segs in phase_resource.items():
            if nid not in active_set:
                continue
            for ph, t0, t1, ram in segs:
                if t0 <= t < t1:
                    used_ram += ram

        for nid in active_set:
            used_vcpu += _node_effective_vcpu(nid, t)

        series.append({
            't':          round(t - t_min),
            'alloc_ram':  round(alloc_ram, 1),
            'used_ram':   round(used_ram, 1),
            'alloc_vcpu': round(alloc_vcpu, 1),
            'used_vcpu':  round(used_vcpu, 1),
            'n_nodes':    len(active_nodes),
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

def chart_overview(
    node_timelines: dict,
    metadata: dict,
    out: Path,
    scheduler_type: str = 'batch',
) -> None:
    """
    Overview chart: all nodes.
    Layout: Gantt (top panel) stacked above CPU/RAM usage (bottom panel).
    BSIM-80: y-axes use absolute physical units (GB, vCPU), not percentages.
    """
    nodes = sorted(node_timelines.values(), key=lambda n: n['launch_t'])
    if not nodes:
        return

    t_min  = min(n['launch_t'] for n in nodes)
    t_max  = max(n['term_t']   for n in nodes)
    t_span = t_max - t_min
    if t_span == 0:
        return

    n_nodes = len(nodes)
    row_h   = max(0.35, min(0.9, 36 / n_nodes))
    gantt_h = max(4, n_nodes * row_h * 1.35 + 1.5)
    usage_h = 2.5
    fig_w   = 18

    fig = plt.figure(figsize=(fig_w, gantt_h + usage_h + 0.8))
    gs  = fig.add_gridspec(2, 1, height_ratios=[gantt_h, usage_h], hspace=0.08)
    ax_gantt = fig.add_subplot(gs[0])
    ax_usage = fig.add_subplot(gs[1], sharex=ax_gantt)

    t_scale = (fig_w - 2) / t_span

    for i, node in enumerate(nodes):
        y = n_nodes - 1 - i
        _draw_node_row(ax_gantt, node, y, row_h, t_min, t_scale,
                       fig_w, scheduler_type=scheduler_type)
        soft = node.get('soft_cpu', '?')
        hard = node.get('hard_cpu', '?')
        ram  = node.get('reserved_ram_gb', node['ram_gb'])
        label = (f"{node['instance']}  ${node['cost']:.2f}"
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
        f'{n_nodes} nodes · {metadata["total_jobs"]} jobs · '
        f'${metadata["total_cost"]:.2f}  '
        f'(left labels: instance  cost  jobs  cpu:soft/hard  ram:limit)',
        fontfamily=FONT, fontsize=9, loc='left',
    )

    # ── Bottom panel: CPU and RAM (BSIM-80: absolute units) ──────────────
    series = _build_usage_series(node_timelines, sample_s=max(60, t_span / 80))
    if series:
        ts_px     = [s['t'] * t_scale for s in series]
        used_ram  = [s['used_ram']    for s in series]
        alloc_ram = [s['alloc_ram']   for s in series]
        used_cpu  = [s['used_vcpu']   for s in series]
        alloc_cpu = [s['alloc_vcpu']  for s in series]

        max_alloc_ram = max(alloc_ram) if alloc_ram else 1.0
        max_alloc_cpu = max(alloc_cpu) if alloc_cpu else 1.0

        # RAM: absolute GB
        ax_usage.fill_between(ts_px, alloc_ram,
                              alpha=0.25, color='#d0cec8', step='post',
                              label=f'RAM provisioned  (peak {max_alloc_ram:.0f} GB)')
        ax_usage.fill_between(ts_px, used_ram,
                              alpha=0.65, color='#A32D2D', step='post',
                              label='RAM used')
        ax_usage.set_ylabel('RAM (GB)', fontfamily=FONT, fontsize=8, color='#A32D2D')
        ax_usage.set_ylim(0, max_alloc_ram * 1.1)
        ax_usage.tick_params(axis='y', labelsize=7, colors='#A32D2D')

        # CPU: absolute vCPU on twin axis
        ax_cpu2 = ax_usage.twinx()
        ax_cpu2.step(ts_px, alloc_cpu, color='#d0cec8', linewidth=0.8,
                     where='post', linestyle='--', label='vCPU provisioned')
        ax_cpu2.step(ts_px, used_cpu, color='#185FA5', linewidth=1.4,
                     where='post', label=f'CPU effective  (peak {max(used_cpu, default=0):.0f} vCPU)')
        ax_cpu2.set_ylabel('CPU (vCPU)', fontfamily=FONT, fontsize=8, color='#185FA5')
        ax_cpu2.set_ylim(0, max_alloc_cpu * 1.1)
        ax_cpu2.tick_params(axis='y', labelsize=7, colors='#185FA5')

        from matplotlib.patches import Patch
        from matplotlib.lines import Line2D
        handles = [
            Patch(color='#d0cec8', alpha=0.5,
                  label=f'RAM provisioned  ({max_alloc_ram:.0f} GB)'),
            Patch(color='#A32D2D', alpha=0.65, label='RAM used'),
            Line2D([0], [0], color='#185FA5', lw=1.4,
                   label=f'CPU effective  (peak {max(used_cpu, default=0):.0f} vCPU)'),
        ]
        ax_usage.legend(handles=handles, fontsize=7, loc='upper right', framealpha=0.9)
        ax_usage.set_facecolor('#fafaf9')
        ax_usage.grid(True, axis='x', alpha=0.15)

    tick_s = _nice_tick_seconds(t_span)
    ticks  = np.arange(0, t_span + tick_s, tick_s)
    ax_usage.set_xticks(ticks * t_scale)
    ax_usage.set_xticklabels(
        [f'{t/60:.0f}m' for t in ticks], fontsize=8, fontfamily=FONT)
    ax_usage.set_xlabel('simulated time', fontfamily=FONT, fontsize=9)

    fig.align_ylabels([ax_usage])
    for ext in ('png', 'svg'):
        fig.savefig(out / f'overview.{ext}', dpi=130, bbox_inches='tight')
    plt.close(fig)
    print(f'  overview.{{png,svg}}  ({n_nodes} nodes)')


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

    n_panels    = 3 if has_waste else 2
    h_ratios    = [gantt_h, usage_h, waste_h] if has_waste else [gantt_h, usage_h]
    total_h     = gantt_h + usage_h + waste_h + 0.6

    fig = plt.figure(figsize=(fig_w, total_h))
    gs  = fig.add_gridspec(n_panels, 1, height_ratios=h_ratios, hspace=0.06)
    ax_gantt = fig.add_subplot(gs[0])
    ax_usage = fig.add_subplot(gs[1], sharex=ax_gantt)
    ax_waste = fig.add_subplot(gs[2], sharex=ax_gantt) if has_waste else None

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
        ts_px    = [s['t'] * t_scale for s in series]
        used_ram = [s['used_ram']    for s in series]
        alloc_ram= [s['alloc_ram']   for s in series]
        used_cpu = [s['used_vcpu']   for s in series]

        # RAM: absolute GB, ceiling = node's physical RAM
        ax_usage.fill_between(ts_px, alloc_ram,
                              alpha=0.25, color='#d0cec8', step='post',
                              label=f'RAM provisioned  ({node_ram_gb:.0f} GB)')
        ax_usage.fill_between(ts_px, used_ram,
                              alpha=0.65, color='#A32D2D', step='post',
                              label='RAM used')
        ax_usage.axhline(node_ram_gb, color='#A32D2D', linewidth=0.8,
                         linestyle='--', alpha=0.4)
        ax_usage.set_ylabel('RAM (GB)', fontfamily=FONT, fontsize=8, color='#A32D2D')
        ax_usage.set_ylim(0, node_ram_gb * 1.05)
        ax_usage.tick_params(axis='y', labelsize=7, colors='#A32D2D')

        # CPU: absolute vCPU, ceiling = node's physical vCPU count
        ax_cpu2 = ax_usage.twinx()
        ax_cpu2.step(ts_px, used_cpu, color='#185FA5', linewidth=1.4,
                     where='post',
                     label=f'CPU effective  (peak {max(used_cpu, default=0):.1f} vCPU)')
        ax_cpu2.axhline(node_vcpu, color='#185FA5', linewidth=0.8,
                        linestyle='--', alpha=0.4)
        ax_cpu2.set_ylabel('CPU (vCPU)', fontfamily=FONT, fontsize=8, color='#185FA5')
        ax_cpu2.set_ylim(0, node_vcpu * 1.05)
        ax_cpu2.tick_params(axis='y', labelsize=7, colors='#185FA5')

        from matplotlib.patches import Patch
        from matplotlib.lines import Line2D
        handles = [
            Patch(color='#d0cec8', alpha=0.5,
                  label=f'RAM provisioned  ({node_ram_gb:.0f} GB)'),
            Patch(color='#A32D2D', alpha=0.65, label='RAM used'),
            Line2D([0], [0], color='#185FA5', lw=1.4,
                   label=f'CPU effective  (peak {max(used_cpu, default=0):.1f} vCPU)'),
        ]
        ax_usage.legend(handles=handles, fontsize=7, loc='upper right', framealpha=0.9)
        ax_usage.set_facecolor('#fafaf9')
        ax_usage.grid(True, axis='x', alpha=0.18)

    # ── Bottom panel: stacked CPU waste (BSIM-82) ─────────────────────
    if ax_waste is not None:
        _draw_cpu_waste_panel(ax_waste, node, t_min, t_scale)
        ax_waste.tick_params(axis='x', labelbottom=False)

    # Shared x-axis ticks on the lowest visible panel
    ax_bottom = ax_waste if ax_waste is not None else ax_usage
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

    node_timelines, metadata = run_and_extract(
        event_list_path=args.events,
        scheduler_type=args.scheduler,
        cfg_path=args.scheduler_config,
        registry_path=args.registry,
        seed=args.seed,
        event_log_path=args.event_log,
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
