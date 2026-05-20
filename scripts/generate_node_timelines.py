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
import random
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

# Make sure we can import batch_sim regardless of CWD
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_sim.core.config_loader import load_simulation_config, load_scheduler_config
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.generator.event_list import load_event_list
from batch_sim.core.schemas import SchedulerType
from batch_sim.metrics.collector import MetricsCollector, EventType
from batch_sim.core.engine import SimulationEngine
# from batch_sim.scheduler.batch_scheduler import BatchScheduler
# from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler

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
    This guarantees charts reflect exactly the simulation whose scorecard
    you are looking at — the same run, the same engine, the same config.

    If event_log_path is absent, falls back to re-running the simulation
    via run_one() (the same code path as the main simulate command) rather
    than constructing a scheduler directly. This ensures at minimum that
    both paths use the same scheduler classes and configuration handling.
    """
    import json as _json
    from batch_sim.metrics.collector import SimEvent

    el           = load_event_list(event_list_path)
    job_profiles = {e.job_id: e for e in el.events}

    if event_log_path:
        # ── Read saved event log ───────────────────────────────────────
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
        accruers_raw = {}   # will be rebuilt from NODE events below
        print(f"  Reading {len(log)} events from saved log")
    else:
        # ── Re-run via run_one() (same code path as simulate command) ──
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

    # ── Build node cost/instance info from NODE events ─────────────────────
    # We reconstruct accruers from the event log directly
    node_instances   = {}   # node_id → instance_name
    node_launch_time = {}
    node_ready_time  = {}
    node_term_time   = {}
    node_idle_dur    = {}

    for e in log:
        nid = e.data.get('node_id')
        if not nid: continue
        t   = e.sim_time
        if e.event_type == EventType.NODE_LAUNCHING:
            node_instances[nid]   = e.data.get('instance_name', '?')
            node_launch_time[nid] = t
        if e.event_type == EventType.NODE_READY:
            node_ready_time[nid] = t
        if e.event_type == EventType.NODE_TERMINATED:
            node_term_time[nid]  = t
            node_idle_dur[nid]   = e.data.get('idle_duration_s', 0)

    # Rebuild cost from launch/term times and instance registry
    inst_map = {i.name: i for i in registry.all_types}

    # ── Per-job phase windows ──────────────────────────────────────────────
    phase_starts  = {}
    phase_windows = collections.defaultdict(list)
    job_centroids = {}
    job_start_t   = {}
    job_status    = {}

    for e in log:
        nid = e.data.get('node_id'); jid = e.data.get('job_id')
        if not nid or not jid: continue
        t = e.sim_time

        if e.event_type == EventType.JOB_START:
            job_start_t[(nid, jid)] = t
            job_centroids[jid]      = e.data.get('centroid_id', '?')

        if e.event_type == EventType.PHASE_TRANSITION:
            key = (nid, jid); ph = e.data['phase']
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
    all_node_ids = set(node_launch_time.keys()) & set(node_term_time.keys())
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
            if n2 != nid: continue
            prof = job_profiles.get(jid)
            if scheduler_type == 'batch':
                res_gb = prof.preprocess_peak_ram_gb if prof else None
            else:
                res_gb = prof.preprocess_steady_ram_gb if prof else None

            ep = job_profiles.get(jid)
            j_soft = ep.workhorse_declared_vcpu if ep else None
            j_hard = j_soft

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
            })
        jobs_here.sort(key=lambda j: j['start_t'])

        node_soft = max((j.get('soft_cpu') or 0 for j in jobs_here), default=0)
        node_hard = max((j.get('hard_cpu') or 0 for j in jobs_here), default=0)

        node_timelines[nid] = {
            'instance':        inst_name,
            'ram_gb':          inst.ram_gb,
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
    """Draw one node's lifecycle as a horizontal Gantt row.

    Adds a small vertical tick at the right edge of each job bar showing
    the reservation limit (peak RAM for Batch, soft limit for K8S).
    The tick label appears to the right of the bar when space allows.
    """
    launch = node['launch_t']
    ready  = node['ready_t']
    term   = node['term_t']

    def px(t):
        return (t - t_origin) * t_scale

    # Warmup band
    ax.barh(y, px(ready) - px(launch), row_h * 0.7,
            left=px(launch), color=PHASE_COLOR['warmup'], zorder=2)

    # Idle band (ready → first job or term)
    first_job_t = node['jobs'][0]['start_t'] if node['jobs'] else term
    last_job_t  = node['jobs'][-1]['end_t']  if node['jobs'] else ready
    if first_job_t > ready:
        ax.barh(y, px(first_job_t) - px(ready), row_h * 0.5,
                left=px(ready), color=PHASE_COLOR['idle'], zorder=2)
    if last_job_t < term:
        ax.barh(y, px(term) - px(last_job_t), row_h * 0.5,
                left=px(last_job_t), color=PHASE_COLOR['idle'], zorder=2)

    # Job phases + reservation tick
    for job in node['jobs']:
        hatch = CENTROID_HATCH.get(job['centroid'], '')
        for ph, t0, t1 in job['phases']:
            if t1 <= t0: continue
            color = PHASE_COLOR.get(ph, '#aaa')
            ax.barh(y, px(t1) - px(t0), row_h * 0.75,
                    left=px(t0), color=color, hatch=hatch,
                    edgecolor='white', linewidth=0.3, zorder=3)
        if job['status'] == 'crash':
            cx = px(job['end_t'])
            ax.plot(cx, y, 'x', color='#e74c3c', markersize=6, zorder=5, mew=1.5)

        # Reservation tick: vertical line at right edge of job bar
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
# Pool-level CPU / RAM usage time series — sampled from event log
# ---------------------------------------------------------------------------

def _build_usage_series(node_timelines: dict, sample_s: float = 60.0) -> list[dict]:
    """
    Build a time series of pool-wide RAM and CPU usage at sample_s intervals.
    Returns list of {t, alloc_ram, used_ram, alloc_vcpu, used_vcpu, n_nodes}.
    """
    if not node_timelines:
        return []

    t_min = min(n['launch_t'] for n in node_timelines.values())
    t_max = max(n['term_t']   for n in node_timelines.values())

    # Build per-job phase lookup: (nid, jid) -> [(phase, t0, t1, ram, vcpu)]
    phase_resource = {}
    for nid, node in node_timelines.items():
        for job in node['jobs']:
            segs = []
            for ph, t0, t1 in job['phases']:
                if ph == 'download':
                    ram, vcpu = 0.5, 1.0
                elif ph == 'preprocess':
                    # peak RAM — read from job if available
                    ram  = job.get('reserved_ram_gb', 4.0)
                    vcpu = 1.0
                elif ph == 'workhorse':
                    ram  = job.get('reserved_ram_gb', 1.0) * 0.08
                    vcpu = 4.0   # approximate; actual varies per stage
                elif ph == 'upload':
                    ram, vcpu = 0.5, 1.0
                else:
                    ram, vcpu = 0.0, 0.0
                segs.append((ph, t0, t1, ram, vcpu))
            phase_resource[(nid, job['job_id'])] = segs

    series = []
    t = t_min
    while t <= t_max + sample_s:
        active_nodes = [
            (nid, n) for nid, n in node_timelines.items()
            if n['launch_t'] <= t < n['term_t']
        ]
        alloc_ram  = sum(n['ram_gb'] for _, n in active_nodes)
        alloc_vcpu = 0.0   # not tracked per-node yet; use job sum
        used_ram   = 0.0
        used_vcpu  = 0.0

        for (nid, jid), segs in phase_resource.items():
            if nid not in dict(active_nodes):
                continue
            for ph, t0, t1, ram, vcpu in segs:
                if t0 <= t < t1:
                    used_ram  += ram
                    used_vcpu += vcpu

        series.append({
            't':          round(t - t_min),
            'alloc_ram':  round(alloc_ram, 1),
            'used_ram':   round(used_ram, 1),
            'used_vcpu':  round(used_vcpu, 1),
            'n_nodes':    len(active_nodes),
        })
        t += sample_s
    return series


# ---------------------------------------------------------------------------
# Overview chart — Gantt (left) + CPU/RAM usage bars (right)
# ---------------------------------------------------------------------------

def chart_overview(
    node_timelines: dict,
    metadata: dict,
    out: Path,
    scheduler_type: str = 'batch',
) -> None:
    """
    Overview chart: all nodes.
    Layout: Gantt (top panel, time left→right) stacked above
            CPU/RAM usage (bottom panel, same time axis).
    Left labels show: instance type, cost, job count, soft/hard CPU limits.
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
    gs  = fig.add_gridspec(2, 1, height_ratios=[gantt_h, usage_h],
                            hspace=0.08)
    ax_gantt = fig.add_subplot(gs[0])
    ax_usage = fig.add_subplot(gs[1], sharex=ax_gantt)

    # ── Shared time scale ─────────────────────────────────────────────
    t_scale = (fig_w - 2) / t_span   # px per second (relative to fig width)

    # ── Top panel: Gantt ──────────────────────────────────────────────
    for i, node in enumerate(nodes):
        y = n_nodes - 1 - i   # top node = highest y
        _draw_node_row(ax_gantt, node, y, row_h, t_min, t_scale,
                       fig_w, scheduler_type=scheduler_type)

        # Left label: instance  $cost  (Nj)  soft/hard CPU  RAM limit
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
    ax_gantt.tick_params(axis='x', labelbottom=False)   # shared axis labels below
    ax_gantt.set_facecolor('#fafaf9')
    ax_gantt.grid(True, axis='x', alpha=0.15, zorder=1)
    ax_gantt.legend(handles=_legend_handles(), fontsize=7,
                    loc='upper right', ncol=3, framealpha=0.9)

    sched_label = 'AWS Batch' if scheduler_type == 'batch' else 'OKD K8S' if scheduler_type == 'k8s' else 'OKD K8S+'
    ax_gantt.set_title(
        f'{sched_label} — Node Lifecycles  '
        f'{n_nodes} nodes · {metadata["total_jobs"]} jobs · '
        f'${metadata["total_cost"]:.2f}  '
        f'(left labels: instance  cost  jobs  cpu:soft/hard  ram:limit)',
        fontfamily=FONT, fontsize=9, loc='left',
    )

    # ── Bottom panel: CPU and RAM over time (time = x axis) ───────────
    series = _build_usage_series(node_timelines, sample_s=max(60, t_span / 80))
    if series:
        ts_px    = [s['t'] * t_scale for s in series]
        used_ram = [s['used_ram']    for s in series]
        alloc_ram= [s['alloc_ram']   for s in series]
        used_cpu = [s['used_vcpu']   for s in series]
        max_alloc= max(alloc_ram) if alloc_ram else 1
        max_cpu  = max(used_cpu)  if used_cpu  else 1

        # RAM: filled area (allocated = light, used = red)
        ax_usage.fill_between(ts_px,
                              [v / max_alloc * 100 for v in alloc_ram],
                              alpha=0.25, color='#d0cec8',
                              step='post', label=f'RAM allocated (peak {max_alloc:.0f} GB)')
        ax_usage.fill_between(ts_px,
                              [v / max_alloc * 100 for v in used_ram],
                              alpha=0.65, color='#A32D2D',
                              step='post', label='RAM used')
        ax_usage.set_ylabel('RAM / alloc %', fontfamily=FONT,
                            fontsize=8, color='#A32D2D')
        ax_usage.set_ylim(0, 110)
        ax_usage.tick_params(axis='y', labelsize=7, colors='#A32D2D')

        # CPU: line on twin y-axis
        ax_cpu2 = ax_usage.twinx()
        ax_cpu2.step(ts_px, [v / max_cpu * 100 for v in used_cpu],
                     color='#185FA5', linewidth=1.4, where='post',
                     label=f'CPU used (peak {max_cpu:.0f} vCPU)')
        ax_cpu2.set_ylabel('CPU / peak %', fontfamily=FONT,
                           fontsize=8, color='#185FA5')
        ax_cpu2.set_ylim(0, 110)
        ax_cpu2.tick_params(axis='y', labelsize=7, colors='#185FA5')

        # Combined legend
        from matplotlib.patches import Patch
        from matplotlib.lines import Line2D
        handles = [
            Patch(color='#d0cec8', alpha=0.5,
                  label=f'RAM allocated  (peak {max_alloc:.0f} GB)'),
            Patch(color='#A32D2D', alpha=0.65, label='RAM used'),
            Line2D([0],[0], color='#185FA5', lw=1.4,
                   label=f'CPU used  (peak {max_cpu:.0f} vCPU)'),
        ]
        ax_usage.legend(handles=handles, fontsize=7, loc='upper right',
                        framealpha=0.9)
        ax_usage.set_facecolor('#fafaf9')
        ax_usage.grid(True, axis='x', alpha=0.15)

    # Shared x-axis ticks (on bottom panel)
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
# Per-node detail chart — Gantt (top) + CPU/RAM usage (bottom), shared time axis
# ---------------------------------------------------------------------------

def chart_per_node(
    node_id: str,
    node: dict,
    out: Path,
    scheduler_type: str = 'batch',
) -> None:
    """
    Per-node detail chart.
    Layout: Gantt (top, one row per job + node row) stacked above
            CPU/RAM usage (bottom), both with time on the horizontal axis.
    Left labels show: centroid, job_id prefix, soft/hard CPU, RAM reservation.
    """
    jobs = node['jobs']
    if not jobs:
        return

    t_min  = node['launch_t']
    t_max  = node['term_t']
    t_span = t_max - t_min
    if t_span == 0:
        return

    n_rows  = len(jobs) + 1
    row_h   = 0.7
    gantt_h = max(2.5, n_rows * row_h * 1.8 + 1.0)
    usage_h = 2.2
    fig_w   = 15

    fig = plt.figure(figsize=(fig_w, gantt_h + usage_h + 0.6))
    gs  = fig.add_gridspec(2, 1, height_ratios=[gantt_h, usage_h],
                            hspace=0.06)
    ax_gantt = fig.add_subplot(gs[0])
    ax_usage = fig.add_subplot(gs[1], sharex=ax_gantt)

    t_scale = (fig_w - 2.5) / t_span

    # ── Node lifecycle row (top row in gantt) ─────────────────────────
    _draw_node_row(ax_gantt, node, n_rows - 1, row_h,
                   t_min, t_scale, fig_w, scheduler_type=scheduler_type)
    soft = node.get('soft_cpu', '?')
    hard = node.get('hard_cpu', '?')
    ax_gantt.text(-0.005, n_rows - 1,
                  f"node  {node['instance']}  cpu:{soft}/{hard}  {node['ram_gb']:.0f}GB",
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

        # Reservation tick: vertical line at right edge with RAM label
        res_gb = job.get('reserved_ram_gb')
        if res_gb is not None:
            tick_x = (job['end_t'] - t_min) * t_scale
            ax_gantt.plot([tick_x, tick_x],
                          [y - row_h * 0.45, y + row_h * 0.45],
                          color='#222', linewidth=1.4, zorder=6)
            ax_gantt.text(tick_x + 0.06, y,
                          f'{res_gb:.0f}G',
                          fontsize=6.5, va='center', color='#222',
                          fontfamily=FONT, zorder=7)

        if job['status'] == 'crash':
            ax_gantt.plot(
                (job['end_t'] - t_min) * t_scale, y,
                'x', color='#e74c3c', markersize=7, mew=2, zorder=5,
            )

        # Left label: centroid  job_id  soft/hard CPU  RAM reservation
        res_str = f'  ram:{res_gb:.0f}G' if res_gb is not None else ''
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
        f'  ({node["ram_gb"]:.0f} GB  ${node["hourly_usd"]:.4f}/hr)'
        f'  lifespan {t_span:.0f}s  ·  {len(jobs)} jobs  ·  ${node["cost"]:.4f}'
        f'  (left labels: centroid  job_id  cpu:soft/hard  ram:limit)',
        fontfamily=FONT, fontsize=9,
    )
    ax_gantt.legend(handles=_legend_handles(), fontsize=8,
                    loc='upper right', ncol=2, framealpha=0.9)
    ax_gantt.grid(True, axis='x', alpha=0.18, zorder=1)
    ax_gantt.set_facecolor('#fafaf9')

    # ── Bottom panel: CPU/RAM over time ──────────────────────────────
    node_tl = {node_id: node}
    series  = _build_usage_series(node_tl, sample_s=max(10, t_span / 60))

    if series:
        ts_px    = [s['t'] * t_scale for s in series]
        used_ram = [s['used_ram']    for s in series]
        alloc_ram= [s['alloc_ram']   for s in series]
        used_cpu = [s['used_vcpu']   for s in series]
        node_ram = node['ram_gb']
        max_cpu  = max(used_cpu) if any(used_cpu) else 1

        ax_usage.fill_between(ts_px,
                              [v / node_ram * 100 for v in alloc_ram],
                              alpha=0.25, color='#d0cec8',
                              step='post', label=f'RAM allocated ({node_ram:.0f} GB)')
        ax_usage.fill_between(ts_px,
                              [v / node_ram * 100 for v in used_ram],
                              alpha=0.65, color='#A32D2D',
                              step='post', label='RAM used')
        ax_usage.set_ylabel('RAM / node %', fontfamily=FONT,
                            fontsize=8, color='#A32D2D')
        ax_usage.set_ylim(0, 110)
        ax_usage.tick_params(axis='y', labelsize=7, colors='#A32D2D')

        ax_cpu2 = ax_usage.twinx()
        ax_cpu2.step(ts_px, [v / max_cpu * 100 for v in used_cpu],
                     color='#185FA5', linewidth=1.4, where='post',
                     label=f'CPU used (peak {max_cpu:.1f} vCPU)')
        ax_cpu2.set_ylabel('CPU / peak %', fontfamily=FONT,
                           fontsize=8, color='#185FA5')
        ax_cpu2.set_ylim(0, 110)
        ax_cpu2.tick_params(axis='y', labelsize=7, colors='#185FA5')

        from matplotlib.patches import Patch
        from matplotlib.lines import Line2D
        handles = [
            Patch(color='#d0cec8', alpha=0.5,
                  label=f'RAM allocated  ({node_ram:.0f} GB)'),
            Patch(color='#A32D2D', alpha=0.65, label='RAM used'),
            Line2D([0],[0], color='#185FA5', lw=1.4,
                   label=f'CPU used  (peak {max_cpu:.1f} vCPU)'),
        ]
        ax_usage.legend(handles=handles, fontsize=7, loc='upper right',
                        framealpha=0.9)
        ax_usage.set_facecolor('#fafaf9')
        ax_usage.grid(True, axis='x', alpha=0.18)

    tick_s = _nice_tick_seconds(t_span)
    ticks  = np.arange(0, t_span + tick_s, tick_s)
    ax_usage.set_xticks(ticks * t_scale)
    ax_usage.set_xticklabels(
        [f'{t:.0f}s' if t_span < 600 else f'{t/60:.1f}m' for t in ticks],
        fontsize=8, fontfamily=FONT,
    )
    ax_usage.set_xlabel('time since node launch', fontfamily=FONT, fontsize=9)

    for ext in ('png', 'svg'):
        fig.savefig(out / f'node_{node_id}.{ext}', dpi=130, bbox_inches='tight')
    plt.close(fig)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _nice_tick_seconds(span: float) -> float:
    """Choose a human-readable tick interval for a time span in seconds."""
    targets = [30, 60, 120, 300, 600, 900, 1800, 3600]
    for t in targets:
        if span / t <= 10:
            return t
    return 3600

def _legend_handles():
    handles = [
        mpatches.Patch(color=PHASE_COLOR['warmup'],     label='warmup'),
        mpatches.Patch(color=PHASE_COLOR['idle'],       label='idle'),
        mpatches.Patch(color=PHASE_COLOR['download'],   label='download'),
        mpatches.Patch(color=PHASE_COLOR['preprocess'], label='preprocess (RAM peak)'),
        mpatches.Patch(color=PHASE_COLOR['workhorse'],  label='workhorse (CPU)'),
        mpatches.Patch(color=PHASE_COLOR['upload'],     label='upload'),
    ]
    return handles


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
            'the simulation. Guarantees charts match the scorecard exactly. '
            'When absent, re-runs the simulation via run_one() (same code path '
            'as the simulate command, but a new independent run).'
        )
    )
    args = parser.parse_args()

    out = Path(args.output or f'results/node_timelines/{args.scheduler}')
    out.mkdir(parents=True, exist_ok=True)

    if args.event_log:
        print(f'Reading saved event log: {args.event_log}')
    else:
        print(f'Re-running {args.scheduler} via run_one() '
              f'(same path as simulate command)...')
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

    # Save structured data
    with open(out / 'summary.json', 'w') as f:
        # Serialise without matplotlib objects
        serial = {
            nid: {k: v for k, v in nd.items()}
            for nid, nd in node_timelines.items()
        }
        json.dump({'metadata': metadata, 'nodes': serial}, f, indent=2)
    print(f'  summary.json')

    # Overview chart
    print(f'Writing charts to {out}')
    chart_overview(node_timelines, metadata, out,
                   scheduler_type=args.scheduler)

    # Per-node charts
    if not args.overview_only:
        nodes_sorted = sorted(
            node_timelines.items(),
            key=lambda x: x[1]['launch_t'],
        )
        limit = args.max_per_node or len(nodes_sorted)
        for i, (nid, node) in enumerate(nodes_sorted[:limit]):
            chart_per_node(nid, node, out,
                       scheduler_type=args.scheduler)
            if (i + 1) % 10 == 0 or (i + 1) == min(limit, len(nodes_sorted)):
                print(f'  per-node charts: {i+1}/{min(limit, len(nodes_sorted))}')

    print(f'Done.  {len(list(out.glob("*.png")))} PNG files in {out}')


if __name__ == '__main__':
    main()
