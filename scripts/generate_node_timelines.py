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
from batch_sim.scheduler.batch_scheduler import BatchScheduler
from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler

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
) -> tuple[dict, dict]:
    """
    Re-run the simulation and return:
      (node_timelines, metadata)

    node_timelines: dict[node_id] -> {
        instance, ram_gb, hourly_usd, launch_t, ready_t, term_t, cost,
        jobs: [ {job_id, centroid, start_t, end_t, status,
                 phases: [(phase, start, end)]} ]
    }
    """
    el       = load_event_list(event_list_path)
    cfg      = load_scheduler_config(cfg_path)
    registry = InstanceRegistry.from_yaml(registry_path)
    metrics  = MetricsCollector()
    peaks    = list({e.preprocess_peak_ram_gb for e in el.events})
    rng      = random.Random(seed)

    if scheduler_type == 'batch':
        sched = BatchScheduler(cfg=cfg, registry=registry, metrics=metrics, rng=rng)
    else:
        sched = K8SPlusScheduler(cfg=cfg, registry=registry, metrics=metrics,
                                  centroid_peak_rams=peaks, rng=rng)

    engine = SimulationEngine(scheduler=sched, metrics=metrics, cfg=cfg)
    engine.run(el)
    sched.finalize(engine.env)

    accruers = {a.node_id: a for a in sched.accruers if a.is_terminated}

    # Node lifecycle events
    node_ready = {}
    node_term  = {}
    for e in metrics.log:
        nid = e.data.get('node_id')
        if not nid: continue
        if e.event_type == EventType.NODE_READY:
            node_ready[nid] = e.sim_time
        if e.event_type == EventType.NODE_TERMINATED:
            node_term[nid] = e.sim_time

    # Per-job phase windows
    phase_starts = {}
    phase_windows = collections.defaultdict(list)
    job_centroids = {}
    job_start_t   = {}
    job_status    = {}

    for e in metrics.log:
        nid = e.data.get('node_id'); jid = e.data.get('job_id')
        if not nid or not jid: continue
        t = e.sim_time

        if e.event_type == EventType.JOB_START:
            job_start_t[(nid, jid)] = t
            job_centroids[jid] = e.data.get('centroid_id', '?')

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

    # Assemble per-node structure
    node_timelines = {}
    for nid, acc in accruers.items():
        jobs_here = []
        for (n2, jid), windows in phase_windows.items():
            if n2 != nid: continue
            jobs_here.append({
                'job_id':   jid,
                'centroid': job_centroids.get(jid, '?'),
                'start_t':  job_start_t.get((nid, jid), windows[0][1] if windows else 0),
                'end_t':    windows[-1][2] if windows else 0,
                'status':   job_status.get((nid, jid), 'complete'),
                'phases':   [(ph, t0, t1) for ph, t0, t1 in windows],
            })
        jobs_here.sort(key=lambda j: j['start_t'])

        node_timelines[nid] = {
            'instance':   acc.instance.name,
            'ram_gb':     acc.instance.ram_gb,
            'hourly_usd': acc.instance.hourly_price_usd,
            'launch_t':   acc.launch_time,
            'ready_t':    node_ready.get(nid, acc.launch_time),
            'term_t':     acc.termination_time,
            'cost':       acc.total_cost_usd,
            'jobs':       jobs_here,
        }

    metadata = {
        'scheduler':  scheduler_type,
        'event_list': event_list_path,
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
) -> None:
    """Draw one node's lifecycle as a horizontal Gantt row."""
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

    # Job phases
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


# ---------------------------------------------------------------------------
# Overview chart — all nodes
# ---------------------------------------------------------------------------

def chart_overview(node_timelines: dict, metadata: dict, out: Path) -> None:
    nodes = sorted(node_timelines.values(), key=lambda n: n['launch_t'])
    if not nodes:
        return

    t_min = min(n['launch_t'] for n in nodes)
    t_max = max(n['term_t']   for n in nodes)
    t_span = t_max - t_min
    if t_span == 0:
        return

    n_nodes  = len(nodes)
    row_h    = max(0.4, min(1.0, 40 / n_nodes))
    fig_h    = max(4, n_nodes * row_h * 1.3 + 2)
    fig_w    = 14

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    t_scale  = (fig_w - 2) / t_span   # pixels per second (relative)

    for i, node in enumerate(nodes):
        y = i
        _draw_node_row(ax, node, y, row_h, t_min, t_scale, fig_w)
        label = f"{node['instance']} ${node['cost']:.2f}  ({len(node['jobs'])}j)"
        ax.text(-0.01, y, label, transform=ax.get_yaxis_transform(),
                ha='right', va='center', fontsize=7, fontfamily=FONT)

    # X axis in minutes
    ax.set_xlim(0, (t_max - t_min) * t_scale)
    tick_s = _nice_tick_seconds(t_span)
    ticks  = np.arange(0, t_span + tick_s, tick_s)
    ax.set_xticks(ticks * t_scale)
    ax.set_xticklabels([f'{t/60:.0f}m' for t in ticks], fontsize=8, fontfamily=FONT)
    ax.set_yticks([])
    ax.set_xlabel('simulated time', fontfamily=FONT, fontsize=10)

    sched_label = 'AWS Batch' if metadata['scheduler'] == 'batch' else 'OKD K8S+'
    ax.set_title(
        f'{sched_label} — All Node Lifecycles\n'
        f'{metadata["total_nodes"]} nodes · {metadata["total_jobs"]} jobs · '
        f'total cost ${metadata["total_cost"]:.2f}',
        fontfamily=FONT, fontsize=11,
    )

    ax.legend(handles=_legend_handles(), fontsize=8,
              loc='upper right', ncol=3, framealpha=0.9)
    ax.grid(True, axis='x', alpha=0.18, zorder=1)
    ax.set_facecolor('#fafaf9')

    plt.tight_layout()
    for ext in ('png', 'svg'):
        fig.savefig(out / f'overview.{ext}', dpi=130, bbox_inches='tight')
    plt.close(fig)
    print(f'  overview.{{png,svg}}  ({n_nodes} nodes)')


# ---------------------------------------------------------------------------
# Per-node detail chart
# ---------------------------------------------------------------------------

def chart_per_node(node_id: str, node: dict, out: Path) -> None:
    jobs = node['jobs']
    if not jobs:
        return

    t_min  = node['launch_t']
    t_max  = node['term_t']
    t_span = t_max - t_min
    if t_span == 0:
        return

    n_rows  = len(jobs) + 1   # +1 for node lifecycle row
    row_h   = 0.7
    fig_h   = max(3, n_rows * row_h * 1.8 + 1.5)
    fig_w   = 12
    t_scale = (fig_w - 3) / t_span

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    # Node lifecycle row at top
    _draw_node_row(ax, node, n_rows - 1, row_h, t_min, t_scale, fig_w)
    ax.text(-0.01, n_rows - 1, 'node', transform=ax.get_yaxis_transform(),
            ha='right', va='center', fontsize=8, fontfamily=FONT, fontweight='bold')

    # Individual job rows
    for i, job in enumerate(jobs):
        y = i
        hatch = CENTROID_HATCH.get(job['centroid'], '')
        for ph, t0, t1 in job['phases']:
            if t1 <= t0: continue
            ax.barh(y, (t1 - t0) * t_scale, row_h * 0.75,
                    left=(t0 - t_min) * t_scale,
                    color=PHASE_COLOR.get(ph, '#aaa'),
                    hatch=hatch, edgecolor='white', linewidth=0.4, zorder=3)
            if (t1 - t0) * t_scale > 20:
                ax.text((t0 - t_min + (t1 - t0) / 2) * t_scale, y,
                        ph[:4], ha='center', va='center',
                        fontsize=7, color='white', fontfamily=FONT, zorder=4)
        if job['status'] == 'crash':
            ax.plot((job['end_t'] - t_min) * t_scale, y,
                    'x', color='#e74c3c', markersize=7, mew=2, zorder=5)
        label = f"{job['centroid'].split('_')[-1].upper()}  {job['job_id'][:8]}"
        ax.text(-0.01, y, label, transform=ax.get_yaxis_transform(),
                ha='right', va='center', fontsize=7.5, fontfamily=FONT)

    # Axes
    ax.set_xlim(0, t_span * t_scale)
    ax.set_ylim(-0.6, n_rows - 0.4)
    tick_s = _nice_tick_seconds(t_span)
    ticks  = np.arange(0, t_span + tick_s, tick_s)
    ax.set_xticks(ticks * t_scale)
    ax.set_xticklabels([f'{t:.0f}s' if t_span < 600 else f'{t/60:.1f}m'
                        for t in ticks], fontsize=8, fontfamily=FONT)
    ax.set_yticks([])
    ax.set_xlabel('time since node launch', fontfamily=FONT, fontsize=9)
    ax.set_title(
        f'Node {node_id}  —  {node["instance"]}  '
        f'({node["ram_gb"]:.0f} GB  ${node["hourly_usd"]:.4f}/hr)\n'
        f'lifespan {t_span:.0f}s  ·  {len(jobs)} jobs  ·  cost ${node["cost"]:.4f}',
        fontfamily=FONT, fontsize=10,
    )
    ax.legend(handles=_legend_handles(), fontsize=8, loc='upper right',
              ncol=2, framealpha=0.9)
    ax.grid(True, axis='x', alpha=0.18, zorder=1)
    ax.set_facecolor('#fafaf9')

    plt.tight_layout()
    fname = f'node_{node_id}'
    for ext in ('png', 'svg'):
        fig.savefig(out / f'{fname}.{ext}', dpi=130, bbox_inches='tight')
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
    parser.add_argument('--scheduler', choices=['batch', 'k8s'], required=True)
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
    args = parser.parse_args()

    out = Path(args.output or f'results/node_timelines/{args.scheduler}')
    out.mkdir(parents=True, exist_ok=True)

    print(f'Re-running {args.scheduler} simulation to extract node timelines...')
    node_timelines, metadata = run_and_extract(
        event_list_path=args.events,
        scheduler_type=args.scheduler,
        cfg_path=args.scheduler_config,
        registry_path=args.registry,
        seed=args.seed,
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
    chart_overview(node_timelines, metadata, out)

    # Per-node charts
    if not args.overview_only:
        nodes_sorted = sorted(
            node_timelines.items(),
            key=lambda x: x[1]['launch_t'],
        )
        limit = args.max_per_node or len(nodes_sorted)
        for i, (nid, node) in enumerate(nodes_sorted[:limit]):
            chart_per_node(nid, node, out)
            if (i + 1) % 10 == 0 or (i + 1) == min(limit, len(nodes_sorted)):
                print(f'  per-node charts: {i+1}/{min(limit, len(nodes_sorted))}')

    print(f'Done.  {len(list(out.glob("*.png")))} PNG files in {out}')


if __name__ == '__main__':
    main()
