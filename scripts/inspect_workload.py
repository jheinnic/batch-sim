"""
scripts/inspect_workload.py

Prints a human-readable summary of a generated event list showing:
  - Per-centroid job counts and parameter distributions
  - Instance tier requirements (which node size each job demands)
  - Advantage ratio distribution at each k value
  - The implied Q1/Q2 split for each (q1_instance × k) combination

Usage:
    python scripts/inspect_workload.py
    python scripts/inspect_workload.py --events workloads/reference_4h_v2.json
    python scripts/inspect_workload.py --events workloads/reference_4h_v2.json --k 2 3 4
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_sim.generator.event_list import load_event_list
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.scheduler.queue_router import compute_advantage_ratio, assign_queue


def _pct(n, total):
    return f'{n/total*100:.1f}%' if total else '—'


def _stats(vals):
    if not vals:
        return 'n/a'
    return (f'min={min(vals):.1f}  mean={statistics.mean(vals):.1f}  '
            f'max={max(vals):.1f}  p95={sorted(vals)[int(len(vals)*0.95)]:.1f}')


def main():
    parser = argparse.ArgumentParser(
        description='Inspect a generated event list for workload size constraints.'
    )
    parser.add_argument('--events',
                        default='workloads/reference_4h_v1.json',
                        help='Path to event list JSON')
    parser.add_argument('--registry',
                        default='configs/instance_registry.yaml')
    parser.add_argument('--k', nargs='+', type=float, default=[2, 3, 4],
                        help='Advantage ratio thresholds to show Q1/Q2 splits for')
    parser.add_argument('--q1-instances', nargs='+',
                        default=['r7i.4xlarge', 'r7i.8xlarge', 'r7i.16xlarge'],
                        help='Q1 MachineSet instance types to evaluate routing for')
    args = parser.parse_args()

    el       = load_event_list(args.events)
    registry = InstanceRegistry.from_yaml(args.registry)
    events   = el.events
    total    = len(events)

    print(f'\n{"="*70}')
    print(f'Workload: {args.events}')
    print(f'Total jobs: {total}')
    print(f'{"="*70}')

    # ── Per-centroid distributions ────────────────────────────────────────
    print('\n── Per-centroid parameter distributions ─────────────────────────────')
    by_centroid = defaultdict(list)
    for e in events:
        by_centroid[e.centroid_id].append(e)

    print(f'\n{"Centroid":<14} {"n":>4}  {"peak_ram_gb (GB)":^38}  {"download_gb (GB)":^38}')
    print(f'{"":14} {"":>4}  {"min":>6} {"mean":>8} {"p95":>8} {"max":>8}  '
          f'{"min":>6} {"mean":>8} {"p95":>8} {"max":>8}')
    print('-' * 90)

    for cid in sorted(by_centroid):
        evts   = by_centroid[cid]
        peaks  = [e.preprocess_peak_ram_gb for e in evts]
        # download_gb = download_duration_s * bandwidth_gbs
        # bandwidth defaults to 500 Mbps = 0.5 GB/s
        bw_gbs = el.metadata.get('network_bandwidth_mbps', 500) / 1000.0
        dl_gb  = [e.download_duration_s * bw_gbs for e in evts]
        p95_p  = sorted(peaks)[int(len(peaks) * 0.95)]
        p95_d  = sorted(dl_gb) [int(len(dl_gb)  * 0.95)]
        print(f'{cid:<14} {len(evts):>4}  '
              f'{min(peaks):>6.1f} {statistics.mean(peaks):>8.1f} '
              f'{p95_p:>8.1f} {max(peaks):>8.1f}  '
              f'{min(dl_gb):>6.1f} {statistics.mean(dl_gb):>8.1f} '
              f'{p95_d:>8.1f} {max(dl_gb):>8.1f}')

    # ── Instance tier requirements ────────────────────────────────────────
    print('\n── Instance tier requirements (minimum RAM to host burst) ────────────')
    print('   (cheapest physically-fitting instance for each job\'s peak RAM)\n')
    os_overhead = 2.0   # matches k8s_os_overhead_gb default
    tier_counts = defaultdict(int)
    unschedulable = 0
    for e in events:
        inst = registry.cheapest_fitting(
            min_ram_gb=e.preprocess_peak_ram_gb + os_overhead,
            min_vcpu=1,
        )
        if inst is None:
            unschedulable += 1
            tier_counts['>all'] += 1
        else:
            tier_counts[inst.name] += 1

    all_instances = registry.all_types
    print(f'  {"Instance":22} {"RAM":>6}  {"$/hr":>7}  {"jobs":>6}  {"pct":>7}')
    print(f'  {"-"*55}')
    for inst in all_instances:
        n = tier_counts.get(inst.name, 0)
        if n == 0:
            continue
        print(f'  {inst.name:<22} {inst.ram_gb:>5.0f}G  '
              f'${inst.hourly_price_usd:>6.4f}  {n:>6}  {_pct(n,total):>7}')
    if unschedulable:
        print(f'  {"[unschedulable]":<22} {"—":>6}  {"—":>7}  '
              f'{unschedulable:>6}  {_pct(unschedulable,total):>7}')

    # ── Advantage ratio distribution ──────────────────────────────────────
    print('\n── Advantage ratio distribution (M − M²/C) / S ──────────────────────')
    print('   C = cheapest fitting instance (per job)\n')
    ratios = []
    for e in events:
        a = assign_queue(e.preprocess_peak_ram_gb, e.preprocess_steady_ram_gb,
                         registry, k=1.0)   # k=1 so everything is "computed"
        if a:
            ratios.append(a.advantage_ratio)

    if ratios:
        breakpoints = [0, 1, 2, 4, 8, float('inf')]
        labels      = ['<1 (Batch≈K8S)', '1–2', '2–4', '4–8', '≥8 (K8S dominant)']
        print(f'  {"Range":<22} {"jobs":>6}  {"pct":>7}')
        print(f'  {"-"*38}')
        for lo, hi, lbl in zip(breakpoints, breakpoints[1:], labels):
            n = sum(1 for r in ratios if lo <= r < hi)
            print(f'  {lbl:<22} {n:>6}  {_pct(n,len(ratios)):>7}')
        print(f'\n  Overall: {_stats(ratios)}')

    # ── Q1/Q2 routing split ───────────────────────────────────────────────
    print('\n── Hybrid routing splits (OKD Q1 vs Batch Q2) ───────────────────────')
    print('   Rows = Q1 MachineSet instance type  ·  Columns = k threshold\n')

    instance_map = {i.name: i for i in registry.all_types}
    header = f'  {"Q1 MachineSet":<22} {"RAM":>6}  '
    for k in args.k:
        header += f'  {"k="+str(k):^20}'
    print(header)
    sub = f'  {"":22} {"":>6}  '
    for _ in args.k:
        sub += f'  {"OKD%":>7} {"Batch%":>7} {"unsched%":>7}'
    print(sub)
    print(f'  {"-"*80}')

    for inst_name in args.q1_instances:
        q1_inst = instance_map.get(inst_name)
        if not q1_inst:
            print(f'  {inst_name}: not in registry')
            continue
        C_net = q1_inst.ram_gb - os_overhead
        row = f'  {inst_name:<22} {q1_inst.ram_gb:>5.0f}G  '
        for k in args.k:
            q1_n = q2_n = unsched_n = 0
            for e in events:
                M = e.preprocess_peak_ram_gb
                S = e.preprocess_steady_ram_gb
                if M > C_net:
                    unsched_n += 1
                elif compute_advantage_ratio(M, S, C_net) >= k:
                    q1_n += 1
                else:
                    q2_n += 1
            row += (f'  {_pct(q1_n,total):>7} {_pct(q2_n,total):>7} '
                    f'{_pct(unsched_n,total):>7} ')
        print(row)

    print(f'\n{"="*70}\n')


    # Per-stage effective thread counts
    print('\n── Per-stage effective thread counts (parallel stages only) ─────────')
    print('   declared → effective shows I/O wait reduction per stage\n')
    for cid in sorted(by_centroid):
        evts = by_centroid[cid][:3]
        print(f'  {cid}:')
        for e_idx, ev in enumerate(evts):
            stages   = ev.workhorse_stages
            parallel = [(s['index'], s['declared_threads'], s['effective_threads'])
                        for s in stages if s['index'] % 2 == 0]
            parts = [f"stage{idx}: {decl}→{eff:.1f}" for idx,decl,eff in parallel]
            print(f'    job {e_idx+1}: {", ".join(parts)}')
        if len(by_centroid[cid]) > 3:
            print(f'    ... ({len(by_centroid[cid])-3} more jobs)')

if __name__ == '__main__':
    main()
