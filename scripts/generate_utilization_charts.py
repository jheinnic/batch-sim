"""
generate_utilization_charts.py

Reads results/utilization_charts/util_metrics.json (produced by RUN-05
in reproduce_all.sh) and writes the three utilization charts as PNG+SVG.

Charts produced:
  chart_A_reserved_utilized_time.{png,svg}
      Panel A: R/A over time (both schedulers)
      Panel B: U1/A and U3/A over time with burst-gap shading

  chart_B_five_ratios_summary.{png,svg}
      Bar chart: R/A, U1/A, U1/R, U2/R, U3/R for both schedulers

  chart_C_decomposition_diagnostic.{png,svg}
      Primary signal (U1-U3)/A and secondary signal (U2-U1)/A over time,
      one panel per scheduler, with automated diagnosis annotation.

Usage:
  python scripts/generate_utilization_charts.py
  python scripts/generate_utilization_charts.py --input  results/utilization_charts/util_metrics.json
                                                 --output results/utilization_charts
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

BC   = '#2C7BB6'   # Batch blue
KC   = '#E08E27'   # K8S orange
FONT = 'monospace'


def _mean_ratio(series: list[dict], num: str, den: str) -> float:
    vals = [s[num] / s[den] * 100 for s in series if s.get(den, 0) > 0]
    return round(statistics.mean(vals), 1) if vals else 0.0


# ---------------------------------------------------------------------------
# Chart A — R/A and U1/A, U3/A time series
# ---------------------------------------------------------------------------

def chart_a(d: dict, out: Path) -> None:
    fig = plt.figure(figsize=(12, 8))
    gs  = gridspec.GridSpec(2, 1, hspace=0.40)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])
    fig.suptitle(
        'RAM Capacity Ratios Over Simulation Time\n'
        'Allocated = net physical RAM (OS overhead pre-subtracted)',
        fontfamily=FONT, fontsize=11, y=0.99,
    )

    # Panel A — R/A
    for sched, color in [('batch', BC), ('k8s', KC)]:
        label = 'AWS Batch' if sched == 'batch' else 'OKD K8S+'
        s   = d[sched]['series']
        ts  = [x['t'] / 3600 for x in s]
        rva = [x['reserved'] / x['alloc'] * 100 if x['alloc'] > 0 else 0 for x in s]
        mean_ra = _mean_ratio(s, 'reserved', 'alloc')
        ax1.plot(ts, rva, color=color, lw=1.5, label=f'{label}  (mean {mean_ra}%)')

    ax1.axhline(100, color='#aaa', lw=0.8, ls='--', alpha=0.7)
    ax1.set_ylabel('Reserved / Allocated  (%)', fontfamily=FONT, fontsize=10)
    ax1.set_ylim(0, 115)
    ax1.set_title(
        'Panel A — What fraction of net capacity did we commit (reserve)?',
        fontfamily=FONT, fontsize=10, loc='left', pad=3,
    )
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.22)
    ax1.text(
        0.01, 0.06,
        'Batch: reserves peak RAM per job (hard limit)\n'
        'K8S+: reserves soft limit per job + fixed spike headroom per node',
        transform=ax1.transAxes, fontsize=8, color='#555', fontfamily=FONT,
    )

    # Panel B — U1/A and U3/A with shaded burst gap
    for sched, color, ls in [('batch', BC, '-'), ('k8s', KC, '--')]:
        label = 'AWS Batch' if sched == 'batch' else 'OKD K8S+'
        s    = d[sched]['series']
        ts   = [x['t'] / 3600 for x in s]
        u1a  = [x['util1'] / x['alloc'] * 100 if x['alloc'] > 0 else 0 for x in s]
        u3a  = [x['util3'] / x['alloc'] * 100 if x['alloc'] > 0 else 0 for x in s]
        mean_u1a = _mean_ratio(s, 'util1', 'alloc')
        mean_u3r = _mean_ratio(s, 'util3', 'reserved')
        gap_mean = d[sched].get('burst_gap', 0)
        ax2.plot(ts, u1a, color=color, lw=1.5, ls=ls,
                 label=f'U1 instantaneous  {label}  (mean {mean_u1a}%)')
        ax2.plot(ts, u3a, color=color, lw=0.8, ls=':', alpha=0.6,
                 label=f'U3 floor  {label}  (mean {mean_u3r:.1f}% of R)')
        ax2.fill_between(ts, u3a, u1a, color=color, alpha=0.10,
                         label=f'Burst gap  {label}  (mean {gap_mean}% of A)')

    ax2.set_ylabel('Utilization / Allocated  (%)', fontfamily=FONT, fontsize=10)
    ax2.set_xlabel('simulated time (hours)', fontfamily=FONT, fontsize=10)
    ax2.set_ylim(0, 35)
    ax2.set_title(
        'Panel B — What fraction of net capacity is actively used?\n'
        '(solid = instantaneous   dotted = floor   shaded = burst gap)',
        fontfamily=FONT, fontsize=10, loc='left', pad=3,
    )
    ax2.legend(fontsize=8, ncol=2)
    ax2.grid(True, alpha=0.22)

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    for ext in ('png', 'svg'):
        fig.savefig(out / f'chart_A_reserved_utilized_time.{ext}',
                    dpi=150, bbox_inches='tight')
    plt.close(fig)
    print('  chart_A_reserved_utilized_time.{png,svg}')


# ---------------------------------------------------------------------------
# Chart B — Five-ratio summary bar chart
# ---------------------------------------------------------------------------

def chart_b(d: dict, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    scheds  = ['batch', 'k8s']
    labels  = ['AWS Batch', 'OKD K8S+']
    colors  = [BC, KC]

    metric_keys = [
        ('r_a',  'R/A  — Reserved / Allocated'),
        ('u1_a', 'U1/A — Instantaneous / Allocated'),
        ('u1_r', 'U1/R — Instantaneous / Reserved'),
        ('u2_r', 'U2/R — Max Theoretical / Reserved'),
        ('u3_r', 'U3/R — Min Theoretical / Reserved'),
    ]

    x = np.arange(len(labels))
    n = len(metric_keys)
    w = 0.13

    for i, (key, lbl) in enumerate(metric_keys):
        vals   = [d[s][key] for s in scheds]
        offset = (i - n / 2 + 0.5) * w
        bars   = ax.bar(x + offset, vals, w, label=lbl,
                        color=colors, alpha=0.30 + i * 0.15)
        for bar, v in zip(bars, vals):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.5,
                f'{v:.1f}%',
                ha='center', va='bottom', fontsize=7.5, fontfamily=FONT,
            )

    # Annotate K8S U2/R note
    k8s_u2r = d['k8s']['u2_r']
    ax.annotate(
        'U2/R: actual spikes can\nexceed soft-limit allocation\n(by design — soft limit is\na scheduling fiction)',
        xy=(1 + 1.5 * w, k8s_u2r), xytext=(1.55, k8s_u2r + 16),
        fontsize=7.5, color='#A32D2D',
        arrowprops=dict(arrowstyle='->', color='#A32D2D', lw=0.7),
    )

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontfamily=FONT, fontsize=11)
    ax.set_ylabel('percentage (%)', fontfamily=FONT)
    ax.set_ylim(0, 130)
    ax.set_title(
        'Five Capacity Ratios — Time-Weighted Means\n'
        'R=Reserved · A=Allocated(net) · '
        'U1=Instantaneous · U2=MaxTheoretical · U3=MinTheoretical',
        fontfamily=FONT, fontsize=10,
    )
    ax.legend(fontsize=8, loc='upper left')
    ax.grid(True, axis='y', alpha=0.22)

    # Burst gap annotations
    for i, sched in enumerate(scheds):
        color = BC if sched == 'batch' else KC
        ax.text(
            i, 3,
            f'Burst gap\n{d[sched]["burst_gap"]}% of Alloc',
            ha='center', fontsize=7.5, color=color, fontfamily=FONT,
            bbox=dict(boxstyle='round,pad=0.2', facecolor='white',
                      alpha=0.7, edgecolor=color),
        )

    plt.tight_layout()
    for ext in ('png', 'svg'):
        fig.savefig(out / f'chart_B_five_ratios_summary.{ext}',
                    dpi=150, bbox_inches='tight')
    plt.close(fig)
    print('  chart_B_five_ratios_summary.{png,svg}')


# ---------------------------------------------------------------------------
# Chart C — Decomposition diagnostic (two-panel, one per scheduler)
# ---------------------------------------------------------------------------

def _diagnose(mean_primary: float, mean_secondary: float) -> tuple[str, str]:
    """Return (diagnosis text, colour) for the annotation box."""
    if mean_primary > 10 and mean_secondary < 5:
        return (
            'Strong YES: decompose Phase 2\n'
            '(bursts active + headroom contested)',
            '#A32D2D',
        )
    elif mean_primary > 10:
        return (
            'Moderate: decomposition reduces cost\n'
            '(bursts active, headroom comfortable)',
            '#E08E27',
        )
    elif mean_secondary < 5:
        return (
            'Investigate: headroom tight\nbut bursts infrequent',
            '#8e44ad',
        )
    else:
        return (
            'K8S+ packing is the win\n'
            '(mostly steady-state;\ndecompose for cost only)',
            '#3B6D11',
        )


def chart_c(d: dict, out: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
    fig.suptitle(
        'Decomposition Diagnostic: Burst Activity and Headroom Slack\n'
        'Primary  (U1−U3)/Alloc  ·  Secondary  (U2−U1)/Alloc',
        fontfamily=FONT, fontsize=11, y=0.99,
    )

    for ax_idx, (sched, color) in enumerate([('batch', BC), ('k8s', KC)]):
        ax    = axes[ax_idx]
        label = 'AWS Batch' if sched == 'batch' else 'OKD K8S+'
        s     = d[sched]['series']
        ts    = [x['t'] / 3600 for x in s]

        primary   = [(x['util1'] - x['util3']) / x['alloc'] * 100
                     if x['alloc'] > 0 else 0 for x in s]
        secondary = [(x['util2'] - x['util1']) / x['alloc'] * 100
                     if x['alloc'] > 0 else 0 for x in s]

        mean_p = statistics.mean(primary)   if primary   else 0.0
        mean_s = statistics.mean(secondary) if secondary else 0.0

        ax.plot(ts, primary,   color=color, lw=1.8, ls='-',
                label=f'Primary (U1−U3)/A  mean={mean_p:.1f}%')
        ax.plot(ts, secondary, color=color, lw=1.8, ls='--',
                label=f'Secondary (U2−U1)/A  mean={mean_s:.1f}%')
        ax.fill_between(ts, 0, primary,   color=color, alpha=0.12)
        ax.fill_between(ts, 0, secondary, color=color, alpha=0.06)

        diag, dcol = _diagnose(mean_p, mean_s)
        ax.text(
            0.02, 0.88, diag,
            transform=ax.transAxes, fontsize=9, color=dcol,
            fontfamily=FONT, verticalalignment='top',
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white',
                      alpha=0.85, edgecolor=dcol, lw=1),
        )

        ax.set_title(label, fontfamily=FONT, fontsize=10, loc='left', pad=3)
        ax.set_ylabel('% of Allocated', fontfamily=FONT, fontsize=9)
        ax.legend(fontsize=9, loc='upper right')
        ax.grid(True, alpha=0.22)
        ymax = max(30, max(primary + secondary, default=0) * 1.25)
        ax.set_ylim(0, ymax)

    axes[1].set_xlabel('simulated time (hours)', fontfamily=FONT, fontsize=10)

    from matplotlib.lines import Line2D
    fig.legend(
        handles=[
            Line2D([0], [0], color='#555', lw=1.8, ls='-',
                   label='solid  = Primary  (U1−U3)/A   [burst activity]'),
            Line2D([0], [0], color='#555', lw=1.8, ls='--',
                   label='dashed = Secondary (U2−U1)/A  [headroom slack]'),
        ],
        fontsize=9, loc='lower center', ncol=2,
        bbox_to_anchor=(0.5, -0.02),
    )

    plt.tight_layout(rect=[0, 0.04, 1, 0.97])
    for ext in ('png', 'svg'):
        fig.savefig(out / f'chart_C_decomposition_diagnostic.{ext}',
                    dpi=150, bbox_inches='tight')
    plt.close(fig)
    print('  chart_C_decomposition_diagnostic.{png,svg}')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description='Generate utilization charts')
    parser.add_argument(
        '--input',
        default='results/utilization_charts/util_metrics.json',
        help='Path to util_metrics.json produced by reproduce_all.sh RUN-05',
    )
    parser.add_argument(
        '--output',
        default='results/utilization_charts',
        help='Directory to write chart files into',
    )
    args = parser.parse_args()

    input_path  = Path(args.input)
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(
            f'{input_path} not found. Run reproduce_all.sh RUN-05 first, '
            'or provide --input path.'
        )

    with open(input_path) as f:
        d = json.load(f)

    print(f'Generating charts from {input_path}')
    print(f'Output directory: {output_path}')
    chart_a(d, output_path)
    chart_b(d, output_path)
    chart_c(d, output_path)
    print('Done.')


if __name__ == '__main__':
    main()
