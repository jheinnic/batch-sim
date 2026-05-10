"""BSIM-36: Visualization suite — 6 chart types."""
from __future__ import annotations
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

BATCH_COLOR = "#2C7BB6"
K8S_COLOR   = "#E08E27"
FONT        = "monospace"

def _save(fig, path, stem):
    path.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "svg"):
        fig.savefig(path / f"{stem}.{ext}", dpi=150, bbox_inches="tight")
    plt.close(fig)

def plot_cost_vs_wait(collated, output_dir):
    fig, ax = plt.subplots(figsize=(8, 5))
    for sched, color, label in [("batch", BATCH_COLOR, "AWS Batch"), ("k8s", K8S_COLOR, "OKD/K8S")]:
        runs = [r for r in collated if r["scheduler"] == sched]
        if not runs: continue
        xs = [r["mean_wait_s"] for r in runs]; ys = [r["total_cost_usd"] for r in runs]
        ax.scatter(xs, ys, color=color, label=label, s=80, zorder=3)
        ax.plot(xs, ys, color=color, alpha=0.5, linewidth=1)
        for x, y, t in zip(xs, ys, [r["panic_threshold_s"] for r in runs]):
            ax.annotate(f"{int(t)}s", (x, y), textcoords="offset points", xytext=(4,4), fontsize=7, color=color)
    ax.set_xlabel("Mean Queue Wait (s)", fontfamily=FONT); ax.set_ylabel("Total Cost (USD)", fontfamily=FONT)
    ax.set_title("Cost vs Service Quality — Pareto Frontier", fontfamily=FONT)
    ax.legend(); ax.grid(True, alpha=0.3); _save(fig, output_dir, "01_cost_vs_wait")

def plot_instance_count_over_time(batch_sc, k8s_sc, output_dir):
    fig, ax = plt.subplots(figsize=(10, 4))
    for sc, color, label in [(batch_sc, BATCH_COLOR, "AWS Batch"), (k8s_sc, K8S_COLOR, "OKD/K8S")]:
        series = sc.get("cost_summary", {}).get("node_count_over_time", [])
        if series:
            ax.step([s[0]/3600 for s in series], [s[1] for s in series], color=color, label=label, where="post")
    ax.set_xlabel("Simulated Time (hours)", fontfamily=FONT); ax.set_ylabel("Running Instances", fontfamily=FONT)
    ax.set_title("Instance Count Over Time", fontfamily=FONT); ax.legend(); ax.grid(True, alpha=0.3)
    _save(fig, output_dir, "02_instance_count")

def plot_idle_decomposition(batch_sc, k8s_sc, output_dir):
    fig, ax = plt.subplots(figsize=(6, 4))
    for i, (sc, color) in enumerate([(batch_sc, BATCH_COLOR), (k8s_sc, K8S_COLOR)]):
        idle = sc.get("idle_decomposition", {})
        pre = idle.get("pre_first_job_s", 0)/3600; post = idle.get("post_last_job_s", 0)/3600
        btw = idle.get("between_jobs_s", 0)/3600
        ax.bar(i, pre, color=color, alpha=0.5); ax.bar(i, btw, bottom=pre, color=color, alpha=0.75)
        ax.bar(i, post, bottom=pre+btw, color=color, alpha=1.0)
    ax.set_xticks([0,1]); ax.set_xticklabels(["AWS Batch", "OKD/K8S"], fontfamily=FONT)
    ax.set_ylabel("Idle Instance-Hours", fontfamily=FONT); ax.set_title("Node Idle Time", fontfamily=FONT)
    ax.grid(True, axis="y", alpha=0.3); _save(fig, output_dir, "03_idle_decomposition")

def plot_cost_over_time(batch_sc, k8s_sc, output_dir):
    fig, ax = plt.subplots(figsize=(10, 4))
    for sc, color, label in [(batch_sc, BATCH_COLOR, "AWS Batch"), (k8s_sc, K8S_COLOR, "OKD/K8S")]:
        series = sc.get("cost_summary", {}).get("cost_over_time", [])
        if series: ax.plot([s[0]/3600 for s in series], [s[1] for s in series], color=color, label=label)
    ax.set_xlabel("Simulated Time (hours)", fontfamily=FONT); ax.set_ylabel("Cumulative Cost (USD)", fontfamily=FONT)
    ax.set_title("EC2 Cost Accumulation", fontfamily=FONT); ax.legend(); ax.grid(True, alpha=0.3)
    _save(fig, output_dir, "04_cost_over_time")

def plot_wait_time_by_centroid(batch_sc, k8s_sc, output_dir):
    bc = batch_sc.get("job_stats", {}).get("per_centroid", {})
    kc = k8s_sc.get("job_stats", {}).get("per_centroid", {})
    cids = sorted(set(list(bc) + list(kc)))
    if not cids: return
    x = np.arange(len(cids)); w = 0.35
    fig, ax = plt.subplots(figsize=(8, 4))
    bm = [(bc.get(c, {}).get("queue_wait_s") or {}).get("mean", 0) or 0 for c in cids]
    km = [(kc.get(c, {}).get("queue_wait_s") or {}).get("mean", 0) or 0 for c in cids]
    bs = [(bc.get(c, {}).get("queue_wait_s") or {}).get("stddev", 0) or 0 for c in cids]
    ks = [(kc.get(c, {}).get("queue_wait_s") or {}).get("stddev", 0) or 0 for c in cids]
    ax.bar(x-w/2, bm, w, yerr=bs, color=BATCH_COLOR, label="AWS Batch", capsize=4)
    ax.bar(x+w/2, km, w, yerr=ks, color=K8S_COLOR, label="OKD/K8S", capsize=4)
    ax.set_xticks(x); ax.set_xticklabels(cids, fontfamily=FONT, fontsize=9)
    ax.set_ylabel("Mean Queue Wait (s)", fontfamily=FONT); ax.set_title("Per-Centroid Wait Time", fontfamily=FONT)
    ax.legend(); ax.grid(True, axis="y", alpha=0.3); _save(fig, output_dir, "05_wait_by_centroid")

def plot_retry_histogram(k8s_sc, output_dir):
    stats = (k8s_sc.get("job_stats") or {}).get("pool_retry_count") or {}
    if not stats or not stats.get("count"): return
    total = stats.get("count", 0); crashed = k8s_sc.get("job_stats", {}).get("pool_crash_count", 0)
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.bar([0,1], [total-crashed, crashed], color=[K8S_COLOR, "#C0392B"])
    ax.set_xticks([0,1]); ax.set_xticklabels(["0 retries", "≥1 retry"], fontfamily=FONT)
    ax.set_ylabel("Jobs", fontfamily=FONT)
    ax.set_title(f"K8S Retry Distribution (mean: {stats.get('mean', 0):.2f})", fontfamily=FONT)
    ax.grid(True, axis="y", alpha=0.3); _save(fig, output_dir, "06_k8s_retries")

def generate_all_charts(collated, batch_sc, k8s_sc, output_dir):
    plots_dir = Path(output_dir) / "plots"
    plot_cost_vs_wait(collated, plots_dir)
    plot_instance_count_over_time(batch_sc, k8s_sc, plots_dir)
    plot_idle_decomposition(batch_sc, k8s_sc, plots_dir)
    plot_cost_over_time(batch_sc, k8s_sc, plots_dir)
    plot_wait_time_by_centroid(batch_sc, k8s_sc, plots_dir)
    plot_retry_histogram(k8s_sc, plots_dir)
