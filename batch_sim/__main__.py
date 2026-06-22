"""batch_sim CLI — subcommands: generate, simulate, validate-tiers, compare, experiment, plot"""
from __future__ import annotations
import json
from pathlib import Path
import click
from rich.console import Console
from rich.table import Table

console = Console()

@click.group()
def cli():
    """Batch compute simulation: AWS Batch vs OKD/K8S."""

@cli.command()
@click.option("--config", required=True, type=click.Path(exists=True))
@click.option("--output", required=True, type=click.Path())
@click.option("--seed", default=42, show_default=True, type=int)
def generate(config, output, seed):
    """Stage 1: generate a reproducible event list."""
    from batch_sim.core.config_loader import load_simulation_config
    from batch_sim.generator.event_list import build_event_list, save_event_list
    cfg = load_simulation_config(config)
    cfg = cfg.model_copy(update={"random_seed": seed})
    console.print("[bold]Generating event list…[/bold]")
    el = build_event_list(cfg)
    save_event_list(el, output)
    console.print(f"[green]✓ {len(el)} events → {output}[/green]")
    t = Table(title="Event List Summary")
    t.add_column("Centroid"); t.add_column("Jobs", justify="right")
    for cid, count in sorted(el.centroid_counts().items()): t.add_row(cid, str(count))
    t.add_row("[bold]TOTAL[/bold]", f"[bold]{len(el)}[/bold]")
    console.print(t)

@cli.command()
@click.option("--events", required=True, type=click.Path(exists=True))
@click.option("--scheduler-config", required=True, type=click.Path(exists=True))
@click.option("--registry", default="configs/instance_registry.yaml", type=click.Path(exists=True))
@click.option("--output", required=True, type=click.Path())
@click.option("--seed", default=42, show_default=True, type=int)
def simulate(events, scheduler_config, registry, output, seed):
    """Stage 2: run one scheduler against a saved event list.

    BSIM-123: the scheduler is determined by the config (its scheduler_type), so
    there is no --scheduler flag to disagree with it.
    """
    from batch_sim.core.config_loader import load_scheduler_config
    from batch_sim.registry.instance_registry import InstanceRegistry
    from batch_sim.experiment_runner import run_one
    from batch_sim.generator.event_list import load_event_list
    cfg = load_scheduler_config(scheduler_config)
    reg = InstanceRegistry.from_yaml(registry)
    el = load_event_list(events)
    console.print(f"[bold]Running {cfg.scheduler_type.value.upper()}…[/bold]")
    sc, metrics = run_one(event_list=el, cfg=cfg, registry=reg, event_list_path=events,
                          seed=seed, return_metrics=True)
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    sc.save(output)
    # Save full event log so reporting tools read it back without re-simulating
    import os
    base = output if not output.endswith('.json') else output[:-5]
    log_path = base + '_events.json'
    metrics.save(log_path)
    console.print(f"[green]✓ Scorecard  → {output}[/green]")
    console.print(f"[green]✓ Event log  → {log_path}[/green]")
    _print_summary(sc)

@cli.command(name="validate-tiers")
@click.option("--sim-config", required=True, type=click.Path(exists=True))
@click.option("--scheduler-config", required=True, type=click.Path(exists=True))
@click.option("--registry", default="configs/instance_registry.yaml", type=click.Path(exists=True))
def validate_tiers(sim_config, scheduler_config, registry):
    """BSIM-114: preflight tier-compatibility check for a sim/scheduler config pair.

    Catches what Pydantic can't: compatible_tiers (sim config) referencing tier
    names that don't exist in the scheduler config's tiers registry, tiers whose
    spike_max_gb leaves no schedulable zone, and centroid bins with no burst-viable
    tier. No-ops cleanly for BatchConfig or a tier-less K8SConfig.
    """
    from batch_sim.core.config_loader import load_simulation_config, load_scheduler_config
    from batch_sim.registry.instance_registry import InstanceRegistry
    from batch_sim.core.tier_preflight import validate_config_pair, TierPreflightError
    sim_cfg = load_simulation_config(sim_config)
    sched_cfg = load_scheduler_config(scheduler_config)
    reg = InstanceRegistry.from_yaml(registry)
    try:
        validate_config_pair(sim_cfg, sched_cfg, reg)
    except TierPreflightError as e:
        raise click.ClickException(str(e))
    console.print("[green]✓ Tier preflight passed[/green]")

@cli.command()
@click.option("--batch", required=True, type=click.Path(exists=True))
@click.option("--k8s", required=True, type=click.Path(exists=True))
def compare(batch, k8s):
    """Side-by-side comparison of two scorecard files."""
    from batch_sim.metrics.aggregator import compare_scorecards
    comparison = compare_scorecards(batch, k8s)
    t = Table(title="Batch vs K8S"); t.add_column("Metric")
    t.add_column("Batch", justify="right"); t.add_column("K8S", justify="right")
    t.add_column("Delta", justify="right")
    fmt = lambda v: f"{v:.3f}" if isinstance(v, float) else str(v)
    for metric, vals in comparison.items():
        if not vals: continue
        d = vals.get("delta", "—")
        color = "green" if isinstance(d, float) and d < 0 else "red"
        t.add_row(metric, fmt(vals.get("batch","—")), fmt(vals.get("k8s","—")),
                  f"[{color}]{fmt(d)}[/{color}]")
    console.print(t)

@cli.command()
@click.option("--events", required=True, type=click.Path(exists=True))
@click.option("--scheduler-config", required=True, type=click.Path(exists=True))
@click.option("--registry", default="configs/instance_registry.yaml", type=click.Path(exists=True))
@click.option("--output", required=True, type=click.Path())
@click.option("--thresholds", default="60,180,300,600,900,1800,3600", show_default=True)
@click.option("--seed", default=42, show_default=True, type=int)
def experiment(events, scheduler_config, registry, output, thresholds, seed):
    """Full panic-threshold sweep across both schedulers."""
    from batch_sim.core.config_loader import load_scheduler_config
    from batch_sim.registry.instance_registry import InstanceRegistry
    from batch_sim.experiment_runner import run_experiment, build_pareto_frontier, detect_meta_effect
    from batch_sim.metrics.visualize import generate_all_charts
    threshold_vals = [float(t.strip()) for t in thresholds.split(",")]
    cfg = load_scheduler_config(scheduler_config)
    reg = InstanceRegistry.from_yaml(registry)
    collated = run_experiment(event_list_path=events, panic_threshold_values=threshold_vals,
                              base_cfg=cfg, registry=reg, output_dir=output, seed=seed)
    frontiers = {s: build_pareto_frontier(collated, s) for s in ("batch", "k8s")}
    with open(Path(output)/"pareto_frontiers.json","w") as f: json.dump(frontiers, f, indent=2)
    meta = detect_meta_effect(collated)
    with open(Path(output)/"meta_effect.json","w") as f: json.dump(meta, f, indent=2)
    mid = sorted(threshold_vals)[len(threshold_vals)//2]
    try:
        _load = lambda s: json.load(open(Path(output)/s/f"threshold_{int(mid)}"/"scorecard.json"))
        generate_all_charts(collated, _load("batch"), _load("k8s"), output)
        console.print(f"[green]✓ Charts → {output}/plots/[/green]")
    except FileNotFoundError as e:
        console.print(f"[yellow]Charts skipped: {e}[/yellow]")
    console.print(f"[green]✓ Experiment complete → {output}/[/green]")

@cli.command()
@click.option("--experiment-dir", required=True, type=click.Path(exists=True))
@click.option("--threshold", default=None, type=float)
def plot(experiment_dir, threshold):
    """Regenerate all charts from a saved experiment directory."""
    from batch_sim.metrics.visualize import generate_all_charts
    exp = Path(experiment_dir)
    with open(exp/"collated.json") as f: collated = json.load(f)
    if threshold is None:
        threshold = sorted(set(r["panic_threshold_s"] for r in collated))[len(collated)//4]
    _load = lambda s: json.load(open(exp/s/f"threshold_{int(threshold)}"/"scorecard.json"))
    generate_all_charts(collated, _load("batch"), _load("k8s"), exp)
    console.print(f"[green]✓ Charts → {exp}/plots/[/green]")

def _print_summary(sc):
    t = Table(title="Run Summary"); t.add_column("Metric"); t.add_column("Value", justify="right")
    js = sc.job_stats; cs = sc.cost_summary; sm = sc.storage_metrics
    if sm is not None:
        t.add_row("Compute cost (EC2)", f"${sm.compute_cost_usd:.4f}")
        t.add_row("Storage cost (EBS)", f"${sm.storage_cost_usd:.4f}  ({sm.storage_pct:.1f}% of total)")
        t.add_row("Total cost", f"${sm.total_cost_usd:.4f}")
    else:
        t.add_row("Total EC2 cost", f"${cs.total_cost_usd:.4f}")
    t.add_row("Jobs completed", str(js.pool_job_count))
    t.add_row("SLA breaches", str(js.pool_sla_breach_count))
    t.add_row("Crashes", str(js.pool_crash_count))
    t.add_row("Panic triggers", str(js.pool_panic_trigger_count))
    t.add_row("Mean wait", f"{(js.pool_queue_wait_s or {}).get('mean', 0) or 0:.1f}s")
    console.print(t)

if __name__ == "__main__":
    cli()
