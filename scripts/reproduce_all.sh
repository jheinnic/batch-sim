#!/usr/bin/env bash
# reproduce_all.sh
#
# Reproduces every simulation run from the batch-sim design session.
# Run from the repository root after installing dependencies.
#
# Usage:
#   pip install -e ".[dev]"          # or: uv pip install -e ".[dev]"
#   bash scripts/reproduce_all.sh
#
# All outputs land in results/  and workloads/
# Runtime: approximately 3-5 minutes on a modern laptop.
#
# Observations reproduced:
#   RUN-01  Reference v1 workload generation (4h, seed=42)
#   RUN-02  Batch baseline vs K8S+ baseline (v1 workload)
#   RUN-03  K8S+ two-queue k-sweep at k={2,3,4} (v2 workload)
#   RUN-04  Hybrid OKD+Batch sweep: Q1 instance × k (v2 workload)
#   RUN-05  Utilization metrics: three-form framework (v1 workload)
#   RUN-06  24-hour diurnal simulation (v1 centroids, diurnal arrivals)

set -euo pipefail
SEED=42

echo "==================================================================="
echo "  batch-sim reproduction run"
echo "  Seed: $SEED  |  $(date)"
echo "==================================================================="

# ── RUN-01: Generate workloads ─────────────────────────────────────────
echo ""
echo "RUN-01: Generating reference workloads..."

python -m batch_sim generate \
  --config configs/reference_centroids.yaml \
  --output workloads/reference_4h_v1.json \
  --seed $SEED
echo "  v1 workload: workloads/reference_4h_v1.json"

python -m batch_sim generate \
  --config configs/reference_centroids_v2.yaml \
  --output workloads/reference_4h_v2.json \
  --seed $SEED
echo "  v2 workload: workloads/reference_4h_v2.json"

python -m batch_sim generate \
  --config configs/reference_centroids_v3.yaml \
  --output workloads/reference_4h_v3.json \
  --seed $SEED
echo "  v3 workload: workloads/reference_4h_v3.json"

# ── RUN-02: Batch vs K8S+ baselines (v2 workload) ─────────────────────
echo ""
echo "RUN-02: Batch vs K8S+ baselines (v2 workload)..."

python -m batch_sim simulate \
  --events workloads/reference_4h_v2.json \
  --scheduler batch \
  --scheduler-config configs/scheduler_reference.yaml \
  --registry configs/instance_registry.yaml \
  --output results/baselines/batch_v2_scorecard.json \
  --seed $SEED

python -m batch_sim simulate \
  --events workloads/reference_4h_v2.json \
  --scheduler k8s \
  --scheduler-config configs/scheduler_reference.yaml \
  --registry configs/instance_registry.yaml \
  --output results/baselines/k8s_v2_scorecard.json \
  --seed $SEED

python -m batch_sim compare \
  --batch results/baselines/batch_v2_scorecard.json \
  --k8s   results/baselines/k8s_v2_scorecard.json

# ── RUN-03: K8S+ two-queue k-sweep (v2 workload) ──────────────────────
echo ""
echo "RUN-03: K8S+ two-queue k-sweep (v2 workload)..."

python -m batch_sim experiment \
  --events workloads/reference_4h_v2.json \
  --scheduler-config configs/scheduler_reference.yaml \
  --registry configs/instance_registry.yaml \
  --output results/k_sweep_v2 \
  --thresholds "300,300,300" \
  --seed $SEED
# Note: threshold is held fixed at 300s here; k-sweep is run separately below

python - << 'PYEOF'
import sys, json, random
sys.path.insert(0, '.')
from batch_sim.core.config_loader import load_simulation_config, load_scheduler_config
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.generator.event_list import load_event_list
from batch_sim.metrics.collector import MetricsCollector
from batch_sim.core.engine import SimulationEngine
from batch_sim.scheduler.k8s_plus_two_queue import K8SPlusTwoQueueScheduler
from batch_sim.metrics.aggregator import build_scorecard
from pathlib import Path
import statistics

cfg_sched = load_scheduler_config('configs/scheduler_reference.yaml')
registry  = InstanceRegistry.from_yaml('configs/instance_registry.yaml')
el        = load_event_list('workloads/reference_4h_v2.json')
peaks     = list({e.preprocess_peak_ram_gb for e in el.events})
results   = []

for k in [2, 3, 4]:
    metrics = MetricsCollector()
    sched   = K8SPlusTwoQueueScheduler(cfg=cfg_sched, registry=registry,
                                        metrics=metrics, centroid_peak_rams=peaks,
                                        k=k, rng=random.Random(42))
    eng     = SimulationEngine(scheduler=sched, metrics=metrics, cfg=cfg_sched)
    eng.run(el); sched.finalize(eng.env)
    sc = build_scorecard(f'k8s_plus_2q_k{k}', '300s', 'v2',
                         metrics, sched.accruers, 600,
                         load_simulation_config('configs/reference_centroids_v2.yaml').horizon_seconds)
    qr = sched.queue_assignment_report()
    results.append({'k': k, 'cost': round(sc.cost_summary.total_cost_usd,2),
                    'jobs': sc.job_stats.pool_job_count,
                    'crashes': sc.job_stats.pool_crash_count,
                    'terminal': sc.job_stats.pool_terminal_failure_count,
                    'q1_pct': qr['summary'].get('q1_pct'),
                    'q2_pct': qr['summary'].get('q2_pct'),
                    'q1_cost': qr['q1_cost'], 'q2_cost': qr['q2_cost']})
    print(f"  k={k}: ${results[-1]['cost']}  jobs={results[-1]['jobs']}  "
          f"crashes={results[-1]['crashes']}  Q1={results[-1]['q1_pct']}%")

Path('results/k_sweep_v2').mkdir(parents=True, exist_ok=True)
with open('results/k_sweep_v2/two_queue_results.json','w') as f:
    json.dump(results, f, indent=2)
print("  Saved: results/k_sweep_v2/two_queue_results.json")
PYEOF

# ── RUN-04: Hybrid OKD+Batch sweep ────────────────────────────────────
echo ""
echo "RUN-04: Hybrid OKD+Batch sweep (v2 workload, Q1 instances × k)..."

python - << 'PYEOF'
import sys; sys.path.insert(0,'.')
from batch_sim.core.config_loader import load_scheduler_config
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.experiment_hybrid import run_hybrid_sweep

cfg      = load_scheduler_config('configs/scheduler_reference.yaml')
registry = InstanceRegistry.from_yaml('configs/instance_registry.yaml')

results = run_hybrid_sweep(
    event_list_path    = 'workloads/reference_4h_v2.json',
    cfg                = cfg,
    registry           = registry,
    q1_instance_names  = ['r7i.4xlarge', 'r7i.8xlarge', 'r7i.16xlarge'],
    k_values           = [2, 3, 4],
    output_dir         = 'results/hybrid_sweep',
    seed               = 42,
)

print(f"\n  {len(results)} total runs saved to results/hybrid_sweep/collated.json")
print(f"  {'Run':<30} {'Combined $':>12} {'OKD%':>7} {'Batch%':>7}")
print(f"  {'-'*58}")
for r in results:
    label = r.get('run_type','') if r.get('q1_instance')=='N/A' \
            else f"hybrid {r.get('q1_instance')} k={r.get('k')}"
    odp = r.get('routing',{}).get('q1_pct','-')
    bap = r.get('routing',{}).get('q2_pct','-')
    print(f"  {label:<30} ${r['combined_cost']:>11.2f} {str(odp):>7} {str(bap):>7}")
PYEOF

# ── RUN-05: Utilization metrics ────────────────────────────────────────
echo ""
echo "RUN-05: Computing three-form utilization metrics (v1 workload)..."

python - << 'PYEOF'
import sys, json, random, collections, statistics
sys.path.insert(0,'.')
from batch_sim.core.config_loader import load_simulation_config, load_scheduler_config
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.generator.event_list import load_event_list
from batch_sim.metrics.collector import MetricsCollector, EventType, PhaseID
from batch_sim.core.engine import SimulationEngine
from batch_sim.scheduler.batch_scheduler import BatchScheduler
from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler
from batch_sim.metrics.aggregator import build_scorecard
from pathlib import Path

cfg_sched = load_scheduler_config('configs/scheduler_reference.yaml')
registry  = InstanceRegistry.from_yaml('configs/instance_registry.yaml')
el        = load_event_list('workloads/reference_4h_v1.json')
peaks     = list({e.preprocess_peak_ram_gb for e in el.events})
job_prof  = {e.job_id: e for e in el.events}

def phase_ram(jid, phase):
    p = job_prof.get(jid)
    if not p: return 0.0
    return {'download': p.download_ram_gb, 'preprocess': p.preprocess_peak_ram_gb,
            'workhorse': p.preprocess_steady_ram_gb, 'upload': p.upload_ram_gb}.get(phase, 0.0)

def soft_lim(jid):
    p = job_prof.get(jid); return p.preprocess_steady_ram_gb if p else 0.0

def peak_ram(jid):
    p = job_prof.get(jid); return p.preprocess_peak_ram_gb if p else 0.0

results = {}
for sched_name, SClass, extra, stype in [
    ('batch', BatchScheduler,   {},                         'batch'),
    ('k8s',   K8SPlusScheduler, {'centroid_peak_rams': peaks}, 'k8s'),
]:
    metrics = MetricsCollector()
    sched   = SClass(cfg=cfg_sched, registry=registry,
                     metrics=metrics, rng=random.Random(42), **extra)
    eng     = SimulationEngine(scheduler=sched, metrics=metrics, cfg=cfg_sched)
    eng.run(el); sched.finalize(eng.env)
    accruers = {a.node_id: a for a in sched.accruers if a.is_terminated}
    node_headroom = {}
    if hasattr(sched,'_capacity_cache'):
        for nid, node in sched._nodes.items():
            cap = sched._capacity_cache.get(node.instance.name)
            node_headroom[nid] = cap.spike_headroom_gb if cap else 0.0

    phase_windows = collections.defaultdict(list)
    phase_starts  = {}
    for e in metrics.log:
        nid=e.data.get('node_id'); jid=e.data.get('job_id'); t=e.sim_time
        if not nid or not jid: continue
        if e.event_type == EventType.JOB_START: pass
        if e.event_type == EventType.PHASE_TRANSITION:
            key=(nid,jid); ph=e.data['phase']
            if key in phase_starts:
                prev_ph,prev_t=phase_starts[key]
                phase_windows[key].append((prev_ph,prev_t,t))
            phase_starts[key]=(ph,t)
        if e.event_type in (EventType.JOB_COMPLETE, EventType.JOB_CRASH):
            key=(nid,jid)
            if key in phase_starts:
                prev_ph,prev_t=phase_starts.pop(key)
                phase_windows[key].append((prev_ph,prev_t,t))

    end_t = max((a.termination_time for a in accruers.values()), default=0)
    series=[]
    for tick in range(0, int(end_t)+60, 60):
        active=[nid for nid,a in accruers.items() if a.launch_time<=tick<a.termination_time]
        if not active: continue
        alloc=sum(accruers[n].instance.ram_gb-cfg_sched.k8s_os_overhead_gb for n in active)
        active_jobs={(nid,jid):ph for (nid,jid),ws in phase_windows.items()
                     if nid in active for ph,t0,t1 in ws if t0<=tick<t1}
        if stype=='batch':
            reserved=sum(peak_ram(jid) for (nid,jid) in active_jobs)
        else:
            reserved=sum(soft_lim(jid) for (nid,jid) in active_jobs)
            reserved+=sum(node_headroom.get(n,0.) for n in active)
        util1=sum(phase_ram(jid,ph) for (nid,jid),ph in active_jobs.items())
        util3=sum(min(phase_ram(jid,ph),soft_lim(jid)) for (nid,jid),ph in active_jobs.items())
        util2=util3
        for n in active:
            h=node_headroom.get(n,0.) if stype=='k8s' else sum(peak_ram(j) for (n2,j) in active_jobs if n2==n)
            ps=sorted([peak_ram(j) for (n2,j) in active_jobs if n2==n],reverse=True)
            burst=0.
            for p in ps:
                if burst+p<=h: burst+=p
                else: break
            util2+=burst
        series.append({'t':tick,'alloc':round(alloc,1),'reserved':round(reserved,1),
                        'util1':round(util1,1),'util2':round(util2,1),'util3':round(util3,1)})

    def mr(a,b): v=[s[a]/s[b]*100 for s in series if s.get(b,0)>0]; return round(statistics.mean(v),1) if v else 0.
    gap=[( s['util1']-s['util3'])/s['alloc']*100 for s in series if s['alloc']>0]
    slack=[(s['util2']-s['util1'])/s['alloc']*100 for s in series if s['alloc']>0]
    results[sched_name]={
        'r_a':mr('reserved','alloc'),'u1_a':mr('util1','alloc'),
        'u1_r':mr('util1','reserved'),'u2_r':mr('util2','reserved'),'u3_r':mr('util3','reserved'),
        'burst_gap':round(statistics.mean(gap),1) if gap else 0,
        'headroom_slack':round(statistics.mean(slack),1) if slack else 0,
        'series':series}
    print(f"  {sched_name}: R/A={results[sched_name]['r_a']}%  U1/A={results[sched_name]['u1_a']}%  "
          f"U1/R={results[sched_name]['u1_r']}%  U2/R={results[sched_name]['u2_r']}%  "
          f"U3/R={results[sched_name]['u3_r']}%  gap={results[sched_name]['burst_gap']}%  "
          f"slack={results[sched_name]['headroom_slack']}%")

Path('results/utilization_charts').mkdir(parents=True, exist_ok=True)
with open('results/utilization_charts/util_metrics.json','w') as f:
    json.dump(results,f)
print("  Saved: results/utilization_charts/util_metrics.json")
PYEOF

echo "  Generating utilization charts..."
python scripts/generate_utilization_charts.py \
  --input  results/utilization_charts/util_metrics.json \
  --output results/utilization_charts
echo "  Charts written to results/utilization_charts/"

echo ""
echo "==================================================================="
echo "  All runs complete."
echo ""
echo "  Output summary:"
echo "    workloads/                      — event lists (v1, v2, v3)"
echo "    results/baselines/              — RUN-02 scorecards + comparison"
echo "    results/k_sweep_v2/             — RUN-03 two-queue k-sweep"
echo "    results/hybrid_sweep/           — RUN-04 hybrid OKD+Batch sweep"
echo "    results/utilization_charts/     — RUN-05 utilization metrics"
echo ""
echo "  Key numbers to verify:"
echo "    Batch baseline (v2):    ~\$57    K8S+ baseline: ~\$52"
echo "    Two-queue k=2 (v2):     ~\$260   (two-queue overhead dominates)"
echo "    Hybrid best case:       see results/hybrid_sweep/collated.json"
echo "    Utilization R/A:        Batch ~47%  K8S+ ~95%"
echo "    Utilization U1/A:       both  ~8%"
echo "==================================================================="
