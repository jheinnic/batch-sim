"""Coverage for scripts/generate_node_timelines.py — the per-node timeline chart tool.

Focuses on run_and_extract(), the data-extraction core shared by both the re-run
and saved-event-log paths. This is where the K8S capacity reconstruction lives
(compute_k8s_capacity), so these tests guard against signature/contract drift
between the schedulers/registry and the charting script (e.g. the BSIM-104 change
of compute_k8s_capacity from a peak-RAM list to a scalar spike_max_gb).
"""
import os
import importlib.util
import pathlib

import pytest
import yaml

os.environ.setdefault("MPLBACKEND", "Agg")  # headless: no display needed for extraction

from batch_sim.core.schemas import (
    CentroidConfig, SimulationConfig, SchedulerType,
)
from batch_sim.generator.event_list import build_event_list, save_event_list

REPO = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "generate_node_timelines.py"
REGISTRY = REPO / "configs" / "instance_registry.yaml"


@pytest.fixture(scope="module")
def gnt():
    """Import the standalone script as a module (main()/argparse are guarded)."""
    spec = importlib.util.spec_from_file_location("gen_node_timelines", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def event_list_path(tmp_path):
    centroid = CentroidConfig(
        id="small", label="Small", arrival_rate_per_hour=40.0, pareto_alpha=2.5,
        download_gb=2.0, preprocess_memory_exponent_a=1.2,
        preprocess_memory_exponent_b=1.4, preprocess_duration_seconds=20.0,
        workhorse_cpu_stages=[60.0, 10.0], workhorse_hard_vcpu=[4],
        io_wait_fraction=0.3, upload_gb=0.2)
    # cool_off_seconds gives run_one a finite horizon so the scale-out monitor
    # terminates (see tests/conftest.py for the same fixture concern).
    sim = SimulationConfig(horizon_seconds=600.0, random_seed=7,
                           cool_off_seconds=1800.0, network_bandwidth_mbps=500.0,
                           centroids=[centroid])
    el = build_event_list(sim)
    p = tmp_path / "events.json"
    save_event_list(el, p)
    return str(p), len(el)


@pytest.fixture
def k8s_cfg_path(tmp_path):
    cfg = {
        "scheduler_type": "k8s",
        "panic_threshold_seconds": 300,
        "sla_target_seconds": 600,
        "warmup_delay_seconds": 5,
        "idle_timeout_seconds": 30,
        "os_overhead_gb": 2.0,
    }
    p = tmp_path / "k8s.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return str(p)


class TestRunAndExtract:
    def test_rerun_path_returns_structure(self, gnt, event_list_path, k8s_cfg_path):
        ev_path, n_jobs = event_list_path
        node_timelines, metadata = gnt.run_and_extract(
            event_list_path=ev_path, scheduler_type="k8s", cfg_path=k8s_cfg_path,
            registry_path=str(REGISTRY), seed=42, os_overhead_gb=2.0)

        assert isinstance(node_timelines, dict) and node_timelines, "expected nodes"
        assert metadata["scheduler"] == "k8s"
        assert metadata["total_nodes"] == len(node_timelines)
        assert metadata["total_jobs"] >= 1
        # every node carries the expected timeline shape
        for nid, nd in node_timelines.items():
            assert {"instance", "ram_gb", "vcpu", "launch_t", "jobs"} <= set(nd)

    def test_k8s_capacity_fields_populated(self, gnt, event_list_path, k8s_cfg_path):
        # Regression guard: this is the compute_k8s_capacity() call site that broke
        # when its signature changed (list -> scalar spike_max_gb). With a K8S
        # scheduler and os_overhead_gb > 0, nodes that ran preprocess jobs must get
        # effective_schedulable_gb / spike_headroom_gb computed without raising.
        ev_path, _ = event_list_path
        node_timelines, _ = gnt.run_and_extract(
            event_list_path=ev_path, scheduler_type="k8s", cfg_path=k8s_cfg_path,
            registry_path=str(REGISTRY), seed=42, os_overhead_gb=2.0)

        with_cap = [nd for nd in node_timelines.values()
                    if nd.get("effective_schedulable_gb") is not None]
        assert with_cap, "no node had K8S capacity reconstructed"
        for nd in with_cap:
            assert nd["spike_headroom_gb"] is not None
            assert 0.0 <= nd["effective_schedulable_gb"] <= nd["ram_gb"]
            # effective = ram - os_overhead - spike_headroom
            assert nd["effective_schedulable_gb"] == pytest.approx(
                nd["ram_gb"] - 2.0 - nd["spike_headroom_gb"], abs=0.05)

    def test_event_log_path_matches_rerun(self, gnt, event_list_path, k8s_cfg_path, tmp_path):
        # The saved-event-log path must reconstruct the same nodes as the re-run path.
        import json
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.config_loader import load_scheduler_config
        from batch_sim.registry.instance_registry import InstanceRegistry
        from batch_sim.generator.event_list import load_event_list

        ev_path, _ = event_list_path
        el = load_event_list(ev_path)
        cfg = load_scheduler_config(k8s_cfg_path)
        registry = InstanceRegistry.from_yaml(str(REGISTRY))
        _, metrics = run_one(event_list=el, cfg=cfg,
                             registry=registry, event_list_path=ev_path, seed=42,
                             return_metrics=True)
        log_path = tmp_path / "saved_events.json"
        log_path.write_text(json.dumps([
            {"event_type": e.event_type.value, "sim_time": e.sim_time, "data": e.data}
            for e in metrics.log]))

        nt_log, _ = gnt.run_and_extract(
            event_list_path=ev_path, scheduler_type="k8s", cfg_path=k8s_cfg_path,
            registry_path=str(REGISTRY), seed=42, event_log_path=str(log_path),
            os_overhead_gb=2.0)
        nt_rerun, _ = gnt.run_and_extract(
            event_list_path=ev_path, scheduler_type="k8s", cfg_path=k8s_cfg_path,
            registry_path=str(REGISTRY), seed=42, os_overhead_gb=2.0)

        # Node IDs are random uuid4 labels (not seeded), so compare structural
        # aggregates rather than the IDs themselves — scheduling is seeded and
        # therefore deterministic in node count and job placement.
        assert len(nt_log) == len(nt_rerun)
        assert sum(len(v["jobs"]) for v in nt_log.values()) == \
               sum(len(v["jobs"]) for v in nt_rerun.values())
