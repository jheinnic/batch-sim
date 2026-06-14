"""BSIM-104-108: Multi-tier boost provisioning — schema, generation, placement,
joint provisioner, and admission."""
import warnings
from collections import Counter

import pytest
import simpy
import numpy as np

from batch_sim.core.schemas import (
    TierProfile, QueueDefinition, parse_tier_set,
    CentroidConfig, SchedulerConfig, SchedulerType, InstanceTypeConfig,
    InstanceFamily, InstanceRegistryConfig,
)
from batch_sim.core.engine import Priority
from batch_sim.generator.job_spec import JobSpec, PhaseProfile
from batch_sim.generator.sampler import sample_job
from batch_sim.metrics.collector import MetricsCollector, EventType
from batch_sim.registry.instance_registry import InstanceRegistry, compute_k8s_capacity
from batch_sim.scheduler.k8s_scheduler import K8SScheduler
from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tier_registry():
    # Three tiers below all share r7i.16xlarge (256 GB / 64 vCPU).
    return InstanceRegistry(InstanceRegistryConfig(instance_types=[
        InstanceTypeConfig(name="r7i.16xlarge", family=InstanceFamily.MEMORY,
                           ram_gb=256, vcpu=64, hourly_price_usd=4.0),
    ]))


def _tiers():
    return [
        TierProfile(name="small_boost",  spike_max_gb=16.0,  spawn_instance_class="r7i.16xlarge"),
        TierProfile(name="medium_boost", spike_max_gb=64.0,  spawn_instance_class="r7i.16xlarge"),
        TierProfile(name="large_boost",  spike_max_gb=128.0, spawn_instance_class="r7i.16xlarge"),
    ]


@pytest.fixture
def tier_cfg():
    return SchedulerConfig(
        scheduler_type=SchedulerType.K8S,
        panic_threshold_seconds=300.0, sla_target_seconds=600.0,
        warmup_delay_seconds=1.0, idle_timeout_seconds=30.0,
        idle_check_interval_seconds=10.0, max_retries=3, replay_delay_seconds=2.0,
        k8s_os_overhead_gb=0.0, scale_out_threshold_s=0.0, scale_out_poll_s=30.0,
        tiers=_tiers())


def _make_job(soft_gb, burst_gb, tiers, vcpu=2, jid=None):
    """Build a JobSpec with exact soft-limit and burst (peak = soft + burst)."""
    prof = PhaseProfile(download_duration_s=0.0,
                        preprocess_peak_ram_gb=soft_gb + burst_gb,
                        workhorse_hard_limit_gb=soft_gb,
                        workhorse_declared_vcpu=vcpu)
    kw = dict(centroid_id="c", profile=prof, soft_cpu=vcpu, hard_cpu=vcpu,
              compatible_tiers=list(tiers))
    if jid is not None:
        kw["job_id"] = jid
    return JobSpec(**kw)


def _provision_once(cfg, registry, jobs, sched_cls=K8SScheduler):
    """Drive only the joint provisioner for a batch of pending jobs and return
    the Counter of launched-node tiers."""
    env = simpy.Environment()
    metrics = MetricsCollector()
    sched = sched_cls(cfg=cfg, registry=registry, metrics=metrics,
                      centroid_peak_rams=[], centroid_tier_config={}, rng=None)
    sched._env = env  # bypass _setup so the scale-out monitor never starts
    for j in jobs:
        sched._job_compatible_tiers[j.job_id] = list(j.compatible_tiers)
        sched._queue.enqueue(j, arrival_time=0.0, priority=Priority.NORMAL, enqueue_time=0.0)
    sched._provision_to_demand_joint(env)
    env.run(until=cfg.warmup_delay_seconds + 1.0)
    return Counter(sched._node_tier_name.values()), sched


# ---------------------------------------------------------------------------
# BSIM-104: schema
# ---------------------------------------------------------------------------

class TestTierSchema:
    def test_parse_tier_set(self):
        assert parse_tier_set("a;b;c") == ["a", "b", "c"]
        assert parse_tier_set("solo") == ["solo"]
        assert parse_tier_set(" a ; b ;") == ["a", "b"]

    def test_queue_definition_alias(self):
        assert QueueDefinition is TierProfile

    def test_delimiter_in_name_rejected(self):
        with pytest.raises(Exception):
            TierProfile(name="a;b", spike_max_gb=16, spawn_instance_class="r7i.16xlarge")

    def test_queue_name_promotes_to_compatible_tiers(self):
        base = dict(id="c", label="t", arrival_rate_per_hour=1.0, pareto_alpha=2.0,
            download_gb=2.0, preprocess_memory_exponent_a=1.0, preprocess_memory_exponent_b=1.0,
            preprocess_duration_seconds=10.0, workhorse_cpu_stages=[60.0, 10.0],
            workhorse_hard_vcpu=[4], io_wait_fraction=0.0, upload_gb=0.1)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c = CentroidConfig(**base, queue_name="small_boost")
            assert c.compatible_tiers == "small_boost"
            assert any(issubclass(x.category, DeprecationWarning) for x in w)

    def test_tiers_queues_bidirectional_sync(self):
        sc = SchedulerConfig(scheduler_type=SchedulerType.K8S, tiers=_tiers())
        assert {t.name for t in sc.queues} == {"small_boost", "medium_boost", "large_boost"}

    def test_per_bin_compatible_tiers_length_check(self):
        base = dict(id="c", label="t", arrival_rate_per_hour=1.0, pareto_alpha=2.0,
            download_gb=2.0, preprocess_memory_exponent_a=1.0, preprocess_memory_exponent_b=1.0,
            preprocess_duration_seconds=10.0, workhorse_cpu_stages=[60.0, 10.0],
            workhorse_hard_vcpu=[4], io_wait_fraction=0.0, upload_gb=0.1)
        with pytest.raises(Exception):
            CentroidConfig(**base, centroid_bin_weights=[1, 1],
                           compatible_tiers=["a", "b", "c"])


# ---------------------------------------------------------------------------
# BSIM-105: generation
# ---------------------------------------------------------------------------

class TestTierGeneration:
    def _centroid(self, **kw):
        base = dict(id="c", label="t", arrival_rate_per_hour=1.0, pareto_alpha=2.0,
            download_gb=2.0, preprocess_memory_exponent_a=1.0, preprocess_memory_exponent_b=1.0,
            preprocess_duration_seconds=10.0, workhorse_cpu_stages=[60.0, 10.0],
            workhorse_hard_vcpu=[4], io_wait_fraction=0.0, upload_gb=0.1)
        base.update(kw)
        return CentroidConfig(**base)

    def test_scalar_multitier_all_bins(self):
        c = self._centroid(compatible_tiers="small_boost;medium_boost")
        j = sample_job(c, np.random.default_rng(1), 500.0)
        assert j.compatible_tiers == ["small_boost", "medium_boost"]

    def test_per_bin_resolution(self):
        c = self._centroid(centroid_bin_weights=[0.5, 0.5],
                           compatible_tiers=["small_boost;medium_boost", "large_boost"])
        rng = np.random.default_rng(2)
        for _ in range(20):
            j = sample_job(c, rng, 500.0)
            if j.bin_idx == 0:
                assert j.compatible_tiers == ["small_boost", "medium_boost"]
            else:
                assert j.compatible_tiers == ["large_boost"]


# ---------------------------------------------------------------------------
# BSIM-106: placement set-membership
# ---------------------------------------------------------------------------

class TestPlacementMembership:
    def test_node_compatible_membership(self, tier_cfg, tier_registry):
        env = simpy.Environment()
        sched = K8SScheduler(cfg=tier_cfg, registry=tier_registry, metrics=MetricsCollector(),
                             centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._node_tier_name["n1"] = "small_boost"
        assert sched._node_compatible("n1", ["small_boost", "medium_boost"])
        assert not sched._node_compatible("n1", ["medium_boost", "large_boost"])

    def test_no_tiers_means_any_node(self, tier_registry):
        cfg = SchedulerConfig(scheduler_type=SchedulerType.K8S,
                              panic_threshold_seconds=300.0, sla_target_seconds=600.0)
        sched = K8SScheduler(cfg=cfg, registry=tier_registry, metrics=MetricsCollector(),
                             centroid_peak_rams=[10.0], centroid_tier_config={}, rng=None)
        # legacy mode: no tier_defs → any node compatible regardless of job tiers
        sched._node_tier_name["n1"] = ""
        assert sched._node_compatible("n1", [])

    def test_viable_tiers_filters_by_burst(self, tier_cfg, tier_registry):
        sched = K8SScheduler(cfg=tier_cfg, registry=tier_registry, metrics=MetricsCollector(),
                             centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        # burst 60 → only medium(64)/large(128), not small(16)
        job = _make_job(soft_gb=8, burst_gb=60, tiers=["small_boost", "medium_boost", "large_boost"])
        assert sched._viable_tiers(job, job.compatible_tiers) == ["medium_boost", "large_boost"]
        # _pick_launch_tier chooses the least-wasteful viable tier (smallest spike)
        sched._job_compatible_tiers[job.job_id] = list(job.compatible_tiers)
        assert sched._pick_launch_tier(job) == "medium_boost"


# ---------------------------------------------------------------------------
# BSIM-107: joint provisioner
# ---------------------------------------------------------------------------

class TestJointProvisioner:
    def test_outliers_consolidate_onto_capable_tier(self, tier_cfg, tier_registry):
        # 10 small-burst jobs (compatible with all tiers) + 2 medium-burst jobs
        # (medium/large only). Minimum node count must be achieved (no dedicated
        # extra node for the outliers), and a medium-capable node must exist.
        jobs = [_make_job(8, 8, ["small_boost", "medium_boost", "large_boost"],
                          jid=f"s{i}") for i in range(10)]
        jobs += [_make_job(8, 60, ["medium_boost", "large_boost"], jid=f"m{i}")
                 for i in range(2)]
        counts, _ = _provision_once(tier_cfg, tier_registry, jobs)
        # 12 jobs, 2 vcpu each, 64 vcpu/node → 32 jobs/node by vcpu; RAM:
        # medium eff=192 GB / 8 = 24 jobs/node. So all 12 fit on ONE node.
        assert sum(counts.values()) == 1
        # the single node must be medium or large (can host the burst-60 outliers)
        assert set(counts) <= {"medium_boost", "large_boost"}
        assert "small_boost" not in counts

    def test_pure_small_picks_least_wasteful_tier(self, tier_cfg, tier_registry):
        # Many small-burst jobs, no outliers → smallest-spike tier (most schedulable).
        jobs = [_make_job(8, 8, ["small_boost", "medium_boost", "large_boost"], jid=f"s{i}")
                for i in range(40)]
        counts, _ = _provision_once(tier_cfg, tier_registry, jobs)
        assert set(counts) == {"small_boost"}

    def test_large_only_jobs_use_large_tier(self, tier_cfg, tier_registry):
        # burst 100 → only large_boost (128) can host it.
        jobs = [_make_job(8, 100, ["small_boost", "medium_boost", "large_boost"], jid=f"l{i}")
                for i in range(2)]
        counts, _ = _provision_once(tier_cfg, tier_registry, jobs)
        assert set(counts) == {"large_boost"}

    def test_k8splus_joint_provisioner_consolidates(self, tier_cfg, tier_registry):
        # K8S+ shares the joint-provisioner logic; outliers must consolidate too.
        cfg = tier_cfg.model_copy(update={"scheduler_type": SchedulerType.K8SPLUS})
        jobs = [_make_job(8, 8, ["small_boost", "medium_boost", "large_boost"], jid=f"s{i}")
                for i in range(10)]
        jobs += [_make_job(8, 60, ["medium_boost", "large_boost"], jid=f"m{i}")
                 for i in range(2)]
        counts, _ = _provision_once(cfg, tier_registry, jobs, sched_cls=K8SPlusScheduler)
        assert sum(counts.values()) == 1
        assert "small_boost" not in counts


# ---------------------------------------------------------------------------
# BSIM-108: admission
# ---------------------------------------------------------------------------

class TestAdmission:
    def _arrive(self, cfg, registry, job):
        env = simpy.Environment()
        metrics = MetricsCollector()
        sched = K8SScheduler(cfg=cfg, registry=registry, metrics=metrics,
                             centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._env = env  # bypass _setup
        sched.on_job_arrival(env, job, arrival_time=0.0)
        return sched, metrics

    def _events(self, metrics, etype):
        return metrics.events_of_type(etype)

    def test_incompatible_declared_tier_warns_and_drops(self, tier_cfg, tier_registry):
        # burst 60: small_boost cannot host → warned and dropped; routed to medium.
        job = _make_job(8, 60, ["small_boost", "medium_boost"], jid="j1")
        sched, metrics = self._arrive(tier_cfg, tier_registry, job)
        warns = self._events(metrics, EventType.TIER_COMPATIBILITY_WARN)
        assert len(warns) == 1
        assert warns[0].data["incompatible_tiers"] == ["small_boost"]
        assert sched._job_compatible_tiers["j1"] == ["medium_boost"]

    def test_no_viable_tier_rejected(self, tier_cfg, tier_registry):
        # burst 200 exceeds every tier's spike → ADMISSION_REJECTED, not enqueued.
        job = _make_job(8, 200, ["small_boost", "medium_boost", "large_boost"], jid="j2")
        sched, metrics = self._arrive(tier_cfg, tier_registry, job)
        rejects = self._events(metrics, EventType.ADMISSION_REJECTED)
        assert len(rejects) == 1
        assert "j2" not in sched._job_compatible_tiers
        assert len(sched._queue._heap) == 0

    def test_compatible_job_no_warn_no_reject(self, tier_cfg, tier_registry):
        job = _make_job(8, 8, ["small_boost", "medium_boost"], jid="j3")
        sched, metrics = self._arrive(tier_cfg, tier_registry, job)
        assert not self._events(metrics, EventType.TIER_COMPATIBILITY_WARN)
        assert not self._events(metrics, EventType.ADMISSION_REJECTED)
        assert sched._job_compatible_tiers["j3"] == ["small_boost", "medium_boost"]


# ---------------------------------------------------------------------------
# BSIM-113: zero-headroom (no-boost) tier
# ---------------------------------------------------------------------------

class TestZeroHeadroomTier:
    def _flat_cfg(self, tier_cfg):
        # One no-boost tier: spike_max_gb=0 → whole node (minus OS) schedulable.
        flat = TierProfile(name="flat", spike_max_gb=0.0, spawn_instance_class="r7i.16xlarge")
        return tier_cfg.model_copy(update={"tiers": [flat]})

    def test_zero_spike_validates(self):
        t = TierProfile(name="flat", spike_max_gb=0.0, spawn_instance_class="r7i.16xlarge")
        assert t.spike_max_gb == 0.0

    def test_zero_spike_full_node_schedulable(self):
        inst = InstanceTypeConfig(name="r7i.16xlarge", family=InstanceFamily.MEMORY,
                                  ram_gb=256, vcpu=64, hourly_price_usd=4.0)
        cap = compute_k8s_capacity(inst, spike_max_gb=0.0, os_overhead_gb=2.0)
        assert cap.spike_headroom_gb == 0.0
        assert cap.effective_schedulable_gb == 254.0  # 256 - 2 - 0

    def test_flat_job_viable_for_zero_tier(self, tier_cfg, tier_registry):
        cfg = self._flat_cfg(tier_cfg)
        sched = K8SScheduler(cfg=cfg, registry=tier_registry, metrics=MetricsCollector(),
                             centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        # flat job: preprocess_peak == soft_limit → burst 0 → compatible with spike=0
        flat = _make_job(soft_gb=32, burst_gb=0, tiers=["flat"], jid="f1")
        assert sched._viable_tiers(flat, ["flat"]) == ["flat"]

    def test_flat_jobs_provision_on_zero_tier(self, tier_cfg, tier_registry):
        cfg = self._flat_cfg(tier_cfg)
        jobs = [_make_job(32, 0, ["flat"], jid=f"f{i}") for i in range(4)]
        counts, _ = _provision_once(cfg, tier_registry, jobs)
        assert set(counts) == {"flat"}

    def test_burst_job_rejected_when_only_zero_tier(self, tier_cfg, tier_registry):
        # A job with positive burst whose only compatible tier is no-boost cannot run.
        cfg = self._flat_cfg(tier_cfg)
        env = simpy.Environment()
        metrics = MetricsCollector()
        sched = K8SScheduler(cfg=cfg, registry=tier_registry, metrics=metrics,
                             centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._env = env
        job = _make_job(soft_gb=32, burst_gb=40, tiers=["flat"], jid="b1")
        sched.on_job_arrival(env, job, arrival_time=0.0)
        assert metrics.events_of_type(EventType.ADMISSION_REJECTED)
        assert "b1" not in sched._job_compatible_tiers
