"""BSIM-125: count-based download-slot admission throttle for K8S+.

Separate from the GB-scaled NodeBurstPool (Phase 2 / preprocess), download_slots
gates how many jobs can simultaneously be in the download-through-bootstrap
pipeline on a node. A job acquires a slot before downloading and holds it
through Phase 2 (bootstrap), releasing it together with the burst-pool
reservation once bootstrap completes -- the slot represents "has unconsumed
downloaded data resident," not just "is transferring."
"""
import simpy
import pytest

from batch_sim.core.schemas import (
    K8SPlusConfig, InstanceTypeConfig, InstanceFamily, InstanceRegistryConfig,
)
from batch_sim.generator.job_spec import JobSpec, PhaseProfile
from batch_sim.metrics.collector import MetricsCollector, EventType
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler, run_job_process_plus


def _make_job(soft_gb, burst_gb, download_s, preprocess_s, jid):
    prof = PhaseProfile(
        download_duration_s=download_s,
        preprocess_duration_s=preprocess_s,
        preprocess_peak_ram_gb=soft_gb + burst_gb,
        workhorse_hard_limit_gb=soft_gb,
        workhorse_declared_vcpu=2,
    )
    return JobSpec(job_id=jid, centroid_id="c", profile=prof, soft_cpu=2, hard_cpu=2)


def _registry():
    return InstanceRegistry(InstanceRegistryConfig(instance_types=[
        InstanceTypeConfig(name="r7i.2xlarge", family=InstanceFamily.MEMORY,
                           ram_gb=64, vcpu=8, hourly_price_usd=0.5)]))


class TestDownloadSlotsConfigDefault:
    def test_default_is_none_unconstrained(self):
        assert K8SPlusConfig().download_slots is None

    def test_accepts_positive_int(self):
        assert K8SPlusConfig(download_slots=3).download_slots == 3

    def test_rejects_non_positive(self):
        with pytest.raises(ValueError):
            K8SPlusConfig(download_slots=0)


class TestDownloadPoolWiring:
    def test_no_pool_constructed_when_unset(self):
        reg = _registry()
        cfg = K8SPlusConfig(os_overhead_gb=0.0, warmup_delay_seconds=1.0)
        env = simpy.Environment()
        sched = K8SPlusScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector(),
                                 centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 1.0)
        assert len(sched._download_pools) == 0

    def test_pool_constructed_with_configured_capacity(self):
        reg = _registry()
        cfg = K8SPlusConfig(os_overhead_gb=0.0, warmup_delay_seconds=1.0, download_slots=2)
        env = simpy.Environment()
        sched = K8SPlusScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector(),
                                 centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 1.0)
        assert len(sched._download_pools) == 1
        pool = next(iter(sched._download_pools.values()))
        assert isinstance(pool, simpy.Resource)
        assert pool.capacity == 2


class TestDownloadSlotSerialization:
    """Two jobs land on the same node at t=0; download_slots=1 forces them to
    serialise through the full download+bootstrap pipeline, not just download."""

    def _run_two_jobs(self, download_slots):
        reg = _registry()
        cfg = K8SPlusConfig(os_overhead_gb=0.0, warmup_delay_seconds=0.001,
                            download_slots=download_slots)
        env = simpy.Environment()
        metrics = MetricsCollector()
        sched = K8SPlusScheduler(cfg=cfg, registry=reg, metrics=metrics,
                                 centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._env = env
        launch = sched._launch_node(env, reg.get_by_name("r7i.2xlarge"))
        env.process(launch)
        env.run(until=0.01)
        node_id = next(iter(sched._nodes))
        node = sched._nodes[node_id]
        bp = sched._burst_pools[node_id]
        dp = sched._download_pools.get(node_id)

        download_start = {}
        preprocess_start = {}

        def _track(job_id):
            for e in metrics.log:
                if e.event_type == EventType.PHASE_TRANSITION and e.data.get('job_id') == job_id:
                    if e.data['phase'] == 'download':
                        download_start[job_id] = e.sim_time
                    elif e.data['phase'] == 'preprocess':
                        preprocess_start[job_id] = e.sim_time

        jobs = [
            _make_job(soft_gb=4.0, burst_gb=0.0, download_s=10.0, preprocess_s=5.0, jid="a"),
            _make_job(soft_gb=4.0, burst_gb=0.0, download_s=10.0, preprocess_s=5.0, jid="b"),
        ]
        for j in jobs:
            env.process(run_job_process_plus(
                env=env, job=j, node=node, metrics=metrics, burst_pool=bp,
                download_pool=dp, arrival_time=0.0, queue_entry_time=0.0, scheduler=sched,
            ))
        env.run()
        for j in jobs:
            _track(j.job_id)
        return download_start, preprocess_start

    def test_unconstrained_both_download_immediately(self):
        download_start, _ = self._run_two_jobs(download_slots=None)
        assert download_start["a"] == download_start["b"]

    def test_single_slot_serialises_second_jobs_download_past_first_jobs_bootstrap(self):
        # With one slot, job b cannot start downloading until job a releases --
        # which only happens after job a's *preprocess* (bootstrap) completes,
        # not merely after job a's download completes (download=10, preprocess=5).
        download_start, preprocess_start = self._run_two_jobs(download_slots=1)
        # job a: download [t0,t0+10), preprocess [t0+10,t0+15) -> releases at t0+15
        assert download_start["b"] - download_start["a"] == pytest.approx(15.0)

    def test_two_slots_both_download_immediately(self):
        download_start, _ = self._run_two_jobs(download_slots=2)
        assert download_start["a"] == download_start["b"]


class TestDownloadWaitStats:
    def test_zero_when_no_pool(self):
        reg = _registry()
        cfg = K8SPlusConfig(os_overhead_gb=0.0, warmup_delay_seconds=0.001)
        sched = K8SPlusScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector(),
                                 centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        stats = sched.download_wait_stats()
        assert stats == {'count': 0, 'mean': 0, 'max': 0, 'nonzero_count': 0}

    def test_reports_wait_when_serialised(self):
        reg = _registry()
        cfg = K8SPlusConfig(os_overhead_gb=0.0, warmup_delay_seconds=0.001, download_slots=1)
        env = simpy.Environment()
        metrics = MetricsCollector()
        sched = K8SPlusScheduler(cfg=cfg, registry=reg, metrics=metrics,
                                 centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=0.01)
        node_id = next(iter(sched._nodes))
        node = sched._nodes[node_id]
        bp = sched._burst_pools[node_id]
        dp = sched._download_pools[node_id]
        jobs = [
            _make_job(soft_gb=4.0, burst_gb=0.0, download_s=10.0, preprocess_s=5.0, jid="a"),
            _make_job(soft_gb=4.0, burst_gb=0.0, download_s=10.0, preprocess_s=5.0, jid="b"),
        ]
        for j in jobs:
            env.process(run_job_process_plus(
                env=env, job=j, node=node, metrics=metrics, burst_pool=bp,
                download_pool=dp, arrival_time=0.0, queue_entry_time=0.0, scheduler=sched,
            ))
        env.run()
        stats = sched.download_wait_stats()
        assert stats['count'] == 2
        assert stats['nonzero_count'] == 1
        assert stats['max'] == pytest.approx(15.0)
