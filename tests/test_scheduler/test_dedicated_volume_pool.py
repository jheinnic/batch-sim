"""BSIM-128: per-job dedicated ephemeral storage volumes, selectable alternative
to each scheduler's default pool model via storage.model: dedicated."""
from __future__ import annotations
import simpy
import pytest

from batch_sim.core.schemas import (
    StoragePoolConfig, StorageModel, InstanceTypeConfig, InstanceFamily,
    InstanceRegistryConfig, BatchConfig, K8SConfig,
)
from batch_sim.scheduler.storage_pool import (
    DedicatedVolumePool, GenerationalStoragePool, NodeStoragePool, make_storage_pool,
)
from batch_sim.metrics.collector import MetricsCollector
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.scheduler.batch_scheduler import BatchScheduler
from batch_sim.scheduler.k8s_scheduler import K8SScheduler


def _instance(max_ebs_volumes: int = 4) -> InstanceTypeConfig:
    return InstanceTypeConfig(name="r7i.4xlarge", family=InstanceFamily.MEMORY,
                               ram_gb=128, vcpu=16, hourly_price_usd=1.0,
                               max_ebs_volumes=max_ebs_volumes)


def _config(**kwargs) -> StoragePoolConfig:
    defaults = dict(initial_volume_count=2, volume_size_gb=1000.0,
                    expansion_trigger_pct=0.80, ebs_price_per_gb_hour=0.0001096)
    defaults.update(kwargs)
    return StoragePoolConfig(**defaults)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TestStorageModelSchema:
    def test_default_is_pool(self):
        assert _config().model == StorageModel.POOL

    def test_accepts_dedicated(self):
        assert _config(model="dedicated").model == StorageModel.DEDICATED

    def test_rejects_unknown_model(self):
        with pytest.raises(ValueError):
            _config(model="something_else")


# ---------------------------------------------------------------------------
# DedicatedVolumePool unit behavior
# ---------------------------------------------------------------------------

class TestDedicatedVolumePool:
    def test_room_bounded_by_max_ebs_volumes_not_size(self):
        # max_ebs_volumes=2 -> exactly 2 concurrent jobs regardless of size
        pool = DedicatedVolumePool("n1", _config(), _instance(max_ebs_volumes=2), open_time=0.0)
        m = MetricsCollector()
        pool.job_start(0.0, "j1", 5000.0, m)   # huge job -- size is irrelevant to room
        assert pool.has_room_for(1.0) is True
        pool.job_start(0.0, "j2", 1.0, m)      # tiny job -- still just "a slot"
        assert pool.has_room_for(1.0) is False  # both slots taken

    def test_room_restored_after_job_exit(self):
        pool = DedicatedVolumePool("n1", _config(), _instance(max_ebs_volumes=1), open_time=0.0)
        m = MetricsCollector()
        pool.job_start(0.0, "j1", 500.0, m)
        assert pool.has_room_for(1.0) is False
        pool.job_exit(10.0, "j1", 500.0, m)
        assert pool.has_room_for(1.0) is True

    def test_capacity_always_equals_committed(self):
        """No slack to strand by construction -- each volume is exactly its job's size."""
        pool = DedicatedVolumePool("n1", _config(), _instance(), open_time=0.0)
        m = MetricsCollector()
        pool.job_start(0.0, "j1", 300.0, m)
        pool.job_start(0.0, "j2", 700.0, m)
        assert pool.pool_capacity_gb == 1000.0
        assert pool.pool_committed_gb == pool.pool_capacity_gb

    def test_cost_accrues_per_job_on_exit(self):
        cfg = _config(ebs_price_per_gb_hour=0.0001096)
        pool = DedicatedVolumePool("n1", cfg, _instance(), open_time=0.0)
        m = MetricsCollector()
        pool.job_start(0.0, "j1", 100.0, m)
        pool.job_exit(3600.0, "j1", 100.0, m)   # 1 hour
        expected = 100.0 * 1.0 * 0.0001096
        assert abs(pool.storage_cost_usd - expected) < 1e-9

    def test_cost_for_two_jobs_different_durations(self):
        cfg = _config(ebs_price_per_gb_hour=0.0001096)
        pool = DedicatedVolumePool("n1", cfg, _instance(), open_time=0.0)
        m = MetricsCollector()
        pool.job_start(0.0, "j1", 100.0, m)
        pool.job_start(0.0, "j2", 200.0, m)
        pool.job_exit(3600.0, "j1", 100.0, m)     # j1: 100 GB x 1h
        pool.job_exit(7200.0, "j2", 200.0, m)      # j2: 200 GB x 2h
        expected = (100.0 * 1.0 + 200.0 * 2.0) * 0.0001096
        assert abs(pool.storage_cost_usd - expected) < 1e-9

    def test_close_accrues_cost_for_still_active_jobs(self):
        cfg = _config(ebs_price_per_gb_hour=0.0001096)
        pool = DedicatedVolumePool("n1", cfg, _instance(), open_time=0.0)
        m = MetricsCollector()
        pool.job_start(0.0, "j1", 100.0, m)   # never exits -- node terminates first
        pool.close(3600.0, m)
        expected = 100.0 * 1.0 * 0.0001096
        assert abs(pool.storage_cost_usd - expected) < 1e-9
        assert pool.pool_capacity_gb == 0.0   # cleared on close

    def test_close_without_metrics_still_accrues_cost(self):
        pool = DedicatedVolumePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 100.0, MetricsCollector())
        pool.close(3600.0)   # no metrics arg
        assert pool.storage_cost_usd > 0.0

    def test_double_close_is_idempotent(self):
        pool = DedicatedVolumePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 100.0, MetricsCollector())
        pool.close(3600.0)
        cost_after_first_close = pool.storage_cost_usd
        pool.close(7200.0)   # must not double-accrue
        assert pool.storage_cost_usd == cost_after_first_close


# ---------------------------------------------------------------------------
# make_storage_pool factory
# ---------------------------------------------------------------------------

class TestMakeStoragePoolFactory:
    def test_default_model_uses_default_cls(self):
        pool = make_storage_pool("n1", _config(), _instance(), 0.0, default_cls=NodeStoragePool)
        assert isinstance(pool, NodeStoragePool)

    def test_dedicated_model_overrides_default_cls(self):
        pool = make_storage_pool("n1", _config(model="dedicated"), _instance(), 0.0,
                                  default_cls=NodeStoragePool)
        assert isinstance(pool, DedicatedVolumePool)

    def test_dedicated_model_overrides_generational_default_too(self):
        pool = make_storage_pool("n1", _config(model="dedicated"), _instance(), 0.0,
                                  default_cls=GenerationalStoragePool)
        assert isinstance(pool, DedicatedVolumePool)


# ---------------------------------------------------------------------------
# Integration: schedulers actually construct DedicatedVolumePool when configured
# ---------------------------------------------------------------------------

def _registry(max_ebs_volumes=2) -> InstanceRegistry:
    return InstanceRegistry(InstanceRegistryConfig(instance_types=[
        InstanceTypeConfig(name="r7i.2xlarge", family=InstanceFamily.MEMORY,
                           ram_gb=64, vcpu=8, hourly_price_usd=0.5,
                           max_ebs_volumes=max_ebs_volumes)]))


class TestSchedulerWiring:
    def test_batch_uses_dedicated_pool_when_configured(self):
        reg = _registry()
        cfg = BatchConfig(warmup_delay_seconds=1.0, storage=_config(model="dedicated"))
        env = simpy.Environment()
        sched = BatchScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector())
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        assert isinstance(sched._storage_pools[node_id], DedicatedVolumePool)

    def test_batch_uses_node_storage_pool_by_default(self):
        reg = _registry()
        cfg = BatchConfig(warmup_delay_seconds=1.0, storage=_config())
        env = simpy.Environment()
        sched = BatchScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector())
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        assert isinstance(sched._storage_pools[node_id], NodeStoragePool)

    def test_k8s_uses_dedicated_pool_when_configured(self):
        reg = _registry()
        cfg = K8SConfig(os_overhead_gb=0.0, warmup_delay_seconds=1.0,
                        storage=_config(model="dedicated"))
        env = simpy.Environment()
        sched = K8SScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector(),
                             centroid_peak_rams=[], rng=None)
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        assert isinstance(sched._storage_pools[node_id], DedicatedVolumePool)

    def test_k8s_uses_generational_pool_by_default(self):
        reg = _registry()
        cfg = K8SConfig(os_overhead_gb=0.0, warmup_delay_seconds=1.0, storage=_config())
        env = simpy.Environment()
        sched = K8SScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector(),
                             centroid_peak_rams=[], rng=None)
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        assert isinstance(sched._storage_pools[node_id], GenerationalStoragePool)

    def test_dedicated_pool_admission_respects_max_ebs_volumes(self):
        """End-to-end: a third job cannot be placed once max_ebs_volumes=2 slots
        are both occupied, regardless of how small its workspace is."""
        reg = _registry(max_ebs_volumes=2)
        cfg = BatchConfig(warmup_delay_seconds=1.0, storage=_config(model="dedicated"))
        env = simpy.Environment()
        sched = BatchScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector())
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        pool = sched._storage_pools[node_id]
        pool.job_start(0.0, "j1", 1.0, sched.metrics)
        pool.job_start(0.0, "j2", 1.0, sched.metrics)
        best = sched._best_fit_node(ram_gb=0.1, vcpu=0.1, workspace_gb_needed=0.1, job_id="j3")
        assert best is None
