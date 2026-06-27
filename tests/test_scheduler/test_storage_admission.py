"""BSIM-127/129: storage capacity as a real admission constraint.

Before this, _batch_fits / _k8s_fits checked RAM and vCPU only; STORAGE_EXHAUSTED
was a pure observability event with zero placement consequence. has_room_for()
on both pool types lets the schedulers' admission checks reject a candidate node
whose storage pool has no room for the incoming job's workspace, mirroring real
K8s' NodeVolumeLimits predicate -- and, per BSIM-126's finding, applied
symmetrically to Batch since its static per-instance ceiling is a *harder*
constraint than K8S+'s (no Karpenter-style next-node-type mitigation).
"""
from __future__ import annotations
import simpy
import pytest

from batch_sim.core.schemas import (
    StoragePoolConfig, InstanceTypeConfig, InstanceFamily, InstanceRegistryConfig,
    BatchConfig, K8SConfig, K8SPlusConfig,
)
from batch_sim.scheduler.storage_pool import NodeStoragePool, GenerationalStoragePool
from batch_sim.metrics.collector import MetricsCollector
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.scheduler.batch_scheduler import BatchScheduler
from batch_sim.scheduler.k8s_scheduler import K8SScheduler
from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler


def _instance(max_ebs_volumes: int = 28) -> InstanceTypeConfig:
    return InstanceTypeConfig(name="r7i.4xlarge", family=InstanceFamily.MEMORY,
                               ram_gb=128, vcpu=16, hourly_price_usd=1.0,
                               max_ebs_volumes=max_ebs_volumes)


def _config(**kwargs) -> StoragePoolConfig:
    defaults = dict(initial_volume_count=2, volume_size_gb=1000.0,
                    expansion_trigger_pct=0.80, ebs_price_per_gb_hour=0.0001096)
    defaults.update(kwargs)
    return StoragePoolConfig(**defaults)


# ---------------------------------------------------------------------------
# has_room_for: NodeStoragePool (Batch)
# ---------------------------------------------------------------------------

class TestNodeStoragePoolHasRoomFor:
    def test_room_when_well_below_trigger(self):
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        assert pool.has_room_for(100.0) is True

    def test_room_exactly_at_trigger_boundary(self):
        # 2 TB pool, 80% trigger = 1600 GB exactly
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        assert pool.has_room_for(1600.0) is True   # <=, not <

    def test_no_room_when_max_volumes_reached_and_over_trigger(self):
        # max_ebs_volumes=2 -> max physical capacity = 2000 GB; trigger = 1600 GB
        pool = NodeStoragePool("n1", _config(), _instance(max_ebs_volumes=2), open_time=0.0)
        assert pool.has_room_for(1601.0) is False

    def test_room_restored_after_job_exit(self):
        m = MetricsCollector()
        pool = NodeStoragePool("n1", _config(), _instance(max_ebs_volumes=2), open_time=0.0)
        pool.job_start(0.0, "j1", 1900.0, m)
        assert pool.has_room_for(100.0) is False
        pool.job_exit(1.0, "j1", 1900.0, m)
        assert pool.has_room_for(100.0) is True


# ---------------------------------------------------------------------------
# has_room_for: GenerationalStoragePool (K8S/K8S+)
# ---------------------------------------------------------------------------

class TestGenerationalStoragePoolHasRoomFor:
    def test_room_within_current_generation(self):
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        assert pool.has_room_for(500.0) is True

    def test_room_via_opening_a_new_generation(self):
        # max_ebs_volumes=28, initial_volume_count=2 -> up to 14 open gens allowed
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 1400.0, MetricsCollector())  # gen0 at 70%
        # 1400 + 300 = 1700 > 1600 trigger -> would need a new gen; plenty of room for one
        assert pool.has_room_for(300.0) is True

    def test_no_room_when_at_max_open_generations(self):
        # max_ebs_volumes=2, initial_volume_count=2 -> only 1 generation can ever be open
        inst = _instance(max_ebs_volumes=2)
        pool = GenerationalStoragePool("n1", _config(), inst, open_time=0.0)
        m = MetricsCollector()
        pool.job_start(0.0, "j1", 1400.0, m)  # gen0 at 70%, no second gen possible
        # 1400 + 300 = 1700 > 1600 trigger -> would need gen1, but ceiling is 1 open gen
        assert pool.has_room_for(300.0) is False

    def test_room_returns_once_a_generation_releases(self):
        inst = _instance(max_ebs_volumes=2)
        pool = GenerationalStoragePool("n1", _config(), inst, open_time=0.0)
        m = MetricsCollector()
        pool.job_start(0.0, "j1", 1400.0, m)
        assert pool.has_room_for(300.0) is False
        pool.job_exit(1.0, "j1", 1400.0, m)  # gen0 releases (only job, now empty)
        assert pool.has_room_for(300.0) is True


# ---------------------------------------------------------------------------
# Integration: scheduler admission checks respect storage room
# ---------------------------------------------------------------------------

def _registry(max_ebs_volumes=2) -> InstanceRegistry:
    return InstanceRegistry(InstanceRegistryConfig(instance_types=[
        InstanceTypeConfig(name="r7i.2xlarge", family=InstanceFamily.MEMORY,
                           ram_gb=64, vcpu=8, hourly_price_usd=0.5,
                           max_ebs_volumes=max_ebs_volumes)]))


class TestBatchFitsRespectsStorage:
    def test_storage_exhausted_node_is_skipped(self):
        reg = _registry(max_ebs_volumes=2)
        cfg = BatchConfig(warmup_delay_seconds=1.0,
                          storage=_config(initial_volume_count=1, volume_size_gb=1000.0))
        env = simpy.Environment()
        sched = BatchScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector())
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        pool = sched._storage_pools[node_id]
        # Fill the pool to exhaustion (max_ebs_volumes=2 -> 2000 GB ceiling; 80% trigger = 1600)
        pool.job_start(0.0, "filler", 1900.0, sched.metrics)
        assert pool.has_room_for(100.0) is False
        # A job needing 100 GB of workspace must not be placed on this node
        best = sched._best_fit_node(ram_gb=1.0, vcpu=1.0, workspace_gb_needed=100.0, job_id="j2")
        assert best is None

    def test_room_when_under_storage_ceiling(self):
        reg = _registry(max_ebs_volumes=2)
        cfg = BatchConfig(warmup_delay_seconds=1.0,
                          storage=_config(initial_volume_count=1, volume_size_gb=1000.0))
        env = simpy.Environment()
        sched = BatchScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector())
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        node = sched._nodes[node_id]
        best = sched._best_fit_node(ram_gb=1.0, vcpu=1.0, workspace_gb_needed=100.0, job_id="j1")
        assert best is node


class TestK8SFitsRespectsStorage:
    def test_storage_exhausted_node_is_skipped(self):
        reg = _registry(max_ebs_volumes=2)
        cfg = K8SConfig(os_overhead_gb=0.0, warmup_delay_seconds=1.0,
                        storage=_config(initial_volume_count=1, volume_size_gb=1000.0))
        env = simpy.Environment()
        sched = K8SScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector(),
                             centroid_peak_rams=[], rng=None)
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        node = sched._nodes[node_id]
        sched._capacity_cache[(node.instance.name, "")] = sched._k8s_capacity(node.instance)
        pool = sched._storage_pools[node_id]
        pool.job_start(0.0, "filler", 1900.0, sched.metrics)
        best = sched._best_fit_node(soft_gb=1.0, vcpu=1.0, workspace_gb_needed=100.0,
                                     job_id="j2", job_tiers=[])
        assert best is None


class TestK8SPlusFitsRespectsStorage:
    def test_storage_exhausted_node_is_skipped(self):
        reg = _registry(max_ebs_volumes=2)
        cfg = K8SPlusConfig(os_overhead_gb=0.0, warmup_delay_seconds=1.0,
                            storage=_config(initial_volume_count=1, volume_size_gb=1000.0))
        env = simpy.Environment()
        sched = K8SPlusScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector(),
                                 centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge")))
        env.run(until=cfg.warmup_delay_seconds + 0.01)
        node_id = next(iter(sched._nodes))
        node = sched._nodes[node_id]
        sched._capacity_cache[(node.instance.name, "")] = sched._k8s_capacity(node.instance)
        pool = sched._storage_pools[node_id]
        pool.job_start(0.0, "filler", 1900.0, sched.metrics)
        best = sched._best_fit_node(soft_gb=1.0, vcpu=1.0, workspace_gb_needed=100.0,
                                     job_id="j2", job_tiers=[])
        assert best is None
