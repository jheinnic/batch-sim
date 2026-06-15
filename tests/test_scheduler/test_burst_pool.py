"""BSIM-122: GB-aware Phase-2 burst concurrency via NodeBurstPool.

The mainline K8S+ scheduler previously gated Phase 2 with a count-based
NodeSemaphore that degenerated to a per-node mutex (permits = floor(spike/spike)
= 1). NodeBurstPool instead admits multiple sub-spike jobs concurrently as long
as their combined burst fits the tier's fixed spike reservation, and serialises
only when it would overflow — never borrowing bin-packing space.
"""
import simpy
import pytest

from batch_sim.scheduler.burst_pool import NodeBurstPool
from batch_sim.core.schemas import (
    TierProfile, K8SPlusConfig, SchedulerType,
    InstanceTypeConfig, InstanceFamily, InstanceRegistryConfig,
)
from batch_sim.metrics.collector import MetricsCollector
from batch_sim.registry.instance_registry import InstanceRegistry
from batch_sim.scheduler.k8s_plus_scheduler import K8SPlusScheduler


def _acquirer(env, pool, amount, hold, record, name):
    yield from pool.acquire(amount)
    record[name] = env.now          # time this job entered Phase 2
    yield env.timeout(hold)
    pool.release(amount)


class TestNodeBurstPoolConcurrency:
    def test_combined_burst_within_reservation_runs_concurrently(self):
        # 16 GB reservation; +6 and +10 bursts (sum 16) must boot concurrently.
        env = simpy.Environment()
        pool = NodeBurstPool(env, node_physical_ram_gb=32, os_overhead_gb=0, headroom_gb=16)
        rec = {}
        env.process(_acquirer(env, pool, 6, 50, rec, "a"))
        env.process(_acquirer(env, pool, 10, 50, rec, "b"))
        env.run()
        assert rec["a"] == 0 and rec["b"] == 0      # both at t=0 — concurrent

    def test_combined_burst_over_reservation_serialises(self):
        # 8 GB reservation; two +6 bursts (sum 12 > 8) must serialise.
        env = simpy.Environment()
        pool = NodeBurstPool(env, node_physical_ram_gb=32, os_overhead_gb=0, headroom_gb=8)
        rec = {}
        env.process(_acquirer(env, pool, 6, 10, rec, "a"))
        env.process(_acquirer(env, pool, 6, 10, rec, "b"))
        env.run()
        assert rec["a"] == 0          # first in immediately
        assert rec["b"] == 10         # second waits for the first to release

    def test_fixed_headroom_ignores_update_max_peak(self):
        # A reservation-sized pool does not grow with the workload.
        env = simpy.Environment()
        pool = NodeBurstPool(env, node_physical_ram_gb=256, os_overhead_gb=0, headroom_gb=16)
        pool.update_max_peak(200)     # legacy hook — must be a no-op here
        assert pool.headroom_gb == 16

    def test_zero_burst_flat_job_never_blocks(self):
        # Flat job (peak == soft → burst 0) on a no-boost reservation proceeds at once.
        env = simpy.Environment()
        pool = NodeBurstPool(env, node_physical_ram_gb=64, os_overhead_gb=0, headroom_gb=0)
        rec = {}
        env.process(_acquirer(env, pool, 0, 5, rec, "flat1"))
        env.process(_acquirer(env, pool, 0, 5, rec, "flat2"))
        env.run()
        assert rec["flat1"] == 0 and rec["flat2"] == 0


class TestK8SPlusBurstPoolWiring:
    def test_pool_sized_to_tier_spike_max(self):
        reg = InstanceRegistry(InstanceRegistryConfig(instance_types=[
            InstanceTypeConfig(name="r7i.2xlarge", family=InstanceFamily.MEMORY,
                               ram_gb=64, vcpu=8, hourly_price_usd=0.5)]))
        cfg = K8SPlusConfig(os_overhead_gb=0.0,
            warmup_delay_seconds=1.0, panic_threshold_seconds=300.0,
            tiers=[TierProfile(name="t16", spike_max_gb=16.0, spawn_instance_class="r7i.2xlarge")])
        env = simpy.Environment()
        sched = K8SPlusScheduler(cfg=cfg, registry=reg, metrics=MetricsCollector(),
                                 centroid_peak_rams=[], centroid_tier_config={}, rng=None)
        sched._env = env
        env.process(sched._launch_node(env, reg.get_by_name("r7i.2xlarge"), tier_name="t16"))
        env.run(until=cfg.warmup_delay_seconds + 1.0)

        assert len(sched._burst_pools) == 1
        pool = next(iter(sched._burst_pools.values()))
        assert isinstance(pool, NodeBurstPool)
        assert pool.headroom_gb == 16.0      # sized to the tier's spike_max_gb, not a permit count
