"""BSIM-92/93: Unit and integration tests for EBS storage pool models."""
from __future__ import annotations
import pytest
from batch_sim.core.schemas import StoragePoolConfig, InstanceTypeConfig, InstanceFamily
from batch_sim.scheduler.storage_pool import NodeStoragePool, GenerationalStoragePool
from batch_sim.metrics.collector import MetricsCollector, EventType


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
# NodeStoragePool (BSIM-92)
# ---------------------------------------------------------------------------

class TestNodeStoragePool:
    def test_initial_capacity(self):
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        assert pool.pool_capacity_gb == 2000.0
        assert pool.pool_committed_gb == 0.0

    def test_committed_rises_at_job_start(self):
        m = MetricsCollector()
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 200.0, m)
        assert pool.pool_committed_gb == 200.0

    def test_committed_falls_at_job_exit(self):
        m = MetricsCollector()
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 200.0, m)
        pool.job_exit(200.0)
        assert pool.pool_committed_gb == 0.0

    def test_committed_two_sequential_jobs(self):
        m = MetricsCollector()
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 300.0, m)
        pool.job_exit(300.0)
        assert pool.pool_committed_gb == 0.0
        pool.job_start(1.0, "j2", 400.0, m)
        assert pool.pool_committed_gb == 400.0
        pool.job_exit(400.0)
        assert pool.pool_committed_gb == 0.0

    def test_capacity_only_increases(self):
        m = MetricsCollector()
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 1700.0, m)  # triggers expansion
        cap_after = pool.pool_capacity_gb
        pool.job_exit(1700.0)
        assert pool.pool_capacity_gb == cap_after  # commitment fell but capacity stays

    def test_expansion_event_at_trigger(self):
        m = MetricsCollector()
        # 2 TB pool; trigger at 80% = 1600 GB; a 1601 GB job crosses the threshold
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 1601.0, m)
        evts = m.events_of_type(EventType.STORAGE_POOL_EXPANDED)
        assert len(evts) == 1
        assert evts[0].data["old_gb"] == 2000.0
        assert evts[0].data["new_gb"] == 3000.0

    def test_no_expansion_below_trigger(self):
        m = MetricsCollector()
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 1599.0, m)  # 79.95% — below 80% threshold
        assert len(m.events_of_type(EventType.STORAGE_POOL_EXPANDED)) == 0

    def test_exhausted_when_volume_ceiling_reached(self):
        m = MetricsCollector()
        # instance with max_ebs_volumes=2 = 2 TB ceiling; try to commit 1900 GB
        inst = _instance(max_ebs_volumes=2)
        pool = NodeStoragePool("n1", _config(), inst, open_time=0.0)
        pool.job_start(0.0, "j1", 1900.0, m)
        assert len(m.events_of_type(EventType.STORAGE_EXHAUSTED)) == 1
        assert len(m.events_of_type(EventType.STORAGE_POOL_EXPANDED)) == 0

    def test_storage_cost_accrues_on_capacity(self):
        m = MetricsCollector()
        cfg = _config(volume_size_gb=1000.0, ebs_price_per_gb_hour=0.0001096)
        pool = NodeStoragePool("n1", cfg, _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 100.0, m)
        pool.job_exit(100.0)
        pool.close(3600.0)
        # capacity = 2000 GB for 1 hr at 0.0001096/GB/hr
        expected = 2000.0 * 1.0 * 0.0001096
        assert abs(pool.storage_cost_usd - expected) < 1e-9

    def test_cost_zero_until_closed(self):
        m = MetricsCollector()
        pool = NodeStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 100.0, m)
        assert pool.storage_cost_usd == 0.0  # not yet closed

    def test_integration_four_600gb_jobs_single_expansion(self):
        """4 × 600 GB jobs on a 2 TB pool: one expansion to 3 TB."""
        m = MetricsCollector()
        cfg = _config(initial_volume_count=2, volume_size_gb=1000.0, expansion_trigger_pct=0.80)
        pool = NodeStoragePool("n1", cfg, _instance(), open_time=0.0)

        # Place all 4 jobs simultaneously
        for i in range(4):
            pool.job_start(0.0, f"j{i}", 600.0, m)

        # committed = 2400, which > 80% of 2000 = 1600 after first job start triggers expansion
        # After 1st job: 600 < 1600 → no expand
        # After 2nd job: 1200 < 1600 → no expand
        # After 3rd job: 1800 > 1600 → expand to 3000; new trigger = 2400; 1800 < 2400 → stop
        # After 4th job: 2400 == 2400 → no expand (threshold is strictly >)
        expanded = m.events_of_type(EventType.STORAGE_POOL_EXPANDED)
        assert len(expanded) == 1
        assert pool.pool_capacity_gb == 3000.0

        # Complete all jobs
        for i in range(4):
            pool.job_exit(600.0)
        pool.close(7200.0)

        # Cost = 3000 GB × 2 hr (7200s) × 0.0001096
        expected_cost = 3000.0 * 2.0 * 0.0001096
        assert abs(pool.storage_cost_usd - expected_cost) < 1e-9


# ---------------------------------------------------------------------------
# GenerationalStoragePool (BSIM-93)
# ---------------------------------------------------------------------------

class TestGenerationalStoragePool:
    def test_initial_generation_opened(self):
        m = MetricsCollector()
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        assert len(pool._generations) == 1
        assert pool._generations[0].capacity_gb == 2000.0

    def test_jobs_within_trigger_stay_in_gen0(self):
        m = MetricsCollector()
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 500.0, m)
        pool.job_start(0.0, "j2", 500.0, m)
        assert len(pool._generations) == 1
        assert pool._generations[0].committed_gb == 1000.0

    def test_new_gen_opened_when_threshold_would_be_crossed(self):
        m = MetricsCollector()
        # 2 TB pool, 80% trigger = 1600 GB
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 1400.0, m)   # gen0: 1400/2000 = 70% — ok
        pool.job_start(1.0, "j2", 300.0, m)    # 1400+300 = 1700 > 1600 → new gen
        assert len(pool._generations) == 2
        gen_opened = m.events_of_type(EventType.STORAGE_GEN_OPENED)
        # Initial gen open has no metrics event (metrics=None); only the overflow event
        assert len(gen_opened) == 1
        assert gen_opened[0].data["gen_id"] == 1

    def test_job_gen_assignment_stable(self):
        m = MetricsCollector()
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 1400.0, m)
        pool.job_start(1.0, "j2", 300.0, m)  # j2 goes to gen1
        assert pool._job_gen["j1"] == 0
        assert pool._job_gen["j2"] == 1

    def test_gen_released_when_last_job_exits(self):
        m = MetricsCollector()
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 1400.0, m)
        pool.job_start(1.0, "j2", 300.0, m)   # j2 → gen1
        pool.job_exit(100.0, "j1", 1400.0, m)  # gen0 now has 0 jobs
        released = m.events_of_type(EventType.STORAGE_GEN_RELEASED)
        assert len(released) == 1
        assert released[0].data["gen_id"] == 0

    def test_gen_cost_stops_at_release(self):
        m = MetricsCollector()
        cfg = _config(ebs_price_per_gb_hour=0.0001096)
        pool = GenerationalStoragePool("n1", cfg, _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 1400.0, m)
        pool.job_start(1.0, "j2", 300.0, m)   # j2 → gen1, gen1 opens at t=1
        pool.job_exit(3600.0, "j1", 1400.0, m)  # gen0 released at t=3600
        pool.job_exit(7200.0, "j2", 300.0, m)   # gen1 released at t=7200
        pool.close(7200.0)

        # gen0: 2000 GB × 1 hr × rate
        # gen1: 2000 GB × (7200-1)/3600 hr × rate ≈ 2 hr × rate
        gen0_cost = 2000.0 * (3600.0 / 3600.0) * 0.0001096
        gen1_cost = 2000.0 * ((7200.0 - 1.0) / 3600.0) * 0.0001096
        assert abs(pool.storage_cost_usd - (gen0_cost + gen1_cost)) < 0.001

    def test_single_gen_node_matches_batch(self):
        """A node that never overflows behaves identically to NodeStoragePool."""
        m = MetricsCollector()
        cfg = _config()
        inst = _instance()
        gen_pool = GenerationalStoragePool("n1", cfg, inst, open_time=0.0)
        node_pool = NodeStoragePool("n1", cfg, inst, open_time=0.0)
        for pool_obj in (gen_pool, node_pool):
            if isinstance(pool_obj, GenerationalStoragePool):
                pool_obj.job_start(0.0, "j1", 400.0, m)
                pool_obj.job_exit(3600.0, "j1", 400.0, m)
                pool_obj.close(3600.0)
            else:
                pool_obj.job_start(0.0, "j1", 400.0, m)
                pool_obj.job_exit(400.0)
                pool_obj.close(3600.0)
        assert abs(gen_pool.storage_cost_usd - node_pool.storage_cost_usd) < 1e-9

    def test_integration_six_large_jobs_two_generations(self):
        """6 large jobs across 2 generations; gen1 releases before node terminates."""
        m = MetricsCollector()
        cfg = _config(initial_volume_count=2, volume_size_gb=1000.0, expansion_trigger_pct=0.80)
        pool = GenerationalStoragePool("n1", cfg, _instance(), open_time=0.0)

        # gen0 capacity = 2000; trigger = 1600
        # j1..j2 each 700 GB: after j2 committed=1400 → below trigger → stay in gen0
        # j3: 1400+700=2100 > 1600 → new gen1; j3 goes to gen1
        pool.job_start(0.0, "j1", 700.0, m)
        pool.job_start(0.0, "j2", 700.0, m)
        pool.job_start(0.0, "j3", 700.0, m)  # triggers gen1
        # j4..j6 each 700 GB in gen1: after j5 committed=1400 → below trigger
        # j6: 1400+700=2100 > 1600 → new gen2; j6 goes to gen2
        pool.job_start(0.0, "j4", 700.0, m)
        pool.job_start(0.0, "j5", 700.0, m)
        pool.job_start(0.0, "j6", 700.0, m)  # triggers gen2

        assert len(pool._generations) == 3
        assert pool._job_gen["j1"] == 0
        assert pool._job_gen["j3"] == 1
        assert pool._job_gen["j6"] == 2

        # Complete gen0 jobs first (j1, j2)
        pool.job_exit(3600.0, "j1", 700.0, m)
        pool.job_exit(3600.0, "j2", 700.0, m)
        released = m.events_of_type(EventType.STORAGE_GEN_RELEASED)
        assert len(released) == 1
        assert released[0].data["gen_id"] == 0
        # Gen0 released at t=3600; node has NOT terminated yet
        assert pool._generations[0].close_time == 3600.0

        # Complete remaining jobs and terminate node
        for jid in ("j3", "j4", "j5", "j6"):
            pool.job_exit(7200.0, jid, 700.0, m)
        pool.close(7200.0)

        released2 = m.events_of_type(EventType.STORAGE_GEN_RELEASED)
        assert len(released2) == 3

    def test_close_emits_release_for_still_open_generation(self):
        """Node termination while a generation still has active jobs (e.g. the
        node hit its TTL) must still emit STORAGE_GEN_RELEASED -- otherwise
        chart code that aggregates capacity purely from open/release events
        sees that capacity as never released, plateauing instead of dropping
        at node termination even though storage_cost_usd (driven by
        close_time, not the event) was already correct."""
        m = MetricsCollector()
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 500.0, m)  # gen0, job never exits before termination
        assert len(m.events_of_type(EventType.STORAGE_GEN_RELEASED)) == 0

        pool.close(3600.0, m)

        released = m.events_of_type(EventType.STORAGE_GEN_RELEASED)
        assert len(released) == 1
        assert released[0].data["gen_id"] == 0
        assert released[0].data["capacity_gb"] == 2000.0
        assert pool._generations[0].close_time == 3600.0

    def test_close_without_metrics_still_sets_close_time(self):
        """metrics is optional on close() -- cost accrual (driven by close_time)
        must work even when no metrics collector is passed."""
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 500.0, MetricsCollector())
        pool.close(3600.0)   # no metrics arg
        assert pool._generations[0].close_time == 3600.0
        assert pool.storage_cost_usd > 0.0

    def test_close_does_not_double_release_already_closed_generation(self):
        m = MetricsCollector()
        pool = GenerationalStoragePool("n1", _config(), _instance(), open_time=0.0)
        pool.job_start(0.0, "j1", 500.0, m)
        pool.job_exit(1800.0, "j1", 500.0, m)  # gen0 releases naturally
        assert len(m.events_of_type(EventType.STORAGE_GEN_RELEASED)) == 1

        pool.close(3600.0, m)  # gen0 already closed -- must not emit a second release
        assert len(m.events_of_type(EventType.STORAGE_GEN_RELEASED)) == 1
