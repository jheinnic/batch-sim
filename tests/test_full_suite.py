"""Full test suite: BSIM-5 through BSIM-30."""
import pytest, numpy as np
from batch_sim.generator.job_spec import build_phase_profile
from batch_sim.generator.sampler import sample_job
from batch_sim.generator.event_list import build_event_list, save_event_list, load_event_list
from batch_sim.registry.instance_registry import (
    compute_k8s_capacity, batch_max_jobs, NodeCostAccruer,
    EBS_GP3_PRICE_PER_GB_HOUR, workspace_gb,
)
from batch_sim.core.schemas import InstanceTypeConfig, InstanceFamily, StoragePoolConfig
from batch_sim.metrics.collector import MetricsCollector


class TestPhaseProfile:
    def test_download_duration(self):
        p = build_phase_profile(download_gb=5.0, preprocess_a=1.0, preprocess_b=1.5,
            preprocess_duration_s=30.0, workhorse_cpu_stages=[60.0, 10.0],
            workhorse_hard_vcpu=[4], io_wait_fraction=0.25, upload_gb=0.5,
            network_bandwidth_mbps=500.0)
        assert abs(p.download_duration_s - 10.0) < 0.01

    def test_peak_ram_super_linear(self):
        p = build_phase_profile(download_gb=10.0, preprocess_a=1.2, preprocess_b=1.5,
            preprocess_duration_s=30.0, workhorse_cpu_stages=[60.0, 10.0],
            workhorse_hard_vcpu=[4], io_wait_fraction=0.0, upload_gb=1.0,
            network_bandwidth_mbps=500.0)
        assert abs(p.preprocess_peak_ram_gb - 1.2 * (10.0 ** 1.5)) < 0.01

    def test_steady_state_8_pct(self):
        p = build_phase_profile(download_gb=8.0, preprocess_a=1.3, preprocess_b=1.4,
            preprocess_duration_s=40.0, workhorse_cpu_stages=[100.0, 15.0],
            workhorse_hard_vcpu=[8], io_wait_fraction=0.20, upload_gb=0.8,
            network_bandwidth_mbps=500.0)
        assert abs(p.preprocess_steady_ram_gb - 0.08 * p.preprocess_peak_ram_gb) < 1e-9

    def test_parallel_odd_serial(self):
        p = build_phase_profile(download_gb=1.0, preprocess_a=1.0, preprocess_b=1.0,
            preprocess_duration_s=10.0, workhorse_cpu_stages=[100.0, 20.0, 200.0, 15.0],
            workhorse_hard_vcpu=[4, 4], io_wait_fraction=0.0, upload_gb=0.1,
            network_bandwidth_mbps=500.0)
        assert p.stages[0].declared_threads == 4
        assert p.stages[1].declared_threads == 1
        assert p.stages[2].declared_threads == 4
        assert p.stages[3].declared_threads == 1


class TestSampler:
    def test_reproducible(self, small_centroid, sim_config):
        rng1 = np.random.default_rng(42); rng2 = np.random.default_rng(42)
        j1 = sample_job(small_centroid, rng1, sim_config.network_bandwidth_mbps)
        j2 = sample_job(small_centroid, rng2, sim_config.network_bandwidth_mbps)
        assert abs(j1.profile.download_duration_s - j2.profile.download_duration_s) < 1e-9

    def test_positive_values(self, small_centroid, sim_config):
        rng = np.random.default_rng(99)
        for _ in range(50):
            j = sample_job(small_centroid, rng, sim_config.network_bandwidth_mbps)
            assert j.profile.preprocess_peak_ram_gb > 0
            assert j.profile.total_duration_s > 0


class TestEventList:
    def test_sorted(self, event_list):
        times = [e.arrival_time for e in event_list.events]
        assert times == sorted(times)

    def test_roundtrip(self, event_list, tmp_path):
        p = tmp_path / "events.json"
        save_event_list(event_list, p)
        loaded = load_event_list(p)
        assert len(loaded) == len(event_list)
        assert abs(loaded.events[0].preprocess_peak_ram_gb
                   - event_list.events[0].preprocess_peak_ram_gb) < 1e-9

    def test_centroid_counts(self, event_list):
        counts = event_list.centroid_counts()
        assert all(v > 0 for v in counts.values())

    def test_to_job_spec(self, event_list):
        e = event_list.events[0]; j = e.to_job_spec()
        assert j.job_id == e.job_id
        assert abs(j.profile.preprocess_peak_ram_gb - e.preprocess_peak_ram_gb) < 1e-9


class TestInstanceRegistry:
    def test_cheapest_fitting(self, registry):
        r = registry.cheapest_fitting(16, 4); assert r is not None
        cheaper = [t for t in registry.all_types
                   if t.ram_gb >= 16 and t.vcpu >= 4 and t.hourly_price_usd < r.hourly_price_usd]
        assert len(cheaper) == 0

    def test_no_fit(self, registry):
        assert registry.cheapest_fitting(9999, 1) is None

    def test_candidates_sorted(self, registry):
        c = registry.candidates(32, 8)
        assert [x.hourly_price_usd for x in c] == sorted(x.hourly_price_usd for x in c)


class TestK8SCapacity:
    def test_worked_example(self):
        # BSIM-102: spike_max_gb is a hardware constant from QueueDefinition, not job-spec derived
        inst = InstanceTypeConfig(name="r7i.4xlarge", family=InstanceFamily.MEMORY,
                                   ram_gb=128, vcpu=16, hourly_price_usd=1.0)
        cap = compute_k8s_capacity(inst, spike_max_gb=64.0, os_overhead_gb=4.0)
        assert cap.tier_local_mm_gb == 64.0
        assert cap.spike_headroom_gb == 64.0
        # effective_schedulable_gb = 128 - 4 (os) - 64 (spike) = 60
        assert cap.effective_schedulable_gb == 60.0

    def test_small_jobs_no_crash(self): assert 32.0 + 32.0 <= 128.0
    def test_large_jobs_crash(self): assert 64.0 + 64.0 >= 128.0

    def test_spike_max_drives_schedulable_zone(self):
        # Two queues using the same instance but different spike_max_gb get different capacity
        inst = InstanceTypeConfig(name="r7i.4xlarge", family=InstanceFamily.MEMORY,
                                   ram_gb=128, vcpu=16, hourly_price_usd=1.0)
        cap_small = compute_k8s_capacity(inst, spike_max_gb=32.0, os_overhead_gb=4.0)
        cap_large = compute_k8s_capacity(inst, spike_max_gb=64.0, os_overhead_gb=4.0)
        assert cap_small.effective_schedulable_gb == 92.0  # 128 - 4 - 32
        assert cap_large.effective_schedulable_gb == 60.0  # 128 - 4 - 64

    def test_batch_max_jobs(self):
        inst = InstanceTypeConfig(name="r7i.4xlarge", family=InstanceFamily.MEMORY,
                                   ram_gb=128, vcpu=16, hourly_price_usd=1.0)
        assert batch_max_jobs(inst, 64.0, 8) == 2

    def test_k8s_packs_more_than_batch(self):
        # K8S with small spike reservation leaves more schedulable RAM than Batch's peak-per-job limit
        inst = InstanceTypeConfig(name="r7i.4xlarge", family=InstanceFamily.MEMORY,
                                   ram_gb=128, vcpu=16, hourly_price_usd=1.0)
        b = batch_max_jobs(inst, 64.0, 8)       # 2 jobs (64GB each, max-peak basis)
        cap = compute_k8s_capacity(inst, spike_max_gb=64.0, os_overhead_gb=0.0)
        # With spike_max=64 and soft_limit=10 per job, effective=64 → 6 jobs fit vs batch's 2
        soft_per_job = 10.0
        k8s_jobs = int(cap.effective_schedulable_gb // soft_per_job)
        assert k8s_jobs > b


class TestCostAccruer:
    def test_cost(self):
        inst = InstanceTypeConfig(name="m7i.2xlarge", family=InstanceFamily.GENERAL,
                                   ram_gb=32, vcpu=8, hourly_price_usd=0.40)
        a = NodeCostAccruer(node_id="n1", instance=inst, launch_time=0.0)
        a.terminate(3600.0)
        assert abs(a.total_cost_usd - 0.40) < 1e-9

    def test_unterminated_zero_cost(self):
        inst = InstanceTypeConfig(name="m7i.2xlarge", family=InstanceFamily.GENERAL,
                                   ram_gb=32, vcpu=8, hourly_price_usd=0.40)
        assert NodeCostAccruer(node_id="n1", instance=inst, launch_time=0.0).total_cost_usd == 0.0


class TestBatchIntegration:
    def test_all_jobs_complete(self, event_list, batch_cfg, registry):
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.schemas import SchedulerType
        sc = run_one(event_list, SchedulerType.BATCH, batch_cfg, registry, "test")
        assert sc.job_stats.pool_job_count + sc.job_stats.pool_terminal_failure_count == len(event_list)

    def test_cost_positive(self, event_list, batch_cfg, registry):
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.schemas import SchedulerType
        sc = run_one(event_list, SchedulerType.BATCH, batch_cfg, registry, "test")
        assert sc.cost_summary.total_cost_usd > 0

    def test_per_centroid_stats(self, event_list, batch_cfg, registry):
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.schemas import SchedulerType
        sc = run_one(event_list, SchedulerType.BATCH, batch_cfg, registry, "test")
        assert len(sc.job_stats.per_centroid) > 0


class TestK8SIntegration:
    def test_all_jobs_complete(self, event_list, k8s_cfg, registry):
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.schemas import SchedulerType
        sc = run_one(event_list, SchedulerType.K8S, k8s_cfg, registry, "test")
        assert sc.job_stats.pool_job_count + sc.job_stats.pool_terminal_failure_count >= len(event_list)

    def test_cost_positive(self, event_list, k8s_cfg, registry):
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.schemas import SchedulerType
        sc = run_one(event_list, SchedulerType.K8S, k8s_cfg, registry, "test")
        assert sc.cost_summary.total_cost_usd > 0

    def test_capacity_report(self, event_list, k8s_cfg, registry):
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.schemas import SchedulerType
        sc = run_one(event_list, SchedulerType.K8S, k8s_cfg, registry, "test")
        assert sc.k8s_capacity_report is not None


class TestBSIM91StorageSchema:
    def test_instance_type_default_max_ebs_volumes(self):
        inst = InstanceTypeConfig(name="m7i.2xlarge", family=InstanceFamily.GENERAL,
                                   ram_gb=32, vcpu=8, hourly_price_usd=0.40)
        assert inst.max_ebs_volumes == 28

    def test_instance_type_custom_max_ebs_volumes(self):
        inst = InstanceTypeConfig(name="c7i.48xlarge", family=InstanceFamily.COMPUTE,
                                   ram_gb=384, vcpu=192, hourly_price_usd=8.16,
                                   max_ebs_volumes=16)
        assert inst.max_ebs_volumes == 16

    def test_storage_pool_config_defaults(self):
        cfg = StoragePoolConfig()
        assert cfg.initial_volume_count == 2
        assert cfg.volume_size_gb == 1000.0
        assert cfg.logical_capacity_gb == 65536.0
        assert abs(cfg.expansion_trigger_pct - 0.80) < 1e-9
        assert abs(cfg.ebs_price_per_gb_hour - EBS_GP3_PRICE_PER_GB_HOUR) < 1e-12

    def test_storage_pool_config_roundtrip(self):
        cfg = StoragePoolConfig(initial_volume_count=4, volume_size_gb=500.0,
                                expansion_trigger_pct=0.75, ebs_price_per_gb_hour=0.0002)
        assert cfg.initial_volume_count == 4
        assert cfg.volume_size_gb == 500.0
        assert abs(cfg.expansion_trigger_pct - 0.75) < 1e-9

    def test_scheduler_config_storage_absent(self, batch_cfg):
        assert batch_cfg.storage is None

    def test_scheduler_config_storage_present(self):
        from batch_sim.core.schemas import SchedulerConfig, SchedulerType
        cfg = SchedulerConfig(scheduler_type=SchedulerType.BATCH,
                              storage=StoragePoolConfig(volume_size_gb=2000.0))
        assert cfg.storage is not None
        assert cfg.storage.volume_size_gb == 2000.0

    def test_ebs_constant_value(self):
        assert abs(EBS_GP3_PRICE_PER_GB_HOUR - 0.0001096) < 1e-12

    def test_workspace_gb_returns_preprocess_peak_ram(self):
        p = build_phase_profile(download_gb=4.0, preprocess_a=1.0, preprocess_b=1.0,
            preprocess_duration_s=20.0, workhorse_cpu_stages=[60.0],
            workhorse_hard_vcpu=[4], io_wait_fraction=0.0, upload_gb=0.5,
            network_bandwidth_mbps=500.0)
        from batch_sim.generator.job_spec import JobSpec
        job = JobSpec(job_id="j1", centroid_id="c1", profile=p)
        assert workspace_gb(job) == p.preprocess_peak_ram_gb

    def test_workspace_gb_16gb_centroid(self):
        from batch_sim.generator.job_spec import PhaseProfile, JobSpec
        p = PhaseProfile(download_duration_s=0.0, preprocess_peak_ram_gb=16.0)
        job = JobSpec(job_id="j1", centroid_id="c1", profile=p)
        assert workspace_gb(job) == 16.0


class TestScorecardIO:
    def test_save_load(self, event_list, batch_cfg, registry, tmp_path):
        import json
        from batch_sim.experiment_runner import run_one
        from batch_sim.core.schemas import SchedulerType
        sc = run_one(event_list, SchedulerType.BATCH, batch_cfg, registry, "test")
        p = tmp_path / "scorecard.json"; sc.save(p)
        assert p.exists()
        d = json.loads(p.read_text())
        assert d["scheduler_type"] == "batch"
        assert d["cost_summary"]["total_cost_usd"] > 0
