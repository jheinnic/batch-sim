"""BSIM-114: preflight tier-compatibility validation."""
import warnings

import pytest

from batch_sim.core.config_loader import load_simulation_config, load_scheduler_config
from batch_sim.core.schemas import (
    BatchConfig, CentroidConfig, InstanceFamily, InstanceRegistryConfig,
    InstanceTypeConfig, K8SConfig, SimulationConfig, TierProfile, TimeWindowOverride,
)
from batch_sim.core.tier_preflight import (
    TierPreflightError, validate_config_pair, validate_tier_physical_limits,
)
from batch_sim.registry.instance_registry import InstanceRegistry


@pytest.fixture
def tier_registry():
    return InstanceRegistry(InstanceRegistryConfig(instance_types=[
        InstanceTypeConfig(name="r7i.4xlarge", family=InstanceFamily.MEMORY,
                            ram_gb=128, vcpu=16, hourly_price_usd=1.008),
    ]))


def _centroid(**overrides):
    kw = dict(
        id="c", label="C", arrival_rate_per_hour=10.0, pareto_alpha=2.5,
        download_gb=2.0, preprocess_memory_exponent_a=1.2,
        preprocess_memory_exponent_b=1.4, preprocess_duration_seconds=20.0,
        workhorse_cpu_stages=[60.0, 10.0], workhorse_hard_vcpu=[4],
        io_wait_fraction=0.3, upload_gb=0.2,
        centroid_bin_weights=[1.0, 1.0],
        bin_preloader_hard_limit_gb=[40.0, 80.0],
        bin_steady_state_hard_limit_gb=[20.0, 20.0],
    )
    kw.update(overrides)
    return CentroidConfig(**kw)


def _sim_config(centroid):
    return SimulationConfig(horizon_seconds=100.0, centroids=[centroid])


def _k8s_cfg(tiers):
    return K8SConfig(
        panic_threshold_seconds=300.0, sla_target_seconds=600.0,
        warmup_delay_seconds=5.0, idle_timeout_seconds=30.0,
        max_retries=3, replay_delay_seconds=2.0, os_overhead_gb=2.0,
        tiers=tiers)


class TestReferenceIntegrity:
    def test_undeclared_tier_in_centroid_compatible_tiers(self, tier_registry):
        centroid = _centroid(compatible_tiers=["known;ghost", "known"])
        cfg = _k8s_cfg([TierProfile(name="known", spike_max_gb=10.0,
                                     spawn_instance_class="r7i.4xlarge")])
        with pytest.raises(TierPreflightError, match="ghost"):
            validate_config_pair(_sim_config(centroid), cfg, tier_registry)

    def test_undeclared_tier_in_time_window_override(self, tier_registry):
        centroid = _centroid(
            compatible_tiers=["known", "known"],
            time_windows=[TimeWindowOverride(
                start_time_s=0.0, end_time_s=50.0,
                compatible_tiers=["known;phantom", "known"])])
        cfg = _k8s_cfg([TierProfile(name="known", spike_max_gb=10.0,
                                     spawn_instance_class="r7i.4xlarge")])
        with pytest.raises(TierPreflightError, match="phantom"):
            validate_config_pair(_sim_config(centroid), cfg, tier_registry)

    def test_clean_pair_does_not_raise(self, tier_registry):
        centroid = _centroid(compatible_tiers=["known", "known"])
        cfg = _k8s_cfg([TierProfile(name="known", spike_max_gb=70.0,
                                     spawn_instance_class="r7i.4xlarge")])
        validate_config_pair(_sim_config(centroid), cfg, tier_registry)


class TestPhysicalValidity:
    def test_rejects_tier_with_no_schedulable_zone(self, tier_registry):
        # ram_gb=128, os_overhead_gb=2.0 -> schedulable zone is 126 GB
        bad_tier = TierProfile(name="oversized", spike_max_gb=126.0,
                                spawn_instance_class="r7i.4xlarge")
        cfg = _k8s_cfg([bad_tier])
        errors = validate_tier_physical_limits(cfg, tier_registry)
        assert len(errors) == 1
        assert "oversized" in errors[0]

    def test_accepts_tier_with_positive_schedulable_zone(self, tier_registry):
        cfg = _k8s_cfg([TierProfile(name="fine", spike_max_gb=125.0,
                                     spawn_instance_class="r7i.4xlarge")])
        assert validate_tier_physical_limits(cfg, tier_registry) == []

    def test_validate_config_pair_surfaces_physical_violation(self, tier_registry):
        centroid = _centroid(compatible_tiers=["oversized", "oversized"])
        cfg = _k8s_cfg([TierProfile(name="oversized", spike_max_gb=126.0,
                                     spawn_instance_class="r7i.4xlarge")])
        with pytest.raises(TierPreflightError, match="oversized"):
            validate_config_pair(_sim_config(centroid), cfg, tier_registry)


class TestBurstReachability:
    def test_warns_when_no_tier_can_host_bin_burst(self, tier_registry):
        # bin 1: min_spike = 80 - 20 = 60 GB; declared tier maxes out at 10 GB.
        centroid = _centroid(compatible_tiers=["small", "small"])
        cfg = _k8s_cfg([TierProfile(name="small", spike_max_gb=10.0,
                                     spawn_instance_class="r7i.4xlarge")])
        with pytest.warns(UserWarning, match="no listed tier can host"):
            validate_config_pair(_sim_config(centroid), cfg, tier_registry)

    def test_silent_when_a_tier_can_host_every_bin_burst(self, tier_registry):
        # bin 0: min_spike=20, bin 1: min_spike=60 -> "big" (70) covers both.
        centroid = _centroid(compatible_tiers=["big", "big"])
        cfg = _k8s_cfg([TierProfile(name="big", spike_max_gb=70.0,
                                     spawn_instance_class="r7i.4xlarge")])
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            validate_config_pair(_sim_config(centroid), cfg, tier_registry)


class TestNoOp:
    def test_noop_for_batch_config(self, tier_registry):
        centroid = _centroid(compatible_tiers=["ghost", "ghost"])
        cfg = BatchConfig(
            panic_threshold_seconds=300.0, sla_target_seconds=600.0,
            warmup_delay_seconds=5.0, idle_timeout_seconds=30.0,
            max_retries=3, replay_delay_seconds=2.0)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            validate_config_pair(_sim_config(centroid), cfg, tier_registry)

    def test_noop_for_tierless_k8s_config(self, tier_registry):
        centroid = _centroid(compatible_tiers=["ghost", "ghost"])
        cfg = _k8s_cfg([])
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            validate_config_pair(_sim_config(centroid), cfg, tier_registry)


class TestRegressionFixture:
    """BSIM-114 acceptance criterion: the corrected jch_centroids_v01.yaml ×
    jch_k8splus_scheduler.yaml pair (12/12 bins, 36 tiers) passes cleanly."""

    def test_real_fixture_pair_passes_with_zero_findings(self):
        sim_cfg = load_simulation_config("configs/jch_centroids_v01.yaml")
        sched_cfg = load_scheduler_config("configs/jch_k8splus_scheduler.yaml")
        registry = InstanceRegistry.from_yaml("configs/instance_registry.yaml")
        assert len(sched_cfg.tiers) == 36
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            validate_config_pair(sim_cfg, sched_cfg, registry)
