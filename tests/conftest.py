"""BSIM-4: Shared pytest fixtures."""
import pytest
import numpy as np
from batch_sim.core.schemas import (
    CentroidConfig, SimulationConfig, InstanceTypeConfig,
    InstanceRegistryConfig, SchedulerConfig, SchedulerType, InstanceFamily
)
from batch_sim.generator.event_list import build_event_list, EventList
from batch_sim.registry.instance_registry import InstanceRegistry


@pytest.fixture
def small_centroid():
    return CentroidConfig(id="small", label="Small Job",
        arrival_rate_per_hour=10.0, pareto_alpha=2.5,
        download_gb=2.0, preprocess_memory_exponent_a=1.2,
        preprocess_memory_exponent_b=1.4, preprocess_duration_seconds=20.0,
        workhorse_cpu_stages=[60.0, 10.0, 120.0, 8.0],
        workhorse_thread_counts=[4, 4], io_wait_fraction=0.30, upload_gb=0.2)

@pytest.fixture
def large_centroid():
    return CentroidConfig(id="large", label="Large Job",
        arrival_rate_per_hour=4.0, pareto_alpha=2.0,
        download_gb=30.0, preprocess_memory_exponent_a=1.5,
        preprocess_memory_exponent_b=1.6, preprocess_duration_seconds=50.0,
        workhorse_cpu_stages=[300.0, 25.0, 600.0, 20.0],
        workhorse_thread_counts=[8, 8], io_wait_fraction=0.25, upload_gb=3.0)

@pytest.fixture
def sim_config(small_centroid, large_centroid):
    return SimulationConfig(horizon_seconds=1800.0, random_seed=7,
        network_bandwidth_mbps=500.0, centroids=[small_centroid, large_centroid])

@pytest.fixture
def event_list(sim_config): return build_event_list(sim_config)

@pytest.fixture
def instance_types():
    return [
        InstanceTypeConfig(name="m7i.2xlarge", family=InstanceFamily.GENERAL, ram_gb=32,  vcpu=8,  hourly_price_usd=0.4032),
        InstanceTypeConfig(name="m7i.4xlarge", family=InstanceFamily.GENERAL, ram_gb=64,  vcpu=16, hourly_price_usd=0.8064),
        InstanceTypeConfig(name="r7i.4xlarge", family=InstanceFamily.MEMORY,  ram_gb=128, vcpu=16, hourly_price_usd=1.0080),
        InstanceTypeConfig(name="r7i.8xlarge", family=InstanceFamily.MEMORY,  ram_gb=256, vcpu=32, hourly_price_usd=2.0160),
        InstanceTypeConfig(name="c7i.4xlarge", family=InstanceFamily.COMPUTE, ram_gb=32,  vcpu=16, hourly_price_usd=0.7140),
    ]

@pytest.fixture
def registry(instance_types):
    return InstanceRegistry(InstanceRegistryConfig(instance_types=instance_types))

@pytest.fixture
def batch_cfg():
    return SchedulerConfig(scheduler_type=SchedulerType.BATCH,
        panic_threshold_seconds=300.0, sla_target_seconds=600.0,
        warmup_delay_seconds=5.0, idle_timeout_seconds=30.0,
        idle_check_interval_seconds=10.0, max_retries=3, replay_delay_seconds=2.0)

@pytest.fixture
def k8s_cfg():
    return SchedulerConfig(scheduler_type=SchedulerType.K8S,
        panic_threshold_seconds=300.0, sla_target_seconds=600.0,
        warmup_delay_seconds=5.0, idle_timeout_seconds=30.0,
        idle_check_interval_seconds=10.0, max_retries=3, replay_delay_seconds=2.0,
        k8s_os_overhead_gb=1.0)
