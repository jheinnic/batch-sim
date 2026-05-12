"""BSIM-2: Pydantic configuration schemas for all simulation inputs."""
from __future__ import annotations
from enum import Enum
from typing import Annotated
from pydantic import BaseModel, Field, model_validator

PositiveFloat = Annotated[float, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]
PositiveInt = Annotated[int, Field(gt=0)]
Fraction = Annotated[float, Field(ge=0.0, le=1.0)]


class CentroidConfig(BaseModel):
    id: str
    label: str
    description: str = ""
    arrival_rate_per_hour: PositiveFloat
    pareto_alpha: PositiveFloat
    download_gb: PositiveFloat
    preprocess_memory_exponent_a: PositiveFloat
    preprocess_memory_exponent_b: PositiveFloat
    preprocess_duration_seconds: PositiveFloat = Field(..., le=120.0)
    workhorse_cpu_stages: list[PositiveFloat] = Field(..., min_length=2)
    workhorse_thread_counts: list[PositiveInt]
    io_wait_fraction: Fraction
    workhorse_io_wait_per_stage: list[Fraction] | None = Field(
        default=None,
        description=(
            "Optional per-parallel-stage I/O wait fractions. "
            "Length must equal len(workhorse_cpu_stages) // 2. "
            "If absent, io_wait_fraction is applied uniformly to all parallel stages."
        )
    )
    upload_gb: PositiveFloat

    @model_validator(mode="after")
    def _validate_stage_arrays(self) -> "CentroidConfig":
        stages  = self.workhorse_cpu_stages
        threads = self.workhorse_thread_counts
        if len(stages) % 2 != 0:
            raise ValueError(f"workhorse_cpu_stages must have even length; got {len(stages)}")
        expected = len(stages) // 2
        if len(threads) != expected:
            raise ValueError(
                f"workhorse_thread_counts must have {expected} entries; got {len(threads)}"
            )
        if self.workhorse_io_wait_per_stage is not None:
            if len(self.workhorse_io_wait_per_stage) != expected:
                raise ValueError(
                    f"workhorse_io_wait_per_stage must have {expected} entries "
                    f"(one per parallel stage); got {len(self.workhorse_io_wait_per_stage)}"
                )
        return self


class SimulationConfig(BaseModel):
    horizon_seconds: PositiveFloat
    random_seed: int = 42
    network_bandwidth_mbps: PositiveFloat = 500.0
    centroids: list[CentroidConfig] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _unique_centroid_ids(self) -> "SimulationConfig":
        ids = [c.id for c in self.centroids]
        if len(ids) != len(set(ids)):
            raise ValueError("Centroid IDs must be unique")
        return self


class InstanceFamily(str, Enum):
    GENERAL = "general"
    MEMORY = "memory"
    COMPUTE = "compute"


class InstanceTypeConfig(BaseModel):
    name: str
    family: InstanceFamily
    ram_gb: PositiveFloat
    vcpu: PositiveInt
    hourly_price_usd: PositiveFloat


class InstanceRegistryConfig(BaseModel):
    instance_types: list[InstanceTypeConfig] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _unique_names(self) -> "InstanceRegistryConfig":
        names = [i.name for i in self.instance_types]
        if len(names) != len(set(names)):
            raise ValueError("Instance type names must be unique")
        return self


class SchedulerType(str, Enum):
    BATCH = "batch"
    K8S = "k8s"


class SchedulerConfig(BaseModel):
    scheduler_type: SchedulerType
    panic_threshold_seconds: PositiveFloat = 300.0
    sla_target_seconds: PositiveFloat = 600.0
    warmup_delay_seconds: PositiveFloat = 90.0
    idle_timeout_seconds: PositiveFloat = 300.0
    idle_check_interval_seconds: PositiveFloat = 30.0
    max_retries: PositiveInt = 3
    replay_delay_seconds: NonNegativeFloat = 10.0
    k8s_os_overhead_gb: NonNegativeFloat = 2.0


class ExperimentConfig(BaseModel):
    event_list_path: str
    instance_registry_path: str = "configs/instance_registry.yaml"
    output_dir: str
    panic_threshold_values: list[PositiveFloat] = Field(..., min_length=2)
    base_scheduler_config: SchedulerConfig
    schedulers: list[SchedulerType] = [SchedulerType.BATCH, SchedulerType.K8S]
