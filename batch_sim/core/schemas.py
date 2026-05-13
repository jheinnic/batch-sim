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
    workhorse_thread_counts: list[PositiveInt] | None = Field(
        default=None,
        description=(
            "Declared thread counts per parallel stage. "
            "When workhorse_hard_vcpu is provided, that array serves "
            "the same purpose and this field may be omitted. "
            "If both are absent, validation will fail."
        )
    )
    io_wait_fraction: Fraction
    workhorse_soft_vcpu: list[int] | None = Field(
        default=None,
        description=(
            "Optional per-parallel-stage minimum vCPU guarantee. "
            "Scheduler reserves max(workhorse_soft_vcpu) per job. "
            "When absent, max(workhorse_thread_counts) is used (current behaviour)."
        )
    )
    workhorse_hard_vcpu: list[int] | None = Field(
        default=None,
        description=(
            "Optional per-parallel-stage CPU burst ceiling. "
            "Equals thread count for that stage — the point beyond which "
            "adding vCPU yields no throughput gain. "
            "Scheduler allows job to burst to max(workhorse_hard_vcpu) "
            "when surplus cycles are available. "
            "When absent, hard == soft (no burst, Batch behaviour)."
        )
    )
    burst_size_min: int = Field(
        default=1, ge=1,
        description="Minimum jobs per burst arrival event. "
                     "Default 1 = existing single-job Poisson behaviour."
    )
    burst_size_max: int = Field(
        default=1, ge=1,
        description="Maximum jobs per burst arrival event (inclusive). "
                     "arrival_rate_per_hour governs burst events per hour, "
                     "not individual jobs. Set min=3, max=18 for real workload."
    )
    pareto_multiplier_min: float = Field(
        default=0.25,
        gt=0.0, lt=1.0,
        description="Lower clamp on the Pareto multiplier applied to all "
                     "sampled parameters. Default 0.25 (quarter of nominal)."
    )
    pareto_multiplier_max: float = Field(
        default=4.0,
        gt=1.0,
        description="Upper clamp on the Pareto multiplier applied to all "
                     "sampled parameters. Default 4.0 (four times nominal). "
                     "Reduce to suppress heavy-tail outliers; increase to "
                     "allow larger spikes in stress-test workloads."
    )
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
        stages = self.workhorse_cpu_stages
        if len(stages) % 2 != 0:
            raise ValueError(f"workhorse_cpu_stages must have even length; got {len(stages)}")
        expected = len(stages) // 2

        # workhorse_thread_counts is optional when workhorse_hard_vcpu is provided
        # In that case, hard_vcpu serves as the thread count declaration
        if self.workhorse_thread_counts is None:
            if self.workhorse_hard_vcpu is None:
                raise ValueError(
                    "Either workhorse_thread_counts or workhorse_hard_vcpu "
                    "must be provided (thread count declaration is required)"
                )
            # Derive thread counts from hard_vcpu array
            object.__setattr__(self, "workhorse_thread_counts",
                               list(self.workhorse_hard_vcpu))
        threads = self.workhorse_thread_counts
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
        if self.burst_size_min > self.burst_size_max:
            raise ValueError(
                f"burst_size_min ({self.burst_size_min}) must be "
                f"<= burst_size_max ({self.burst_size_max})"
            )
        for arr_name, arr in [("workhorse_soft_vcpu", self.workhorse_soft_vcpu),
                               ("workhorse_hard_vcpu", self.workhorse_hard_vcpu)]:
            if arr is not None and len(arr) != expected:
                raise ValueError(
                    f"{arr_name} must have {expected} entries "
                    f"(one per parallel stage); got {len(arr)}"
                )
        if (self.workhorse_soft_vcpu is not None and
                self.workhorse_hard_vcpu is not None):
            for i, (s, h) in enumerate(
                    zip(self.workhorse_soft_vcpu, self.workhorse_hard_vcpu)):
                if s > h:
                    raise ValueError(
                        f"workhorse_soft_vcpu[{i}]={s} > "
                        f"workhorse_hard_vcpu[{i}]={h}"
                    )
        return self


class SimulationConfig(BaseModel):
    horizon_seconds: PositiveFloat
    random_seed: int = 42
    network_bandwidth_mbps: PositiveFloat = 500.0
    cooloff_seconds: float = Field(
        default=0.0, ge=0.0,
        description=(
            "Extra simulation time after horizon_seconds during which "
            "no new jobs arrive but running jobs are allowed to finish. "
            "Prevents horizon truncation of jobs that start near the end "
            "of the arrival window. Total sim duration = "
            "horizon_seconds + cooloff_seconds."
        )
    )
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
