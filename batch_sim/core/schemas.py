"""BSIM-2: Pydantic configuration schemas for all simulation inputs."""
from __future__ import annotations
from enum import Enum
from typing import Annotated, Literal
from pydantic import BaseModel, Field, field_validator, model_validator

PositiveFloat = Annotated[float, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]
PositiveInt = Annotated[int, Field(gt=0)]
Fraction = Annotated[float, Field(ge=0.0, le=1.0)]


class TimeWindowOverride(BaseModel):
    """BSIM-77: Per-centroid time-window override for arrival rate and bin weights."""
    start_time_s: NonNegativeFloat
    end_time_s: PositiveFloat
    burst_rate: PositiveFloat | None = Field(
        default=None,
        description="Override arrival rate (bursts/hour) during this window. "
                    "Absent = inherit centroid baseline.",
    )
    centroid_bin_weights: list[PositiveFloat] | None = Field(
        default=None,
        description="Override bin weights during this window. "
                    "Must have the same length as the centroid's centroid_bin_weights. "
                    "Absent = inherit centroid baseline weights.",
    )

    @model_validator(mode="after")
    def _validate_window(self) -> "TimeWindowOverride":
        if self.end_time_s <= self.start_time_s:
            raise ValueError(
                f"end_time_s ({self.end_time_s}) must be > "
                f"start_time_s ({self.start_time_s})"
            )
        return self


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
    io_wait_fraction: Fraction
    workhorse_soft_vcpu: list[int] | None = Field(
        default=None,
        description=(
            "Optional per-parallel-stage minimum vCPU guarantee. "
            "Scheduler reserves max(workhorse_soft_vcpu) per job. "
            "When absent, max(workhorse_hard_vcpu) is used."
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
        gt=0.0, le=1.0,
        description="Lower clamp on the Pareto multiplier applied to all "
                     "sampled parameters. Default 0.25 (quarter of nominal)."
    )
    pareto_multiplier_max: float = Field(
        default=4.0,
        ge=1.0,
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

    # BSIM-74: inter-arrival draw strategy
    arrival_spacing: Literal["poisson", "approximate"] = Field(
        default="poisson",
        description=(
            "poisson (default): memoryless rng.expovariate — exact Poisson, "
            "preserves burst clustering. "
            "approximate: average of 5000 draws — reduced variance, predictable "
            "arrival counts. Use for test configs (tiny, teeny)."
        )
    )

    # BSIM-75: discrete size-bin model (optional; absent = Pareto path)
    centroid_bin_weights: list[PositiveFloat] | None = Field(
        default=None,
        description="Unnormalized bin weights. Length determines number of bins. "
                    "When present, bin sampling replaces the Pareto multiplier path.",
    )
    bin_download_gb: list[PositiveFloat] | None = Field(
        default=None, description="Download size in GB per bin."
    )
    bin_upload_gb: list[PositiveFloat] | None = Field(
        default=None, description="Upload size in GB per bin."
    )
    bin_preprocess_duration_s: list[PositiveFloat] | None = Field(
        default=None, description="Preprocess wall-clock duration in seconds per bin."
    )
    bin_preloader_hard_limit_gb: list[PositiveFloat] | None = Field(
        default=None, description="Declared preprocess RAM hard limit per bin (K8S reservation)."
    )
    bin_preloader_actual_gb: list[list[float]] | None = Field(
        default=None,
        description="Per-bin [lo, hi] range for actual preprocess peak RAM. "
                    "Uniform draw within this range. Each entry must be [lo, hi] with 0 < lo < hi.",
    )
    bin_steady_state_hard_limit_gb: list[PositiveFloat] | None = Field(
        default=None, description="Declared workhorse RAM hard limit per bin."
    )
    bin_steady_state_actual_gb: list[list[float]] | None = Field(
        default=None,
        description="Per-bin [lo, hi] range for actual workhorse RAM. "
                    "Each entry must be [lo, hi] with 0 < lo < hi.",
    )
    bin_workhorse_scale: list[PositiveFloat] | None = Field(
        default=None,
        description="Scale factor applied to workhorse_cpu_stages per bin. "
                    "1.0 = nominal duration; 2.0 = twice as long.",
    )

    # BSIM-77: time-window overrides (optional)
    time_windows: list[TimeWindowOverride] | None = Field(
        default=None,
        description="Per-centroid time-window overrides for burst_rate and bin weights. "
                    "Windows must be non-overlapping. Gaps between windows inherit the "
                    "centroid baseline.",
    )

    @model_validator(mode="after")
    def _validate_stage_arrays(self) -> "CentroidConfig":
        stages = self.workhorse_cpu_stages
        if len(stages) % 2 != 0:
            raise ValueError(f"workhorse_cpu_stages must have even length; got {len(stages)}")
        expected = len(stages) // 2

        # workhorse_hard_vcpu is the sole thread count declaration.
        if self.workhorse_hard_vcpu is None:
            raise ValueError(
                "workhorse_hard_vcpu is required (one entry per parallel stage). "
                "It declares the thread count and hard CPU ceiling for each stage."
            )
        threads = list(self.workhorse_hard_vcpu)
        if len(threads) != expected:
            raise ValueError(
                f"workhorse_hard_vcpu must have "
                f"{expected} entries (one per parallel stage); got {len(threads)}"
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

        # BSIM-75: bin model validation
        if self.centroid_bin_weights is not None:
            n_bins = len(self.centroid_bin_weights)
            if n_bins == 0:
                raise ValueError("centroid_bin_weights must not be empty")
            bin_arrays = {
                "bin_download_gb": self.bin_download_gb,
                "bin_upload_gb": self.bin_upload_gb,
                "bin_preprocess_duration_s": self.bin_preprocess_duration_s,
                "bin_preloader_hard_limit_gb": self.bin_preloader_hard_limit_gb,
                "bin_preloader_actual_gb": self.bin_preloader_actual_gb,
                "bin_steady_state_hard_limit_gb": self.bin_steady_state_hard_limit_gb,
                "bin_steady_state_actual_gb": self.bin_steady_state_actual_gb,
                "bin_workhorse_scale": self.bin_workhorse_scale,
            }
            for name, arr in bin_arrays.items():
                if arr is not None and len(arr) != n_bins:
                    raise ValueError(
                        f"{name} has {len(arr)} entries but "
                        f"centroid_bin_weights has {n_bins}"
                    )
            for pair_name in ("bin_preloader_actual_gb", "bin_steady_state_actual_gb"):
                arr = getattr(self, pair_name)
                if arr is not None:
                    for i, pair in enumerate(arr):
                        if len(pair) != 2:
                            raise ValueError(
                                f"{pair_name}[{i}] must be [lo, hi]; got {pair}"
                            )
                        lo, hi = pair
                        if lo <= 0 or lo >= hi:
                            raise ValueError(
                                f"{pair_name}[{i}]=[{lo}, {hi}] invalid: "
                                f"require 0 < lo < hi"
                            )

        # BSIM-77: time-window validation
        if self.time_windows:
            n_bins = len(self.centroid_bin_weights) if self.centroid_bin_weights else None
            sorted_windows = sorted(self.time_windows, key=lambda w: w.start_time_s)
            for i, w in enumerate(sorted_windows):
                if i > 0 and w.start_time_s < sorted_windows[i - 1].end_time_s:
                    raise ValueError(
                        f"time_windows overlap: window ending at "
                        f"{sorted_windows[i-1].end_time_s}s and window starting at "
                        f"{w.start_time_s}s"
                    )
                if w.centroid_bin_weights is not None:
                    if n_bins is None:
                        raise ValueError(
                            "time_window centroid_bin_weights override requires "
                            "centroid_bin_weights to be set on the centroid"
                        )
                    if len(w.centroid_bin_weights) != n_bins:
                        raise ValueError(
                            f"time_window [{w.start_time_s}, {w.end_time_s}) "
                            f"centroid_bin_weights has {len(w.centroid_bin_weights)} "
                            f"entries but centroid has {n_bins}"
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
    K8SPLUS = "k8splus"


# ---------------------------------------------------------------------------
# BSIM-83: Time-based scheduling policy schema
# ---------------------------------------------------------------------------

class DrainRule(BaseModel):
    """
    A single drain condition: node enters DRAINING when idle_vcpu has
    continuously exceeded this threshold for duration_s seconds.
    """
    idle_vcpu: NonNegativeFloat
    duration_s: PositiveFloat


class QueuePolicy(BaseModel):
    """
    A memory-band queue: handles jobs whose preprocess_peak_ram_gb falls
    in (exclusive_min_gb, inclusive_max_gb].
    """
    exclusive_min_gb: NonNegativeFloat = Field(default=0.0)
    inclusive_max_gb: PositiveFloat
    spawn_instance_class: str
    spawn_rate_per_min: PositiveFloat = Field(
        default=1.0,
        description="Maximum node launches per minute for this queue.",
    )
    drain_rules: list[DrainRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_queue(self) -> "QueuePolicy":
        if self.inclusive_max_gb <= self.exclusive_min_gb:
            raise ValueError(
                f"inclusive_max_gb ({self.inclusive_max_gb}) must be > "
                f"exclusive_min_gb ({self.exclusive_min_gb})"
            )
        if len(self.drain_rules) >= 2:
            sorted_rules = sorted(self.drain_rules, key=lambda r: r.idle_vcpu)
            for i in range(len(sorted_rules) - 1):
                lo, hi = sorted_rules[i], sorted_rules[i + 1]
                if hi.duration_s >= lo.duration_s:
                    raise ValueError(
                        f"drain_rules not monotone: "
                        f"idle_vcpu={lo.idle_vcpu} → duration_s={lo.duration_s}, "
                        f"idle_vcpu={hi.idle_vcpu} → duration_s={hi.duration_s}; "
                        f"higher idle_vcpu must pair with strictly shorter duration_s"
                    )
        return self


class TimeWindowPolicy(BaseModel):
    """
    A time window within a 24-hour day [start_time_s, end_time_s) that
    defines one or more memory-band queues.
    """
    start_time_s: NonNegativeFloat
    end_time_s: PositiveFloat
    queues: list[QueuePolicy] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _validate_window(self) -> "TimeWindowPolicy":
        if self.end_time_s <= self.start_time_s:
            raise ValueError(
                f"end_time_s ({self.end_time_s}) must be > "
                f"start_time_s ({self.start_time_s})"
            )
        queues_sorted = sorted(self.queues, key=lambda q: q.exclusive_min_gb)
        if queues_sorted[0].exclusive_min_gb != 0.0:
            raise ValueError(
                f"First queue band in window [{self.start_time_s}, {self.end_time_s}) "
                f"must start at exclusive_min_gb=0; "
                f"got {queues_sorted[0].exclusive_min_gb}"
            )
        for i in range(len(queues_sorted) - 1):
            lo, hi = queues_sorted[i], queues_sorted[i + 1]
            if lo.inclusive_max_gb != hi.exclusive_min_gb:
                raise ValueError(
                    f"Queue band gap/overlap in window [{self.start_time_s}, {self.end_time_s}): "
                    f"band ({lo.exclusive_min_gb}, {lo.inclusive_max_gb}] "
                    f"is not contiguous with "
                    f"band ({hi.exclusive_min_gb}, {hi.inclusive_max_gb}]"
                )
        return self


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
    time_window_policy: list[TimeWindowPolicy] | None = Field(
        default=None,
        description=(
            "BSIM-83: Optional time-based scheduling policy. "
            "When present, partitions the 24-hour day into windows that each "
            "define memory-band queues with explicit instance classes and drain rules. "
            "When absent, the scheduler uses its default instance-selection behavior."
        ),
    )

    @model_validator(mode="after")
    def _validate_time_window_policy(self) -> "SchedulerConfig":
        if not self.time_window_policy:
            return self
        windows = sorted(self.time_window_policy, key=lambda w: w.start_time_s)
        if windows[0].start_time_s != 0.0:
            raise ValueError(
                f"time_window_policy must start at 0s; "
                f"first window starts at {windows[0].start_time_s}s"
            )
        if windows[-1].end_time_s != 86400.0:
            raise ValueError(
                f"time_window_policy must end at 86400s (24 h); "
                f"last window ends at {windows[-1].end_time_s}s"
            )
        for i in range(len(windows) - 1):
            lo, hi = windows[i], windows[i + 1]
            if lo.end_time_s != hi.start_time_s:
                raise ValueError(
                    f"time_window_policy gap/overlap: "
                    f"window ending at {lo.end_time_s}s is not contiguous with "
                    f"window starting at {hi.start_time_s}s"
                )
        return self


class ExperimentConfig(BaseModel):
    event_list_path: str
    instance_registry_path: str = "configs/instance_registry.yaml"
    output_dir: str
    panic_threshold_values: list[PositiveFloat] = Field(..., min_length=2)
    base_scheduler_config: SchedulerConfig
    schedulers: list[SchedulerType] = [SchedulerType.BATCH, SchedulerType.K8S]
