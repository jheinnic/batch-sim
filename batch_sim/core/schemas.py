"""BSIM-2: Pydantic configuration schemas for all simulation inputs."""
from __future__ import annotations
import warnings
from enum import Enum
from typing import Annotated, Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PositiveFloat = Annotated[float, Field(gt=0)]
NonNegativeFloat = Annotated[float, Field(ge=0)]
PositiveInt = Annotated[int, Field(gt=0)]
Fraction = Annotated[float, Field(ge=0.0, le=1.0)]

# BSIM-104: delimiter separating tier names within a single compatible_tiers string.
TIER_SET_DELIMITER = ";"


def parse_tier_set(s: str) -> list[str]:
    """BSIM-104: Split a semicolon-delimited compatibility string into tier names.

    A single tier name (no delimiter) yields a one-element list. Whitespace
    around each name is trimmed; empty segments are dropped.
    """
    return [t.strip() for t in s.split(TIER_SET_DELIMITER) if t.strip()]


class TimeWindowOverride(BaseModel):
    """BSIM-77 / BSIM-100: Per-centroid time-window override for arrival rate, bin weights,
    and queue routing."""
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
    compatible_tiers: str | list[str] | None = Field(
        default=None,
        description=(
            "BSIM-104: Override tier compatibility for this centroid during this window. "
            "A single string sets the compatibility set for all bins; semicolons "
            "separate multiple tier names within the string (e.g. 'small;medium'). "
            "A list sets it per-bin: element[bin_idx], each itself a (possibly "
            "semicolon-delimited) compatibility set. List length must equal "
            "centroid_bin_weights length. Absent = inherit centroid default."
        ),
    )
    queue_name: str | list[str] | None = Field(
        default=None,
        description=(
            "DEPRECATED (BSIM-104): use compatible_tiers. A single-queue value is "
            "promoted to a single-element compatibility set at load time."
        ),
    )

    @model_validator(mode="after")
    def _validate_window(self) -> "TimeWindowOverride":
        if self.end_time_s <= self.start_time_s:
            raise ValueError(
                f"end_time_s ({self.end_time_s}) must be > "
                f"start_time_s ({self.start_time_s})"
            )
        # BSIM-104: promote deprecated queue_name → compatible_tiers
        if self.queue_name is not None:
            warnings.warn(
                "TimeWindowOverride.queue_name is deprecated (BSIM-104); "
                "use compatible_tiers instead.",
                DeprecationWarning, stacklevel=2,
            )
            if self.compatible_tiers is None:
                self.compatible_tiers = self.queue_name
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

    # BSIM-104: tier compatibility binding
    compatible_tiers: str | list[str] | None = Field(
        default=None,
        description=(
            "BSIM-104: Tier profiles this centroid's jobs may run on. "
            "A single string sets the compatibility set for all bins; semicolons "
            "separate multiple tier names within the string (e.g. 'small;medium'). "
            "A list sets it per-bin: element[bin_idx], each itself a (possibly "
            "semicolon-delimited) compatibility set. List length must equal "
            "len(centroid_bin_weights). Absent = no constraint (legacy / no-tier mode "
            "or burst-derived inference when the scheduler has tiers configured)."
        ),
    )
    # BSIM-100: deprecated single-queue binding (promoted to compatible_tiers)
    queue_name: str | list[str] | None = Field(
        default=None,
        description=(
            "DEPRECATED (BSIM-104): use compatible_tiers. A single-queue value is "
            "promoted to a single-element compatibility set at load time."
        ),
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

        # BSIM-104: promote deprecated queue_name → compatible_tiers
        if self.queue_name is not None:
            warnings.warn(
                "CentroidConfig.queue_name is deprecated (BSIM-104); "
                "use compatible_tiers instead.",
                DeprecationWarning, stacklevel=2,
            )
            if self.compatible_tiers is None:
                self.compatible_tiers = self.queue_name

        # BSIM-104: per-bin compatible_tiers list length check
        if isinstance(self.compatible_tiers, list):
            n_bins = len(self.centroid_bin_weights) if self.centroid_bin_weights else 0
            if len(self.compatible_tiers) != n_bins:
                raise ValueError(
                    f"compatible_tiers list has {len(self.compatible_tiers)} entries but "
                    f"centroid_bin_weights has {n_bins}"
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
                if isinstance(w.compatible_tiers, list):
                    if n_bins is None:
                        raise ValueError(
                            f"time_window [{w.start_time_s}, {w.end_time_s}) "
                            "compatible_tiers list requires centroid_bin_weights to be set"
                        )
                    if len(w.compatible_tiers) != n_bins:
                        raise ValueError(
                            f"time_window [{w.start_time_s}, {w.end_time_s}) "
                            f"compatible_tiers list has {len(w.compatible_tiers)} entries "
                            f"but centroid has {n_bins} bins"
                        )

        return self


class SimulationConfig(BaseModel):
    horizon_seconds: PositiveFloat
    random_seed: int = 42
    network_bandwidth_mbps: PositiveFloat = 500.0
    cool_off_seconds: float = Field(
        default=0.0, ge=0.0,
        description=(
            "Extra simulation time after horizon_seconds during which "
            "no new jobs arrive but running jobs are allowed to finish. "
            "Prevents horizon truncation of jobs that start near the end "
            "of the arrival window. Total sim duration = "
            "horizon_seconds + cool_off_seconds."
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
    max_ebs_volumes: PositiveInt = 28


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
# BSIM-83 / BSIM-100: Time-based scheduling policy schema
# ---------------------------------------------------------------------------

class DrainRule(BaseModel):
    """
    A single drain condition: node enters DRAINING when idle_vcpu has
    continuously exceeded this threshold for duration_s seconds.
    """
    idle_vcpu: NonNegativeFloat
    duration_s: PositiveFloat


class TierProfile(BaseModel):
    """BSIM-104: Global, static definition of a node-tier profile (node pool).

    A tier binds an instance type to a fixed split of its RAM between the
    bin-packing zone and the preprocess-spike semaphore zone.  Multiple tiers
    may share a spawn_instance_class, differing only in spike_max_gb — this is
    what lets the joint provisioner (BSIM-107) trade bin-packing headroom for
    burst headroom on the same hardware.

    Hardware constants — they do not vary across time windows.  Every node
    launched for this tier carries these properties for its full lifetime.

    Renamed from QueueDefinition (BSIM-100); QueueDefinition remains as an alias.
    """
    name: str = Field(..., description="Unique tier identifier referenced by centroid compatible_tiers.")
    spike_max_gb: NonNegativeFloat = Field(
        ...,
        description=(
            "Non-schedulable semaphore region reserved on every node in this tier (GB). "
            "effective_schedulable_gb = instance.ram_gb - os_overhead_gb - spike_max_gb. "
            "A job is burst-compatible with this tier when "
            "(preprocess_peak - soft_limit) <= spike_max_gb. "
            "BSIM-113: 0 declares a no-boost tier — the whole node (minus OS overhead) "
            "is schedulable, suitable for flat jobs whose preprocess_peak <= soft_limit."
        ),
    )
    spawn_instance_class: str = Field(
        ..., description="Instance type name (from registry) launched for this tier."
    )

    @field_validator("name")
    @classmethod
    def _no_delimiter_in_name(cls, v: str) -> str:
        if TIER_SET_DELIMITER in v:
            raise ValueError(
                f"tier name {v!r} must not contain the compatibility-set "
                f"delimiter {TIER_SET_DELIMITER!r}"
            )
        return v


# BSIM-104: backward-compatible alias for the renamed model.
QueueDefinition = TierProfile


class QueuePolicy(BaseModel):
    """Per-window behavioral configuration for a queue.

    BSIM-100 (new format): set ``name`` to reference a global QueueDefinition.
    The hardware constants (spike_max_gb, spawn_instance_class) live on the
    QueueDefinition; only spawn rate, node cap, and drain rules vary per window.

    Legacy format (deprecated): set ``exclusive_min_gb`` / ``inclusive_max_gb`` /
    ``spawn_instance_class`` directly.  Accepted when no global ``queues:`` registry
    is declared on the SchedulerConfig.  Will be removed in a future epic.
    """
    # ── New format (BSIM-100) ────────────────────────────────────────────────
    name: str | None = Field(
        default=None,
        description="Named queue reference (BSIM-100). Must match a QueueDefinition.name.",
    )
    max_nodes: PositiveInt | None = Field(
        default=None,
        description="Maximum nodes active for this queue during this window. None = unlimited.",
    )
    # ── Legacy format (deprecated) ───────────────────────────────────────────
    exclusive_min_gb: NonNegativeFloat | None = Field(
        default=None,
        description="Deprecated (BSIM-100). Lower bound of the RAM band for implicit routing.",
    )
    inclusive_max_gb: PositiveFloat | None = Field(
        default=None,
        description="Deprecated (BSIM-100). Upper bound of the RAM band for implicit routing.",
    )
    spawn_instance_class: str | None = Field(
        default=None,
        description="Deprecated (BSIM-100). Moved to QueueDefinition.spawn_instance_class.",
    )
    # ── Shared ───────────────────────────────────────────────────────────────
    spawn_rate_per_min: PositiveFloat = Field(
        default=1.0,
        description="Maximum node launches per minute for this queue.",
    )
    drain_rules: list[DrainRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_queue(self) -> "QueuePolicy":
        is_new = self.name is not None
        is_legacy = self.inclusive_max_gb is not None
        if not is_new and not is_legacy:
            raise ValueError(
                "QueuePolicy must have either 'name' (new format, BSIM-100) "
                "or 'inclusive_max_gb' (legacy format)"
            )
        if is_legacy and self.inclusive_max_gb <= (self.exclusive_min_gb or 0.0):
            raise ValueError(
                f"inclusive_max_gb ({self.inclusive_max_gb}) must be > "
                f"exclusive_min_gb ({self.exclusive_min_gb or 0.0})"
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

    @property
    def is_named(self) -> bool:
        """True when using the new BSIM-100 named-queue format."""
        return self.name is not None


class TimeWindowPolicy(BaseModel):
    """A time window within a 24-hour day [start_time_s, end_time_s).

    Lists the queues active during this window and their behavioural config.
    When using the named-queue format (BSIM-100), each entry references a
    global QueueDefinition by name.  Queues not listed in a window are dormant.
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
        # Legacy format only: validate contiguous RAM-band coverage.
        legacy = [q for q in self.queues if not q.is_named]
        if legacy:
            legacy_sorted = sorted(legacy, key=lambda q: q.exclusive_min_gb or 0.0)
            if (legacy_sorted[0].exclusive_min_gb or 0.0) != 0.0:
                raise ValueError(
                    f"First legacy queue band in window [{self.start_time_s}, {self.end_time_s}) "
                    f"must start at exclusive_min_gb=0; "
                    f"got {legacy_sorted[0].exclusive_min_gb}"
                )
            for i in range(len(legacy_sorted) - 1):
                lo, hi = legacy_sorted[i], legacy_sorted[i + 1]
                if lo.inclusive_max_gb != (hi.exclusive_min_gb or 0.0):
                    raise ValueError(
                        f"Queue band gap/overlap in window [{self.start_time_s}, {self.end_time_s}): "
                        f"band ({lo.exclusive_min_gb}, {lo.inclusive_max_gb}] "
                        f"is not contiguous with "
                        f"band ({hi.exclusive_min_gb}, {hi.inclusive_max_gb}]"
                    )
        return self


# ---------------------------------------------------------------------------
# BSIM-E18: Karpenter-style provisioner (replaces time_window_policy)
# ---------------------------------------------------------------------------

class KarpenterProvisioner(BaseModel):
    """
    Workload-reactive node provisioner modelled on Karpenter semantics.

    Instance selection: at scale-out time, scores every allowed instance type
    by jobs-covered-per-dollar-per-hour against the live queue, then picks the
    highest-scoring type.  Naturally upsizes under load and downsizes when only
    a handful of jobs are pending — no calendar-based rules required.

    Node lifecycle: three orthogonal TTL timers replace the multi-threshold
    drain rules of the time-window policy:
      empty_ttl_s         — terminate this many seconds after the last job leaves
      underutilize_ttl_s  — drain when utilisation < underutilize_threshold_pct
                            for this many consecutive seconds
      max_node_ttl_s      — hard lifetime cap (Karpenter expireAfter); prevents
                            node hoarding regardless of utilisation

    Consolidation: after each job completes, if the node's utilisation drops
    below consolidation_threshold_pct AND another ready node has enough spare
    capacity to absorb the remaining load, the node enters DRAINING immediately
    rather than waiting for a TTL to fire.
    """
    allowed_instance_types: list[str] = Field(
        ..., min_length=1,
        description="Instance type names the provisioner may launch. "
                    "Must match names in the instance registry.",
    )
    empty_ttl_s: PositiveFloat = Field(
        default=30.0,
        description="Seconds after a node empties before it is terminated. "
                    "Analogous to Karpenter consolidateAfter=WhenEmpty.",
    )
    underutilize_threshold_pct: NonNegativeFloat = Field(
        default=30.0, le=100.0,
        description="vCPU utilisation percentage below which the "
                    "underutilisation timer starts accruing.",
    )
    underutilize_ttl_s: PositiveFloat = Field(
        default=300.0,
        description="Seconds a node must stay below underutilize_threshold_pct "
                    "(while still running jobs) before it enters DRAINING. "
                    "Analogous to Karpenter consolidateAfter=WhenUnderutilized.",
    )
    max_node_ttl_s: PositiveFloat = Field(
        default=3600.0,
        description="Hard node lifetime cap from READY time. "
                    "Analogous to Karpenter expireAfter.",
    )
    consolidation_threshold_pct: NonNegativeFloat = Field(
        default=40.0, le=100.0,
        description="If a node's utilisation drops below this percentage after "
                    "a job completes AND its remaining jobs fit on another node, "
                    "it enters DRAINING immediately (active consolidation).",
    )


# ---------------------------------------------------------------------------
# BSIM-91: EBS thin-pool storage cost config
# ---------------------------------------------------------------------------

class StoragePoolConfig(BaseModel):
    """Per-node EBS thin-pool storage configuration shared by Batch and K8S."""
    initial_volume_count: PositiveInt = Field(
        default=2,
        description="Number of EBS volumes attached at node launch.",
    )
    volume_size_gb: PositiveFloat = Field(
        default=1000.0,
        description="Physical size of each EBS volume in GB.",
    )
    logical_capacity_gb: PositiveFloat = Field(
        default=65536.0,
        description="LVM thin-pool overcommit ceiling (default 64 TB).",
    )
    expansion_trigger_pct: Fraction = Field(
        default=0.80,
        description="Expand physical pool when committed > this fraction × capacity.",
    )
    ebs_price_per_gb_hour: PositiveFloat = Field(
        default=0.0001096,
        description="EBS gp3 cost in USD per GB per hour (us-east-1 on-demand).",
    )


class BaseSchedulerConfig(BaseModel):
    """BSIM-109: cross-cutting scheduler config — fields every scheduler reads.

    Concrete per-scheduler schemas (BatchConfig / K8SConfig / K8SPlusConfig) extend
    this and add only the fields their scheduler consumes. `SchedulerConfig` is the
    discriminated union over them keyed on `scheduler_type`, so a config's scheduler
    is intrinsic to the config — no separate argument needed (BSIM-123).

    extra='forbid': a config carrying a field its scheduler doesn't consume (e.g. a
    Batch config with `tiers`) is a hard error, not a silent no-op — the type *is*
    the support matrix.
    """
    model_config = ConfigDict(extra="forbid")

    panic_threshold_seconds: PositiveFloat = 300.0
    sla_target_seconds: PositiveFloat = 600.0
    warmup_delay_seconds: PositiveFloat = 90.0
    idle_timeout_seconds: PositiveFloat = 300.0
    idle_check_interval_seconds: PositiveFloat = 30.0  # BSIM-110: dead, pending removal
    max_retries: PositiveInt = 3
    replay_delay_seconds: NonNegativeFloat = 10.0
    scale_out_threshold_s: NonNegativeFloat = Field(
        default=0.0,
        description=(
            "BSIM-86: Minimum queue-wait time (seconds) before the scale-out monitor "
            "provisions a new node. 0 = provision immediately for any unplaceable job."
        ),
    )
    scale_out_poll_s: PositiveFloat = Field(
        default=60.0,
        description=(
            "BSIM-86: Polling interval (seconds) for the scale-out monitor. "
            "Also serves as the cool_down period after each node launch."
        ),
    )
    storage: StoragePoolConfig | None = Field(
        default=None,
        description=(
            "BSIM-91: EBS thin-pool storage cost model. "
            "When absent, storage costs are not tracked."
        ),
    )


class BatchConfig(BaseSchedulerConfig):
    """AWS Batch scheduler — cross-cutting fields only (no K8S/tier concepts)."""
    scheduler_type: Literal[SchedulerType.BATCH] = SchedulerType.BATCH


class K8SConfig(BaseSchedulerConfig):
    """K8S scheduler — adds the K8S-family fields (os overhead, time windows, tiers)."""
    scheduler_type: Literal[SchedulerType.K8S] = SchedulerType.K8S
    os_overhead_gb: NonNegativeFloat = 2.0
    time_window_policy: list[TimeWindowPolicy] | None = Field(
        default=None,
        description=(
            "BSIM-83: Optional time-based scheduling policy. "
            "When present, partitions the 24-hour day into windows that each "
            "define memory-band queues with explicit instance classes and drain rules. "
            "When absent, the scheduler uses its default instance-selection behavior."
        ),
    )
    tiers: list[TierProfile] = Field(
        default_factory=list,
        description=(
            "BSIM-104: Global tier-profile registry. "
            "When non-empty, enables tier-compatibility routing: centroids declare "
            "the tiers their jobs may run on via compatible_tiers; each node pool is "
            "sized by the tier's spike_max_gb rather than derived from job-spec data. "
            "Multiple tiers may share a spawn_instance_class. "
            "When empty, the scheduler falls back to legacy RAM-band routing."
        ),
    )
    queues: list[TierProfile] = Field(
        default_factory=list,
        description=(
            "DEPRECATED (BSIM-104): use tiers. Kept in sync with tiers at load time "
            "so existing configs and not-yet-migrated readers continue to work."
        ),
    )

    @model_validator(mode="after")
    def _validate_k8s(self) -> "K8SConfig":
        # BSIM-104: keep deprecated `queues` and canonical `tiers` in sync.
        if self.queues and not self.tiers:
            warnings.warn(
                "K8SConfig.queues is deprecated (BSIM-104); use tiers instead.",
                DeprecationWarning, stacklevel=2,
            )
            self.tiers = self.queues
        elif self.tiers and not self.queues:
            self.queues = self.tiers

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
        # BSIM-104: validate named tier references against the global registry.
        if self.tiers:
            defined = {t.name for t in self.tiers}
            for w in self.time_window_policy:
                for qp in w.queues:
                    if qp.is_named and qp.name not in defined:
                        raise ValueError(
                            f"Tier '{qp.name}' in window [{w.start_time_s}, {w.end_time_s}) "
                            f"is not declared in the global 'tiers' registry. "
                            f"Declared tiers: {sorted(defined)}"
                        )
        return self


class K8SPlusConfig(K8SConfig):
    """K8S+ scheduler — adds the Karpenter-style provisioner (K8S+-only)."""
    scheduler_type: Literal[SchedulerType.K8SPLUS] = SchedulerType.K8SPLUS
    provisioner: KarpenterProvisioner | None = Field(
        default=None,
        description=(
            "BSIM-E18: Karpenter-style workload-reactive provisioner. "
            "When present, replaces time_window_policy with demand-scored "
            "instance selection and three-TTL node lifecycle management."
        ),
    )


# BSIM-109: discriminated union — `load_scheduler_config` returns the concrete
# subclass, so the loaded type *is* the scheduler. Usable as a field type and via
# TypeAdapter; construct the concrete subclasses directly.
SchedulerConfig = Annotated[
    BatchConfig | K8SConfig | K8SPlusConfig,
    Field(discriminator="scheduler_type"),
]


class ExperimentConfig(BaseModel):
    event_list_path: str
    instance_registry_path: str = "configs/instance_registry.yaml"
    output_dir: str
    panic_threshold_values: list[PositiveFloat] = Field(..., min_length=2)
    base_scheduler_config: SchedulerConfig
    schedulers: list[SchedulerType] = [SchedulerType.BATCH, SchedulerType.K8S]
