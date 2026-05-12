"""BSIM-5: Per-job phase profile and job specification."""
from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from typing import List


@dataclass
class Stage:
    index: int
    cpu_seconds: float
    declared_threads: int
    effective_threads: float
    wall_clock_seconds: float


@dataclass
class PhaseProfile:
    download_duration_s: float
    download_ram_gb: float = 0.5
    preprocess_duration_s: float = 0.0
    preprocess_peak_ram_gb: float = 0.0
    preprocess_steady_ram_gb: float = 0.0
    preprocess_vcpu: float = 1.0
    stages: List[Stage] = field(default_factory=list)
    workhorse_duration_s: float = 0.0
    workhorse_peak_vcpu: float = 0.0
    workhorse_declared_vcpu: int = 0
    workhorse_ram_gb: float = 0.0
    upload_duration_s: float = 0.0
    upload_ram_gb: float = 0.5

    @property
    def total_duration_s(self) -> float:
        return (self.download_duration_s + self.preprocess_duration_s
                + self.workhorse_duration_s + self.upload_duration_s)

    @property
    def peak_ram_gb(self) -> float:
        return self.preprocess_peak_ram_gb

    @property
    def soft_limit_ram_gb(self) -> float:
        return self.preprocess_steady_ram_gb


@dataclass
class JobSpec:
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    centroid_id: str = ""
    profile: PhaseProfile = field(default_factory=PhaseProfile)
    retry_count: int = 0

    def fresh_copy(self) -> "JobSpec":
        import copy
        return copy.deepcopy(self)


def build_phase_profile(
    *, download_gb, preprocess_a, preprocess_b, preprocess_duration_s,
    workhorse_cpu_stages, workhorse_thread_counts, io_wait_fraction,
    upload_gb, network_bandwidth_mbps,
    io_wait_fractions: list[float] | None = None,
) -> PhaseProfile:
    """
    io_wait_fractions: optional list with one value per parallel stage.
    If provided, overrides the scalar io_wait_fraction for each parallel stage
    independently, allowing CPU-bound and I/O-bound stages to be modelled
    with different effective thread counts.
    Falls back to scalar io_wait_fraction if absent (backward-compatible).
    """
    bandwidth_gbs = network_bandwidth_mbps / 1000.0
    download_duration_s = download_gb / bandwidth_gbs
    peak_ram_gb = preprocess_a * (download_gb ** preprocess_b)
    steady_ram_gb = 0.08 * peak_ram_gb

    stages: list[Stage] = []
    parallel_idx = 0
    workhorse_total_s = 0.0
    max_effective = 0.0
    max_declared = 0

    for i, cpu_seconds in enumerate(workhorse_cpu_stages):
        if i % 2 == 0:
            declared = workhorse_thread_counts[parallel_idx]
            # Use per-stage wait if provided, else fall back to scalar
            stage_wait = (
                io_wait_fractions[parallel_idx]
                if io_wait_fractions is not None
                else io_wait_fraction
            )
            effective = declared * (1.0 - stage_wait)
            parallel_idx += 1
        else:
            declared = 1
            effective = 1.0
        wall_s = cpu_seconds / effective if effective > 0 else cpu_seconds
        stages.append(Stage(index=i, cpu_seconds=cpu_seconds,
                            declared_threads=declared, effective_threads=effective,
                            wall_clock_seconds=wall_s))
        workhorse_total_s += wall_s
        max_effective = max(max_effective, effective)
        max_declared = max(max_declared, declared)

    upload_duration_s = upload_gb / bandwidth_gbs

    return PhaseProfile(
        download_duration_s=download_duration_s,
        preprocess_duration_s=preprocess_duration_s,
        preprocess_peak_ram_gb=peak_ram_gb,
        preprocess_steady_ram_gb=steady_ram_gb,
        preprocess_vcpu=1.0,
        stages=stages,
        workhorse_duration_s=workhorse_total_s,
        workhorse_peak_vcpu=max_effective,
        workhorse_declared_vcpu=max_declared,
        workhorse_ram_gb=steady_ram_gb,
        upload_duration_s=upload_duration_s,
    )
