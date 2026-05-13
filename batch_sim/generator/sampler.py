"""BSIM-6: Pareto distribution sampler."""
from __future__ import annotations
import numpy as np
from numpy.random import Generator
from batch_sim.core.schemas import CentroidConfig
from batch_sim.generator.job_spec import JobSpec, build_phase_profile


def _pareto_multiplier(
    alpha: float,
    rng: Generator,
    min_mult: float = 0.25,
    max_mult: float = 4.0,
) -> float:
    """
    Draw a mean-normalised Pareto multiplier clamped to [min_mult, max_mult].
    min_mult and max_mult come from CentroidConfig.pareto_multiplier_{min,max}
    so each centroid can control its own tail behaviour independently.
    """
    if alpha > 1.0:
        draw = rng.pareto(alpha) + 1.0
        mean = alpha / (alpha - 1.0)
        multiplier = draw / mean
    else:
        multiplier = rng.uniform(0.8, 1.2)
    return float(np.clip(multiplier, min_mult, max_mult))


def sample_job(centroid: CentroidConfig, rng: Generator, network_bandwidth_mbps: float) -> JobSpec:
    alpha   = centroid.pareto_alpha
    lo      = centroid.pareto_multiplier_min
    hi      = centroid.pareto_multiplier_max

    def pm() -> float:
        """Shorthand: draw one multiplier using this centroid's clamp bounds."""
        return _pareto_multiplier(alpha, rng, min_mult=lo, max_mult=hi)

    download_gb = max(centroid.download_gb * pm(), 0.1)
    a = centroid.preprocess_memory_exponent_a * pm()
    b = centroid.preprocess_memory_exponent_b
    preprocess_duration_s = min(
        centroid.preprocess_duration_seconds * pm(), 120.0
    )
    perturbed_stages = [
        max(s * pm(), 1.0)
        for s in centroid.workhorse_cpu_stages
    ]
    # Thread counts are hardware parallelism declarations, not workload
    # size variables. They are NOT Pareto-perturbed. Scaling them with
    # the same multiplier as download_gb causes declared_vcpu to balloon
    # (e.g. 16 threads * 3x multiplier = 48 vcpu), leaving room for only
    # one job per node regardless of available RAM.
    perturbed_threads = list(centroid.workhorse_thread_counts)
    # Per-stage I/O wait — independent perturbation per stage if specified
    if centroid.workhorse_io_wait_per_stage is not None:
        io_wait_fractions = [
            float(np.clip(w + rng.normal(0, 0.05), 0.05, 0.95))
            for w in centroid.workhorse_io_wait_per_stage
        ]
        # Scalar io_wait kept as fallback; use first stage value for any
        # code that still references the scalar (e.g. display/logging)
        io_wait = float(np.mean(io_wait_fractions))
    else:
        io_wait = float(np.clip(centroid.io_wait_fraction + rng.normal(0, 0.05), 0.05, 0.95))
        io_wait_fractions = None

    upload_gb = max(centroid.upload_gb * _pareto_multiplier(alpha, rng), 0.01)

    profile = build_phase_profile(
        download_gb=download_gb, preprocess_a=a, preprocess_b=b,
        preprocess_duration_s=preprocess_duration_s,
        workhorse_cpu_stages=perturbed_stages,
        workhorse_thread_counts=perturbed_threads,
        io_wait_fraction=io_wait, upload_gb=upload_gb,
        network_bandwidth_mbps=network_bandwidth_mbps,
        io_wait_fractions=io_wait_fractions,
    )
    # BSIM-69: derive job-level soft/hard CPU limits from per-stage arrays
    if centroid.workhorse_soft_vcpu is not None:
        soft_cpu = max(centroid.workhorse_soft_vcpu)
    else:
        soft_cpu = profile.workhorse_declared_vcpu

    if centroid.workhorse_hard_vcpu is not None:
        hard_cpu = max(centroid.workhorse_hard_vcpu)
    else:
        hard_cpu = soft_cpu   # no burst: Batch behaviour

    return JobSpec(centroid_id=centroid.id, profile=profile,
                   soft_cpu=soft_cpu, hard_cpu=hard_cpu)
