"""BSIM-6: Pareto distribution sampler."""
from __future__ import annotations
import numpy as np
from numpy.random import Generator
from batch_sim.core.schemas import CentroidConfig
from batch_sim.generator.job_spec import JobSpec, build_phase_profile


def _pareto_multiplier(alpha: float, rng: Generator) -> float:
    if alpha > 1.0:
        draw = rng.pareto(alpha) + 1.0
        mean = alpha / (alpha - 1.0)
        multiplier = draw / mean
    else:
        multiplier = rng.uniform(0.8, 1.2)
    return float(np.clip(multiplier, 0.25, 4.0))


def sample_job(centroid: CentroidConfig, rng: Generator, network_bandwidth_mbps: float) -> JobSpec:
    alpha = centroid.pareto_alpha
    download_gb = max(centroid.download_gb * _pareto_multiplier(alpha, rng), 0.1)
    a = centroid.preprocess_memory_exponent_a * _pareto_multiplier(alpha, rng)
    b = centroid.preprocess_memory_exponent_b
    preprocess_duration_s = min(
        centroid.preprocess_duration_seconds * _pareto_multiplier(alpha, rng), 120.0
    )
    perturbed_stages = [
        max(s * _pareto_multiplier(alpha, rng), 1.0)
        for s in centroid.workhorse_cpu_stages
    ]
    perturbed_threads = [
        int(np.clip(round(t * _pareto_multiplier(alpha, rng)), 1, 64))
        for t in centroid.workhorse_thread_counts
    ]
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
    return JobSpec(centroid_id=centroid.id, profile=profile)
