"""BSIM-7/8/9: Arrival model, event list, and serialization."""
from __future__ import annotations
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import numpy as np
from numpy.random import Generator
from batch_sim.core.schemas import CentroidConfig, SimulationConfig
from batch_sim.generator.job_spec import JobSpec, PhaseProfile, Stage
from batch_sim.generator.sampler import sample_job


@dataclass
class ArrivalRecord:
    arrival_time: float
    centroid_id: str
    bin_weights_override: list[float] | None = None  # BSIM-77: active window weights


def _active_window(centroid, t):
    """Return the TimeWindowOverride covering time t, or None."""
    if not centroid.time_windows:
        return None
    for w in centroid.time_windows:
        if w.start_time_s <= t < w.end_time_s:
            return w
    return None


def _window_boundary(centroid, t, horizon_seconds):
    """
    Return the time at which the current rate context ends — either the end of
    the active window, the start of the next window, or horizon_seconds.
    """
    if not centroid.time_windows:
        return horizon_seconds
    for w in centroid.time_windows:
        if w.start_time_s <= t < w.end_time_s:
            return min(w.end_time_s, horizon_seconds)
    # t is in a gap; advance to the nearest upcoming window boundary
    upcoming = [w.start_time_s for w in centroid.time_windows if w.start_time_s > t]
    return min(min(upcoming), horizon_seconds) if upcoming else horizon_seconds


def generate_arrivals(centroids, horizon_seconds, rng):
    """
    Generate arrival records for all centroids.

    arrival_rate_per_hour governs BURST events per hour.
    Each burst emits N jobs with identical arrival_time, where N ~ Uniform[burst_size_min, burst_size_max].

    arrival_spacing (per centroid):
      'poisson'     — memoryless rng.exponential(1/λ); exact Poisson, preserves clustering.
      'approximate' — np.average of 5000 draws; reduced variance for test configs.

    Time windows (BSIM-77/78): piecewise-constant Poisson with exact boundary crossing.
      At time t in a context ending at W_end with rate λ:
        Draw τ ~ Exp(λ). If t+τ ≥ W_end: discard, advance to W_end, restart with new rate.
        If t+τ < W_end: place arrival at t+τ.
    """
    records = []
    for centroid in centroids:
        lo  = centroid.burst_size_min
        hi  = centroid.burst_size_max
        approximate = (centroid.arrival_spacing == "approximate")
        t   = 0.0
        while t < horizon_seconds:
            window  = _active_window(centroid, t)
            w_end   = _window_boundary(centroid, t, horizon_seconds)
            lam     = ((window.burst_rate / 3600.0)
                       if (window and window.burst_rate is not None)
                       else centroid.arrival_rate_per_hour / 3600.0)

            if approximate:
                gap = float(np.average(rng.exponential(1.0 / lam, 5000)))
            else:
                gap = rng.exponential(1.0 / lam)

            if t + gap >= w_end:
                # Boundary crossing — discard this draw, restart at boundary
                t = w_end
                continue

            t += gap
            if t >= horizon_seconds:
                break

            active_weights = (window.centroid_bin_weights
                              if (window and window.centroid_bin_weights is not None)
                              else None)
            n = int(rng.integers(lo, hi + 1)) if hi > lo else lo
            for _ in range(n):
                records.append(ArrivalRecord(
                    arrival_time=t,
                    centroid_id=centroid.id,
                    bin_weights_override=active_weights,
                ))
    records.sort(key=lambda r: r.arrival_time)
    return records


@dataclass
class JobArrivalEvent:
    job_id: str
    arrival_time: float
    centroid_id: str
    download_duration_s: float
    download_ram_gb: float
    preprocess_duration_s: float
    preprocess_peak_ram_gb: float
    preprocess_steady_ram_gb: float
    preprocess_vcpu: float
    workhorse_stages: list[dict]
    workhorse_duration_s: float
    workhorse_peak_vcpu: float
    workhorse_declared_vcpu: int
    workhorse_ram_gb: float
    upload_duration_s: float
    upload_ram_gb: float
    soft_cpu: int = 0       # BSIM-69: K8S soft limit (scheduler reservation)
    hard_cpu: int = 0       # BSIM-69: K8S hard limit (burst ceiling = thread count)
    workhorse_hard_limit_gb: float = 0.0  # bin_steady_state_hard_limit_gb → K8S memory request

    def to_job_spec(self) -> JobSpec:
        stages = [
            Stage(index=s["index"], cpu_seconds=s["cpu_seconds"],
                  declared_threads=s["declared_threads"],
                  effective_threads=s["effective_threads"],
                  wall_clock_seconds=s["wall_clock_seconds"])
            for s in self.workhorse_stages
        ]
        profile = PhaseProfile(
            download_duration_s=self.download_duration_s,
            download_ram_gb=self.download_ram_gb,
            preprocess_duration_s=self.preprocess_duration_s,
            preprocess_peak_ram_gb=self.preprocess_peak_ram_gb,
            preprocess_steady_ram_gb=self.preprocess_steady_ram_gb,
            preprocess_vcpu=self.preprocess_vcpu,
            stages=stages,
            workhorse_duration_s=self.workhorse_duration_s,
            workhorse_peak_vcpu=self.workhorse_peak_vcpu,
            workhorse_declared_vcpu=self.workhorse_declared_vcpu,
            workhorse_hard_limit_gb=self.workhorse_hard_limit_gb,
            workhorse_ram_gb=self.workhorse_ram_gb,
            upload_duration_s=self.upload_duration_s,
            upload_ram_gb=self.upload_ram_gb,
        )
        return JobSpec(job_id=self.job_id, centroid_id=self.centroid_id,
                       profile=profile,
                       soft_cpu=self.soft_cpu, hard_cpu=self.hard_cpu)


def _event_from_job(arrival_time, job):
    p = job.profile
    return JobArrivalEvent(
        job_id=job.job_id, arrival_time=arrival_time, centroid_id=job.centroid_id,
        download_duration_s=p.download_duration_s, download_ram_gb=p.download_ram_gb,
        preprocess_duration_s=p.preprocess_duration_s,
        preprocess_peak_ram_gb=p.preprocess_peak_ram_gb,
        preprocess_steady_ram_gb=p.preprocess_steady_ram_gb,
        preprocess_vcpu=p.preprocess_vcpu,
        workhorse_stages=[dict(index=s.index, cpu_seconds=s.cpu_seconds,
                               declared_threads=s.declared_threads,
                               effective_threads=s.effective_threads,
                               wall_clock_seconds=s.wall_clock_seconds) for s in p.stages],
        workhorse_duration_s=p.workhorse_duration_s,
        workhorse_peak_vcpu=p.workhorse_peak_vcpu,
        workhorse_declared_vcpu=p.workhorse_declared_vcpu,
        workhorse_hard_limit_gb=p.workhorse_hard_limit_gb,
        workhorse_ram_gb=p.workhorse_ram_gb,
        upload_duration_s=p.upload_duration_s, upload_ram_gb=p.upload_ram_gb,
        soft_cpu=job.soft_cpu, hard_cpu=job.hard_cpu,
    )


@dataclass
class EventList:
    events: list[JobArrivalEvent]
    metadata: dict[str, Any]

    def __len__(self): return len(self.events)

    def centroid_counts(self):
        counts = {}
        for e in self.events:
            counts[e.centroid_id] = counts.get(e.centroid_id, 0) + 1
        return counts

    @property
    def time_span_seconds(self):
        return self.events[-1].arrival_time if self.events else 0.0


def build_event_list(config: SimulationConfig) -> EventList:
    rng = np.random.default_rng(config.random_seed)
    centroid_map = {c.id: c for c in config.centroids}
    arrivals = generate_arrivals(config.centroids, config.horizon_seconds, rng)
    events = [_event_from_job(r.arrival_time,
                              sample_job(centroid_map[r.centroid_id], rng,
                                         config.network_bandwidth_mbps,
                                         bin_weights_override=r.bin_weights_override))
              for r in arrivals]
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "random_seed": config.random_seed,
        "horizon_seconds": config.horizon_seconds,
        "network_bandwidth_mbps": config.network_bandwidth_mbps,
        "centroid_ids": [c.id for c in config.centroids],
        "total_jobs": len(events),
        "cool_off_seconds": config.cool_off_seconds,
        "burst_params": {
            c.id: {"min": c.burst_size_min, "max": c.burst_size_max}
            for c in config.centroids
        },
    }
    return EventList(events=events, metadata=metadata)


def save_event_list(event_list: EventList, path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"metadata": event_list.metadata,
                   "events": [asdict(e) for e in event_list.events]}, f, indent=2)


def load_event_list(path) -> EventList:
    with open(path) as f:
        payload = json.load(f)
    return EventList(events=[JobArrivalEvent(**e) for e in payload["events"]],
                     metadata=payload["metadata"])
