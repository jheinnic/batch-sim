"""BSIM-114: preflight tier-compatibility validation across the sim/scheduler config pair.

`compatible_tiers` on a centroid names tier profiles defined in a *separate* scheduler
config file, so Pydantic can't validate the cross-file reference at load time — a
typo'd or physically-impossible tier name otherwise surfaces only at run time, as
scattered ADMISSION_REJECTED/TIER_COMPATIBILITY_WARN events. This module catches the
error classes a hand-edited tier-string pairing actually produces: missing tier names,
naming drift, and reservations that leave no schedulable zone.
"""
from __future__ import annotations
import warnings
from typing import Any

from batch_sim.core.schemas import SimulationConfig, parse_tier_set
from batch_sim.registry.instance_registry import InstanceRegistry


class TierPreflightError(ValueError):
    """Raised by validate_config_pair when any reference-integrity or
    physical-validity check fails. Lists every violation found, not just the first —
    hand-editing tier strings across two files tends to produce several at once."""


def _bin_tiers(raw: "str | list[str] | None", bin_idx: int) -> list[str]:
    if raw is None:
        return []
    val = raw[bin_idx] if isinstance(raw, list) else raw
    return parse_tier_set(val) if val else []


def validate_tier_physical_limits(scheduler_config: Any, registry: InstanceRegistry) -> list[str]:
    """Standalone check: every declared TierProfile leaves a positive schedulable zone
    on its spawn_instance_class. Needs only the scheduler config and the registry —
    no sim_config required. Returns a list of error strings (empty = all valid)."""
    tiers = getattr(scheduler_config, "tiers", None) or []
    os_overhead = getattr(scheduler_config, "os_overhead_gb", 0.0)
    errors: list[str] = []
    for t in tiers:
        inst = registry.get_by_name(t.spawn_instance_class)
        if inst is None:
            errors.append(
                f"tier '{t.name}': spawn_instance_class '{t.spawn_instance_class}' "
                f"not found in the instance registry")
            continue
        schedulable = inst.ram_gb - os_overhead
        if t.spike_max_gb >= schedulable:
            errors.append(
                f"tier '{t.name}': spike_max_gb={t.spike_max_gb} >= schedulable zone "
                f"{schedulable:.2f} GB on {t.spawn_instance_class} "
                f"(ram_gb={inst.ram_gb}, os_overhead_gb={os_overhead}) — "
                f"leaves no bin-packing room")
    return errors


def validate_config_pair(
    sim_config: SimulationConfig,
    scheduler_config: Any,
    registry: InstanceRegistry,
) -> None:
    """Validate sim_config's centroid tier references against scheduler_config's
    tier registry, and every tier's physical validity on its instance type.

    No-op when scheduler_config has no tiers (BatchConfig — Batch ignores
    compatible_tiers and its admission control entirely; or a tier-less/legacy K8S
    config with nothing to cross-reference).

    Raises TierPreflightError listing every reference-integrity (BSIM-104 tier names)
    and physical-validity (tier reservation vs. instance RAM) violation found. Emits a
    UserWarning per centroid bin whose listed tiers can't host its declared burst
    (jobs in that bin will be ADMISSION_REJECTED at run time) — a warning, not an
    error, since it may be intentional (e.g. a bin fed entirely by burst-derived
    inference elsewhere).
    """
    tiers = getattr(scheduler_config, "tiers", None)
    if not tiers:
        return

    errors = validate_tier_physical_limits(scheduler_config, registry)
    defined = {t.name for t in tiers}
    tier_spike = {t.name: t.spike_max_gb for t in tiers}

    for c in sim_config.centroids:
        n_bins = len(c.centroid_bin_weights) if c.centroid_bin_weights else 1
        hard = c.bin_preloader_hard_limit_gb
        soft = c.bin_steady_state_hard_limit_gb

        for bin_idx in range(n_bins):
            bin_tiers = _bin_tiers(c.compatible_tiers, bin_idx)
            for name in bin_tiers:
                if name not in defined:
                    errors.append(
                        f"centroid '{c.id}' bin {bin_idx}: compatible_tiers names "
                        f"undeclared tier '{name}' (declared: {sorted(defined)})")

            for w_idx, w in enumerate(c.time_windows or []):
                for name in _bin_tiers(w.compatible_tiers, bin_idx):
                    if name not in defined:
                        errors.append(
                            f"centroid '{c.id}' bin {bin_idx} time_windows[{w_idx}] "
                            f"[{w.start_time_s},{w.end_time_s}): compatible_tiers names "
                            f"undeclared tier '{name}' (declared: {sorted(defined)})")

            # Burst reachability (warning): only computable when both hard limits are
            # declared bin-wise — these are the same fields the per-job admission
            # check reads (BSIM-108), so this mirrors run-time behaviour exactly.
            if hard is not None and soft is not None and bin_tiers:
                min_spike = hard[bin_idx] - soft[bin_idx]
                if not any(tier_spike.get(n, -1.0) >= min_spike for n in bin_tiers):
                    warnings.warn(
                        f"centroid '{c.id}' bin {bin_idx}: no listed tier can host "
                        f"this bin's burst (min_spike={min_spike:.2f} GB) among "
                        f"{bin_tiers} — jobs in this bin will be ADMISSION_REJECTED",
                        stacklevel=2)

    if errors:
        raise TierPreflightError(
            f"{len(errors)} tier preflight error(s):\n" +
            "\n".join(f"  - {e}" for e in errors))
