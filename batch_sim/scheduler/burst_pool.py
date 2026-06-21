"""
BSIM-55: NodeBurstPool — RAM-aware burst coordination for Phase 2.

Replaces the fixed-permit NodeSemaphore with a pool that tracks actual GB
in use during Phase 2 bursts. Two small-spike jobs may burst simultaneously
if their combined peak fits within headroom; a large-spike job serialises
against any other concurrent burst.

Physical model:
  headroom_gb  = node.physical_ram - os_overhead - node_max_peak
  where node_max_peak = max(M for all jobs placed on this node)

  Before entering Phase 2, each job acquires M GB from the pool.
  It holds that reservation for the duration of Phase 2, then releases.
  If headroom_gb - in_use_gb < M, the job blocks until prior bursts complete.

Headroom update:
  When a new job is placed on the node and its M exceeds the current max,
  headroom_gb is recomputed. Jobs already in Phase 2 are NOT interrupted
  (they hold valid reservations); new requests simply observe the tighter pool.
"""

from __future__ import annotations
import simpy


class NodeBurstPool:
    """
    Per-node burst RAM pool for Phase-2 spike coordination.
    Thread-safe in the SimPy single-process sense.
    """

    def __init__(
        self,
        env: simpy.Environment,
        node_physical_ram_gb: float,
        os_overhead_gb: float,
        headroom_gb: float | None = None,
    ) -> None:
        self._env = env
        self._physical = node_physical_ram_gb
        self._os = os_overhead_gb
        self._node_max_peak: float = 0.0   # max M among jobs placed so far
        # BSIM-122: when headroom_gb is given, pool capacity is FIXED at that
        # reservation (the tier's spike_max_gb) and does not grow with the workload —
        # the reservation is the budget, and bursts never borrow bin-packing space.
        # The legacy workload-derived update_max_peak() path remains for the pre-tier
        # two-queue scheduler (which passes no headroom_gb).
        self._fixed_headroom: bool = headroom_gb is not None
        self._headroom: float = headroom_gb if headroom_gb is not None else 0.0
        self._in_use: float = 0.0
        self._waiters: list[tuple[float, simpy.Event]] = []  # (required_gb, event)

    # ------------------------------------------------------------------
    # Called by scheduler when a new job is placed on this node
    # ------------------------------------------------------------------

    def update_max_peak(self, new_job_peak_gb: float) -> None:
        """
        Legacy (pre-BSIM-122) workload-derived sizing: burst pool capacity =
        node_max_peak (the reserved burst region). No-op when the pool was
        constructed with a fixed headroom_gb (BSIM-122 tier reservation).
        """
        if self._fixed_headroom:
            return
        if new_job_peak_gb > self._node_max_peak:
            self._node_max_peak = new_job_peak_gb
            self._headroom = self._node_max_peak

    @property
    def headroom_gb(self) -> float:
        return self._headroom

    @property
    def available_gb(self) -> float:
        return max(0.0, self._headroom - self._in_use)

    # ------------------------------------------------------------------
    # Phase 2 coordination
    # ------------------------------------------------------------------

    def acquire(self, peak_ram_gb: float):
        """
        SimPy generator. Returns immediately if `peak_ram_gb` of burst headroom is
        free (claiming it); otherwise blocks until a release() transfers the claim.

        Transfer semantics: when a waiter is woken, release() has ALREADY added
        peak_ram_gb to _in_use on its behalf, so the woken path must NOT re-claim
        or re-check (doing so double-counts and deadlocks the waiter).
        """
        if self._in_use + peak_ram_gb <= self._headroom:
            self._in_use += peak_ram_gb
            return
        event = self._env.event()
        self._waiters.append((peak_ram_gb, event))
        yield event   # release() already claimed peak_ram_gb for us on wake

    def release(self, peak_ram_gb: float) -> None:
        """Release burst reservation; transfer the claim to any waiters that now fit."""
        self._in_use = max(0.0, self._in_use - peak_ram_gb)
        # Wake waiters in FIFO order if they can now be satisfied
        remaining = []
        for required, ev in self._waiters:
            if self._in_use + required <= self._headroom:
                self._in_use += required   # transfer claim to the waiter before waking
                ev.succeed()
            else:
                remaining.append((required, ev))
        self._waiters = remaining
