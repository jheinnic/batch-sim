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
    ) -> None:
        self._env = env
        self._physical = node_physical_ram_gb
        self._os = os_overhead_gb
        self._node_max_peak: float = 0.0   # max M among jobs placed so far
        self._headroom: float = 0.0        # C - os - node_max_peak
        self._in_use: float = 0.0
        self._waiters: list[tuple[float, simpy.Event]] = []  # (required_gb, event)

    # ------------------------------------------------------------------
    # Called by scheduler when a new job is placed on this node
    # ------------------------------------------------------------------

    def update_max_peak(self, new_job_peak_gb: float) -> None:
        """
        Burst pool capacity = spike_headroom = node_max_peak (the reserved burst
        region), NOT C - os - M (which is the schedulable capacity and causes
        deadlock for any job where M > (C-os)/2).
        """
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
        SimPy generator. Blocks until `peak_ram_gb` of burst headroom is free,
        then marks it as in use.
        """
        while self._in_use + peak_ram_gb > self._headroom:
            event = self._env.event()
            self._waiters.append((peak_ram_gb, event))
            yield event
        self._in_use += peak_ram_gb

    def release(self, peak_ram_gb: float) -> None:
        """Release burst reservation; wake any waiters that can now proceed."""
        self._in_use = max(0.0, self._in_use - peak_ram_gb)
        # Wake waiters in FIFO order if they can now be satisfied
        remaining = []
        for required, ev in self._waiters:
            if self._in_use + required <= self._headroom:
                ev.succeed()
                self._in_use += required   # pre-claim for the waiter
            else:
                remaining.append((required, ev))
        self._waiters = remaining
