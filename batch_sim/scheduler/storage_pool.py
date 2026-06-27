"""BSIM-92/93: EBS thin-pool storage models for Batch (single pool) and K8S (generational)."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from batch_sim.core.schemas import StorageModel

if TYPE_CHECKING:
    from batch_sim.metrics.collector import MetricsCollector
    from batch_sim.core.schemas import StoragePoolConfig, InstanceTypeConfig


@dataclass
class NodeStoragePool:
    """BSIM-92: Single monotonically-expanding thin pool backing one Batch node."""
    node_id: str
    config: "StoragePoolConfig"
    instance: "InstanceTypeConfig"
    open_time: float

    pool_capacity_gb: float = field(init=False)
    pool_committed_gb: float = field(init=False)
    attached_volumes: int = field(init=False)
    _close_time: float = field(init=False, default=-1.0)

    def __post_init__(self) -> None:
        self.pool_capacity_gb = self.config.initial_volume_count * self.config.volume_size_gb
        self.pool_committed_gb = 0.0
        self.attached_volumes = self.config.initial_volume_count

    @property
    def max_physical_capacity_gb(self) -> float:
        return self.instance.max_ebs_volumes * self.config.volume_size_gb

    def has_room_for(self, workspace_gb: float) -> bool:
        """BSIM-127: would admitting a job with this workspace exceed the
        node's storage-exhaustion ceiling? Mirrors the condition _maybe_expand
        uses internally so a job is never placed somewhere it would only
        immediately trigger STORAGE_EXHAUSTED."""
        max_committed = self.config.expansion_trigger_pct * self.max_physical_capacity_gb
        return self.pool_committed_gb + workspace_gb <= max_committed

    def announce(self, t: float, metrics: "MetricsCollector") -> None:
        """Emit the STORAGE_POOL_OPENED event for this pool's initial capacity.

        __post_init__ sets pool_capacity_gb without metrics, so no event is
        emitted there. Call this once from _launch_node immediately after pool
        construction so the chart code has a storage data point even for
        nodes that never cross the expansion trigger.
        """
        metrics.storage_pool_opened(t, self.node_id, self.pool_capacity_gb)

    def job_start(self, t: float, job_id: str, workspace_gb: float,
                  metrics: "MetricsCollector") -> None:
        """Allocate thin LV for a starting job; expand pool if threshold crossed."""
        self.pool_committed_gb += workspace_gb
        self._maybe_expand(t, metrics)

    def job_exit(self, t: float, job_id: str, workspace_gb: float,
                 metrics: "MetricsCollector") -> None:
        """Release thin LV when a job completes or crashes.

        t/job_id/metrics are unused here (cost accrual is lazy, driven by
        pool_capacity_gb and close_time, not per-exit events) but accepted so
        every pool class shares one job_exit protocol -- needed for
        DedicatedVolumePool (BSIM-128), which is selectable for any scheduler
        and does need them.
        """
        self.pool_committed_gb = max(0.0, self.pool_committed_gb - workspace_gb)

    def _maybe_expand(self, t: float, metrics: "MetricsCollector") -> None:
        trigger = self.config.expansion_trigger_pct * self.pool_capacity_gb
        while self.pool_committed_gb > trigger:
            if self.attached_volumes >= self.instance.max_ebs_volumes:
                metrics.storage_exhausted(t, self.node_id,
                                          self.pool_committed_gb, self.pool_capacity_gb)
                return
            old_gb = self.pool_capacity_gb
            self.attached_volumes += 1
            self.pool_capacity_gb += self.config.volume_size_gb
            trigger = self.config.expansion_trigger_pct * self.pool_capacity_gb
            metrics.storage_pool_expanded(t, self.node_id, old_gb, self.pool_capacity_gb,
                                          self.pool_committed_gb,
                                          self.config.expansion_trigger_pct)

    def close(self, t: float) -> None:
        """Mark the pool closed (node terminated) to stop cost accrual."""
        if self._close_time < 0:
            self._close_time = t

    @property
    def storage_cost_usd(self) -> float:
        """Cost billed on capacity (not commitment) for the node's lifetime."""
        if self._close_time < 0:
            return 0.0
        lifetime_h = (self._close_time - self.open_time) / 3600.0
        return self.pool_capacity_gb * lifetime_h * self.config.ebs_price_per_gb_hour


@dataclass
class _PoolGeneration:
    gen_id: int
    capacity_gb: float
    open_time: float
    ebs_price_per_gb_hour: float
    committed_gb: float = 0.0
    active_jobs: int = 0
    close_time: float = -1.0   # wall-clock time of STORAGE_GEN_RELEASED

    @property
    def is_closed(self) -> bool:
        return self.close_time >= 0

    @property
    def cost_usd(self) -> float:
        if self.close_time < 0:
            return 0.0
        return self.capacity_gb * (self.close_time - self.open_time) / 3600.0 * self.ebs_price_per_gb_hour


@dataclass
class GenerationalStoragePool:
    """BSIM-93: Multi-generation thin pool for K8S — bounds stranded capacity."""
    node_id: str
    config: "StoragePoolConfig"
    instance: "InstanceTypeConfig"
    open_time: float

    _generations: list[_PoolGeneration] = field(init=False, default_factory=list)
    _job_gen: dict[str, int] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self._open_generation(self.open_time)

    def _open_generation(self, t: float, trigger_committed_pct: float = 0.0,
                          metrics: "MetricsCollector | None" = None) -> _PoolGeneration:
        gen_id = len(self._generations)
        cap = self.config.initial_volume_count * self.config.volume_size_gb
        gen = _PoolGeneration(gen_id=gen_id, capacity_gb=cap,
                              open_time=t, ebs_price_per_gb_hour=self.config.ebs_price_per_gb_hour)
        self._generations.append(gen)
        if metrics is not None:
            metrics.storage_gen_opened(t, self.node_id, gen_id, cap, trigger_committed_pct)
        return gen

    @property
    def _current_gen(self) -> _PoolGeneration:
        return self._generations[-1]

    @property
    def _open_generation_count(self) -> int:
        return sum(1 for g in self._generations if not g.is_closed)

    def has_room_for(self, workspace_gb: float) -> bool:
        """BSIM-127: room exists either within the current generation's trigger,
        or -- if admitting this job would require opening a new generation --
        if the node has not yet reached its total EBS attachment ceiling across
        all currently-open generations (each generation holds initial_volume_count
        volumes for as long as it stays open)."""
        gen = self._current_gen
        trigger = self.config.expansion_trigger_pct * gen.capacity_gb
        if gen.committed_gb + workspace_gb <= trigger:
            return True
        max_open_generations = self.instance.max_ebs_volumes // self.config.initial_volume_count
        return self._open_generation_count < max_open_generations

    def job_start(self, t: float, job_id: str, workspace_gb: float,
                  metrics: "MetricsCollector") -> None:
        """Assign job to current generation; open a new generation if threshold would be crossed."""
        gen = self._current_gen
        trigger = self.config.expansion_trigger_pct * gen.capacity_gb
        if gen.committed_gb + workspace_gb > trigger:
            # Close current gen to new placements, open a fresh one
            committed_pct = gen.committed_gb / gen.capacity_gb if gen.capacity_gb > 0 else 0.0
            gen = self._open_generation(t, committed_pct, metrics)
        gen.committed_gb += workspace_gb
        gen.active_jobs += 1
        self._job_gen[job_id] = gen.gen_id

    def job_exit(self, t: float, job_id: str, workspace_gb: float,
                 metrics: "MetricsCollector") -> None:
        """Release thin LV; close the generation if its last job has exited."""
        gen_id = self._job_gen.pop(job_id, None)
        if gen_id is None:
            return
        gen = self._generations[gen_id]
        gen.committed_gb = max(0.0, gen.committed_gb - workspace_gb)
        gen.active_jobs -= 1
        if gen.active_jobs == 0 and not gen.is_closed:
            gen.close_time = t
            metrics.storage_gen_released(
                t, self.node_id, gen.gen_id, gen.capacity_gb,
                gen.close_time - gen.open_time,
                sum(1 for v in self._job_gen.values() if v == gen_id) + 1,
            )

    def announce(self, t: float, metrics: "MetricsCollector") -> None:
        """Emit the STORAGE_GEN_OPENED event for generation 0.

        __post_init__ opens gen 0 without metrics, so no event is emitted there.
        Call this once from _launch_node immediately after pool construction so the
        chart code can see the initial capacity from node-launch time onwards.
        """
        gen = self._generations[0]
        metrics.storage_gen_opened(t, self.node_id, gen.gen_id, gen.capacity_gb, 0.0)

    def close(self, t: float, metrics: "MetricsCollector | None" = None) -> None:
        """Force-close any still-open generation when the node terminates.

        Jobs still running at node termination never reach job_exit's
        last-job-departs path, so without this their generation's close_time
        (correct for cost accrual) would never be paired with a
        STORAGE_GEN_RELEASED event -- leaving chart code that aggregates
        capacity purely from open/release events to see it as still open
        forever, plateauing instead of dropping at node termination.
        """
        for gen in self._generations:
            if not gen.is_closed:
                gen.close_time = t
                if metrics is not None:
                    metrics.storage_gen_released(
                        t, self.node_id, gen.gen_id, gen.capacity_gb,
                        gen.close_time - gen.open_time, gen.active_jobs,
                    )

    @property
    def storage_cost_usd(self) -> float:
        return sum(g.cost_usd for g in self._generations)

    @property
    def pool_capacity_gb(self) -> float:
        """Current total capacity across all open generations."""
        return sum(g.capacity_gb for g in self._generations if not g.is_closed)


@dataclass
class DedicatedVolumePool:
    """BSIM-128: per-job dedicated volumes -- each job gets its own volume sized
    to its own workspace_gb, attached at job_start and detached at job_exit.
    No thin-pool or generation/overlap bookkeeping: every volume is always
    exactly as committed as it is large, by construction, so there is no
    expansion, no trigger, and no stranded capacity to track. Node concurrency
    is bounded directly by max_ebs_volumes via has_room_for(), enforced the
    same way as the other two pool models.

    Selectable for any scheduler via storage.model: dedicated, for head-to-head
    comparison against that scheduler's default pool abstraction.
    """
    node_id: str
    config: "StoragePoolConfig"
    instance: "InstanceTypeConfig"
    open_time: float

    _active: dict = field(init=False, default_factory=dict)   # job_id -> (open_t, size_gb)
    _closed_cost_usd: float = field(init=False, default=0.0)
    _close_time: float = field(init=False, default=-1.0)

    def has_room_for(self, workspace_gb: float) -> bool:
        return len(self._active) < self.instance.max_ebs_volumes

    def announce(self, t: float, metrics: "MetricsCollector") -> None:
        """No-op: there is no shared initial capacity to announce -- each
        job's volume is its own, sized only once it actually starts."""

    def job_start(self, t: float, job_id: str, workspace_gb: float,
                  metrics: "MetricsCollector") -> None:
        self._active[job_id] = (t, workspace_gb)

    def job_exit(self, t: float, job_id: str, workspace_gb: float,
                 metrics: "MetricsCollector") -> None:
        entry = self._active.pop(job_id, None)
        if entry is None:
            return
        open_t, size_gb = entry
        duration_h = (t - open_t) / 3600.0
        self._closed_cost_usd += size_gb * duration_h * self.config.ebs_price_per_gb_hour

    def close(self, t: float, metrics: "MetricsCollector | None" = None) -> None:
        """Force-close any still-active job volumes when the node terminates,
        accruing their cost up to t (mirrors GenerationalStoragePool.close())."""
        if self._close_time >= 0:
            return
        self._close_time = t
        for job_id, (open_t, size_gb) in list(self._active.items()):
            duration_h = (t - open_t) / 3600.0
            self._closed_cost_usd += size_gb * duration_h * self.config.ebs_price_per_gb_hour
        self._active.clear()

    @property
    def storage_cost_usd(self) -> float:
        return self._closed_cost_usd

    @property
    def pool_capacity_gb(self) -> float:
        """Current total capacity across active dedicated volumes. Always
        exactly equal to pool_committed_gb -- no slack to strand by construction."""
        return sum(size for _, size in self._active.values())

    @property
    def pool_committed_gb(self) -> float:
        return self.pool_capacity_gb


def make_storage_pool(node_id: str, config: "StoragePoolConfig", instance: "InstanceTypeConfig",
                      open_time: float, default_cls: type):
    """BSIM-128: construct the pool class this node should use. config.model
    == DEDICATED always uses DedicatedVolumePool regardless of scheduler;
    POOL (default) uses whichever class the calling scheduler normally
    defaults to (NodeStoragePool for Batch, GenerationalStoragePool for K8S/K8S+)."""
    cls = DedicatedVolumePool if config.model == StorageModel.DEDICATED else default_cls
    return cls(node_id=node_id, config=config, instance=instance, open_time=open_time)
