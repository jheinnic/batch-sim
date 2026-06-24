"""Batch proportional-CFS CPU distribution — iterative accumulation bug fix.

The pre-fix loop overwrote each unsaturated job's boost_alloc every round
with that round's share of the leftover pool, instead of adding to what it
had already accumulated in prior rounds. Any job needing more than one round
to saturate (or never saturating) lost its earlier rounds' allocation,
stranding real, unused node capacity that nobody actually wanted.
"""
import pytest

from batch_sim.scheduler.cpu_boost_integration import (
    _BatchJob, _distribute_proportional_cfs,
)


def _job(soft, threads):
    return _BatchJob(job_id=f"{soft}-{threads}", soft_cpu=soft,
                      stage_threads=threads, io_wait=0.0)


class TestAccumulationAcrossRounds:
    def test_wham_light_mix_fully_utilises_node(self):
        # The exact scenario found in a real chart: two low-share, high-
        # ceiling jobs (WHAM) and two higher-share, low-ceiling jobs
        # (LIGHT) on a 32-vCPU node. Correct water-filling: everyone lands
        # at 8, fully using the node. The pre-fix bug left WHAM at 2.667
        # each (10.67 vCPU stranded, unused, even though WHAM's ceiling of
        # 16 was nowhere near reached).
        wham1, wham2 = _job(1, 16), _job(1, 16)
        light1, light2 = _job(2, 8), _job(2, 8)
        jobs = [wham1, wham2, light1, light2]
        _distribute_proportional_cfs(jobs, node_physical_vcpu=32)

        assert wham1.boost_alloc == pytest.approx(8.0)
        assert wham2.boost_alloc == pytest.approx(8.0)
        assert light1.boost_alloc == pytest.approx(8.0)
        assert light2.boost_alloc == pytest.approx(8.0)
        assert sum(j.boost_alloc for j in jobs) == pytest.approx(32.0)

    def test_three_round_saturation_accumulates_correctly(self):
        # Graduated ceilings force three separate rounds to resolve; each
        # surviving job's allocation must be the SUM of every round's
        # share it received, not just the last round's.
        a = _job(1, 2)    # saturates round 1
        b = _job(1, 5)    # saturates round 2
        c = _job(1, 100)  # never saturates, absorbs everything left
        jobs = [a, b, c]
        _distribute_proportional_cfs(jobs, node_physical_vcpu=30)

        assert a.boost_alloc == pytest.approx(2.0)
        assert b.boost_alloc == pytest.approx(5.0)
        assert c.boost_alloc == pytest.approx(23.0)
        assert sum(j.boost_alloc for j in jobs) == pytest.approx(30.0)


class TestNoContention:
    def test_nobody_saturates_full_capacity_still_distributed(self):
        # Every job fits comfortably under its ceiling on round 1; the
        # node's full physical capacity is still divided proportionally.
        a, b = _job(1, 100), _job(3, 100)
        _distribute_proportional_cfs([a, b], node_physical_vcpu=20)
        assert a.boost_alloc == pytest.approx(5.0)    # 1/4 share
        assert b.boost_alloc == pytest.approx(15.0)   # 3/4 share

    def test_single_job_capped_at_its_own_stage_threads(self):
        job = _job(2, 8)
        _distribute_proportional_cfs([job], node_physical_vcpu=16)
        assert job.boost_alloc == pytest.approx(8.0)   # capped, 8 wasted


class TestIdempotence:
    def test_rerunning_on_same_jobs_resets_boost_alloc(self):
        # The function must reset boost_alloc itself (called fresh at
        # every JOB_START/PHASE_TRANSITION/JOB_COMPLETE event with the
        # same long-lived job objects) rather than accumulate across calls.
        wham1, wham2 = _job(1, 16), _job(1, 16)
        light1, light2 = _job(2, 8), _job(2, 8)
        jobs = [wham1, wham2, light1, light2]
        _distribute_proportional_cfs(jobs, node_physical_vcpu=32)
        _distribute_proportional_cfs(jobs, node_physical_vcpu=32)
        assert wham1.boost_alloc == pytest.approx(8.0)
        assert sum(j.boost_alloc for j in jobs) == pytest.approx(32.0)
