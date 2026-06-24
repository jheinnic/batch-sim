"""BSIM-70: CPU boost solver (Option 2 greedy allocator) — thread-aware auction."""
import pytest

from batch_sim.scheduler.cpu_boost_solver import JobCPUState, solve_cpu_boost


class TestUnconstrainedBehaviorUnchanged:
    """stage_threads == 0 (caller has no per-stage info) must reproduce the
    pre-fix, hard_cpu-only headroom behaviour exactly."""

    def test_single_job_boosts_to_hard_cpu(self):
        job = JobCPUState(job_id="a", soft_cpu=2, hard_cpu=8, io_wait=0.0)
        result = solve_cpu_boost([job], node_physical_vcpu=16)
        assert job.boost_alloc == pytest.approx(8.0)
        assert job.effective_vcpu == pytest.approx(8.0)
        assert result.hard_limit_waste == pytest.approx(8.0)  # 14 unreserved, only 6 used

    def test_two_equal_jobs_split_surplus_in_io_wait_order(self):
        low = JobCPUState(job_id="low", soft_cpu=1, hard_cpu=8, io_wait=0.0)
        high = JobCPUState(job_id="high", soft_cpu=1, hard_cpu=8, io_wait=0.5)
        solve_cpu_boost([low, high], node_physical_vcpu=10)
        # unreserved=8, io_returned_at_soft=1*0+1*0.5=0.5, surplus=8.5
        # low goes first (lower io_wait): grant=min(7, 8.5)=7 -> boost=8
        assert low.boost_alloc == pytest.approx(8.0)
        # remaining=1.5; high: grant=min(7,1.5)=1.5 -> boost=2.5
        assert high.boost_alloc == pytest.approx(2.5)

    def test_physical_overflow_guard_still_applies(self):
        # A solo job's own io_wait soft-return must not inflate boost_alloc
        # past the node's physical vCPU.
        job = JobCPUState(job_id="a", soft_cpu=14, hard_cpu=20, io_wait=0.9)
        solve_cpu_boost([job], node_physical_vcpu=16)
        assert job.boost_alloc <= 16.0 + 1e-9


class TestThreadAwareAuction:
    """The fix: a job cannot win more surplus than its current stage can
    use, and the unused remainder falls through to the next job in line."""

    def test_low_io_wait_job_capped_at_stage_threads_not_hard_cpu(self):
        # "LIGHT": low io_wait so it auctions first, hard_cpu=8 but its
        # current stage can only use 3 total (1 of stage headroom above
        # its soft_cpu=2).
        light = JobCPUState(job_id="light", soft_cpu=2, hard_cpu=8,
                             io_wait=0.0, stage_threads=3)
        # "WHAM": higher io_wait (auctions second) but fully able to use
        # its hard_cpu=16 right now.
        wham = JobCPUState(job_id="wham", soft_cpu=1, hard_cpu=16,
                            io_wait=0.1, stage_threads=16)
        solve_cpu_boost([light, wham], node_physical_vcpu=16)

        # light must not win more than its stage can use, regardless of
        # going first in the auction and having ample hard_cpu headroom.
        assert light.boost_alloc == pytest.approx(3.0)
        # the surplus light couldn't use must flow to wham, not vanish.
        assert wham.boost_alloc > 8.0
        assert wham.boost_alloc == pytest.approx(13.1)

    def test_starved_job_recovers_capacity_thread_capped_job_cannot_use(self):
        # Without the fix, jobA (low io_wait, hard_cpu=8, but stage caps it
        # at 2) would greedily lock up 7 units of surplus it cannot use,
        # leaving jobB starved even though jobB can use up to 8.
        job_a = JobCPUState(job_id="a", soft_cpu=1, hard_cpu=8,
                             io_wait=0.0, stage_threads=2)
        job_b = JobCPUState(job_id="b", soft_cpu=1, hard_cpu=8,
                             io_wait=0.1, stage_threads=8)
        solve_cpu_boost([job_a, job_b], node_physical_vcpu=10)

        assert job_a.boost_alloc == pytest.approx(2.0)   # capped at its stage ceiling
        assert job_b.boost_alloc == pytest.approx(8.0)   # recovers what A couldn't use
        # The solver's own effective_vcpu is now realistic — it never
        # exceeds what engine.py's independent stage_cap clamp would also
        # allow, so the chart and real job progress no longer disagree.
        assert job_a.effective_vcpu <= job_a.stage_threads
        assert job_b.effective_vcpu <= job_b.stage_threads

    def test_unconstrained_stage_threads_sentinel_falls_back_to_hard_cpu(self):
        # stage_threads left at the 0 default must behave exactly like the
        # unconstrained (pre-fix) path, not like "0 usable threads".
        job = JobCPUState(job_id="a", soft_cpu=1, hard_cpu=8, io_wait=0.0)
        assert job.stage_threads == 0
        solve_cpu_boost([job], node_physical_vcpu=10)
        assert job.boost_alloc == pytest.approx(8.0)

    def test_stage_threads_below_soft_cpu_grants_no_boost_but_keeps_floor(self):
        # A job whose current stage is even less parallel than its own
        # soft reservation gets zero additional boost, but soft_cpu is a
        # guaranteed floor that is never taken away.
        job = JobCPUState(job_id="a", soft_cpu=4, hard_cpu=16,
                           io_wait=0.0, stage_threads=1)
        other = JobCPUState(job_id="b", soft_cpu=1, hard_cpu=16,
                             io_wait=0.5, stage_threads=16)
        solve_cpu_boost([job, other], node_physical_vcpu=20)
        assert job.boost_alloc == pytest.approx(4.0)   # soft_cpu floor only
        assert other.boost_alloc > 1.0                 # absorbs what job couldn't use

    def test_no_jobs_with_stage_headroom_leaves_hard_limit_waste(self):
        # If every job is stage-capped at (or below) its soft_cpu, the
        # unusable surplus is genuinely wasted, not silently dropped.
        job_a = JobCPUState(job_id="a", soft_cpu=2, hard_cpu=8,
                             io_wait=0.0, stage_threads=2)
        job_b = JobCPUState(job_id="b", soft_cpu=2, hard_cpu=8,
                             io_wait=0.1, stage_threads=2)
        result = solve_cpu_boost([job_a, job_b], node_physical_vcpu=16)
        assert job_a.boost_alloc == pytest.approx(2.0)
        assert job_b.boost_alloc == pytest.approx(2.0)
        assert result.hard_limit_waste > 0.0
