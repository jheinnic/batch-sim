"""BSIM-75 follow-up: pareto_alpha/download_gb/upload_gb/
preprocess_duration_seconds/preprocess_memory_exponent_{a,b} are required only
where sample_job() actually reads them -- the Pareto-path scalar, or the
no-bin-array fallback for that one specific field -- not unconditionally."""
import numpy as np
import pytest

from batch_sim.core.config_loader import load_simulation_config
from batch_sim.core.schemas import CentroidConfig
from batch_sim.generator.sampler import sample_job


def _base(**overrides):
    kw = dict(
        id="c", label="C", arrival_rate_per_hour=10.0,
        workhorse_cpu_stages=[60.0, 10.0], workhorse_hard_vcpu=[4],
        io_wait_fraction=0.3,
    )
    kw.update(overrides)
    return kw


class TestParetoModeStillRequiresFallbackFields:
    def test_missing_all_six_raises(self):
        with pytest.raises(ValueError, match="pareto_alpha"):
            CentroidConfig(**_base())

    def test_fully_specified_succeeds(self):
        CentroidConfig(**_base(
            pareto_alpha=2.5, download_gb=8.0, upload_gb=2.0,
            preprocess_duration_seconds=30.0,
            preprocess_memory_exponent_a=1.2, preprocess_memory_exponent_b=1.4,
        ))

    def test_error_lists_every_missing_field(self):
        with pytest.raises(ValueError) as exc_info:
            CentroidConfig(**_base())
        msg = str(exc_info.value)
        for name in ("pareto_alpha", "download_gb", "upload_gb",
                     "preprocess_duration_seconds",
                     "preprocess_memory_exponent_a", "preprocess_memory_exponent_b"):
            assert name in msg


class TestFullBinModeOmitsAllSix:
    def test_no_pareto_fields_needed_when_every_bin_array_set(self):
        CentroidConfig(**_base(
            centroid_bin_weights=[1.0, 1.0],
            bin_download_gb=[8.0, 16.0], bin_upload_gb=[2.0, 4.0],
            bin_preprocess_duration_s=[20.0, 30.0],
            bin_preloader_hard_limit_gb=[24.0, 32.0],
        ))


class TestPerFieldGranularity:
    """Each fallback field's requirement is independent -- omitting one
    bin array re-requires only the field it covers, not all six."""

    def test_missing_bin_download_gb_requires_only_download_gb(self):
        with pytest.raises(ValueError) as exc_info:
            CentroidConfig(**_base(
                centroid_bin_weights=[1.0, 1.0],
                bin_upload_gb=[2.0, 4.0],
                bin_preprocess_duration_s=[20.0, 30.0],
                bin_preloader_hard_limit_gb=[24.0, 32.0],
            ))
        msg = str(exc_info.value)
        assert "download_gb" in msg
        assert "upload_gb" not in msg
        assert "preprocess_memory_exponent" not in msg

    def test_missing_bin_preloader_hard_limit_gb_requires_both_exponents(self):
        with pytest.raises(ValueError) as exc_info:
            CentroidConfig(**_base(
                centroid_bin_weights=[1.0, 1.0],
                bin_download_gb=[8.0, 16.0], bin_upload_gb=[2.0, 4.0],
                bin_preprocess_duration_s=[20.0, 30.0],
            ))
        msg = str(exc_info.value)
        assert "preprocess_memory_exponent_a" in msg
        assert "preprocess_memory_exponent_b" in msg
        assert "download_gb" not in msg

    def test_supplying_the_field_directly_also_satisfies_the_validator(self):
        # A field can still be set explicitly even when its bin array is also
        # present -- the validator only objects when BOTH are absent.
        CentroidConfig(**_base(
            centroid_bin_weights=[1.0, 1.0],
            bin_download_gb=[8.0, 16.0], bin_upload_gb=[2.0, 4.0],
            bin_preprocess_duration_s=[20.0, 30.0],
            preprocess_memory_exponent_a=1.2, preprocess_memory_exponent_b=1.4,
        ))


class TestSampleJobIntegration:
    def test_sample_job_works_with_no_pareto_fields_in_full_bin_mode(self):
        centroid = CentroidConfig(**_base(
            centroid_bin_weights=[1.0, 1.0],
            bin_download_gb=[8.0, 16.0], bin_upload_gb=[2.0, 4.0],
            bin_preprocess_duration_s=[20.0, 30.0],
            bin_preloader_hard_limit_gb=[24.0, 32.0],
            bin_steady_state_hard_limit_gb=[4.0, 8.0],
        ))
        rng = np.random.default_rng(42)
        for _ in range(20):
            job = sample_job(centroid, rng, network_bandwidth_mbps=500.0)
            # RAM must come from the bin's hard limit, not a placeholder-derived value
            assert job.profile.preprocess_peak_ram_gb in (24.0, 32.0)


class TestRealFixturesValidate:
    """Regression: the canonical workloads this change was motivated by."""

    def test_jch_centroids_v01_validates_and_loads(self):
        cfg = load_simulation_config("configs/jch_centroids_v01.yaml")
        assert len(cfg.centroids) == 4

    def test_jch_centroids_v02_validates_and_loads(self):
        cfg = load_simulation_config("configs/jch_centroids_v02.yaml")
        assert len(cfg.centroids) == 1
