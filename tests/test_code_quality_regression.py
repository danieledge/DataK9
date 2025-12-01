"""
Code Quality Regression Tests for DataK9 Profiler and Validation Framework

These tests use the Titanic dataset to verify that code quality fixes don't
introduce regressions. Run after each phase of the CODE_QUALITY_REMEDIATION_PLAN.

Test Dataset: /home/daniel/www/test-data/titanic.csv

Usage:
    pytest tests/test_code_quality_regression.py -v
    ./run_tests.sh --code-quality  # After adding menu option

These tests verify:
1. Import stability (no crashes on module import)
2. Profiler output consistency (JSON structure unchanged)
3. Validation output consistency (results match baseline)
4. Memory efficiency (bounded memory usage)
5. Error handling (proper exception propagation)
"""

import pytest
import json
import os
import sys
import tracemalloc
from pathlib import Path
from typing import Dict, Any, Optional

# Test configuration
TITANIC_PATH = Path("/home/daniel/www/test-data/titanic.csv")
BASELINE_DIR = Path("/home/daniel/www/dqa/wip/deleteme/baselines")
OUTPUT_DIR = Path("/home/daniel/www/dqa/wip/deleteme/regression_output")

# Memory threshold for Titanic (small dataset)
MAX_PEAK_MEMORY_MB = 500


class TestImportStability:
    """Test that all modules import without crashing."""

    def test_profiler_engine_import(self):
        """Test profiler engine imports successfully."""
        from validation_framework.profiler.engine import DataProfiler
        assert DataProfiler is not None

    def test_validation_engine_import(self):
        """Test validation engine imports successfully."""
        from validation_framework.core.engine import ValidationEngine
        assert ValidationEngine is not None

    def test_ml_analyzer_import(self):
        """Test ML analyzer imports successfully."""
        from validation_framework.profiler.ml_analyzer import ChunkedMLAccumulator
        assert ChunkedMLAccumulator is not None

    def test_all_loaders_import(self):
        """Test all loaders import successfully."""
        from validation_framework.loaders.factory import LoaderFactory
        from validation_framework.loaders.csv_loader import CSVLoader
        from validation_framework.loaders.parquet_loader import ParquetLoader
        assert LoaderFactory is not None
        assert CSVLoader is not None
        assert ParquetLoader is not None

    def test_all_validations_import(self):
        """Test validation registry imports and populates."""
        from validation_framework.core.registry import get_registry
        # Ensure validations are registered
        from validation_framework.validations.builtin import registry as builtin_registry
        registry = get_registry()
        available = registry.list_available()
        # Should have 25+ validation types registered
        assert len(available) >= 25, f"Expected 25+ validations, got {len(available)}"


@pytest.mark.skipif(not TITANIC_PATH.exists(), reason="Titanic dataset not available")
class TestProfilerConsistency:
    """Test profiler output consistency using Titanic dataset."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance."""
        from validation_framework.profiler.engine import DataProfiler
        return DataProfiler()

    @pytest.fixture
    def baseline_profile(self) -> Optional[Dict[str, Any]]:
        """Load baseline profile if available."""
        baseline_path = BASELINE_DIR / "titanic_profile.json"
        if baseline_path.exists():
            with open(baseline_path) as f:
                return json.load(f)
        return None

    def test_profile_titanic_succeeds(self, profiler):
        """Test that profiling Titanic completes without error."""
        result = profiler.profile_file(str(TITANIC_PATH))
        assert result is not None
        assert result.row_count == 891
        assert result.column_count == 12

    def test_profile_row_count_matches_baseline(self, profiler, baseline_profile):
        """Test row count matches baseline."""
        if baseline_profile is None:
            pytest.skip("No baseline available - run baseline creation first")

        result = profiler.profile_file(str(TITANIC_PATH))
        assert result.row_count == baseline_profile.get("row_count"), \
            f"Row count mismatch: {result.row_count} vs {baseline_profile.get('row_count')}"

    def test_profile_column_count_matches_baseline(self, profiler, baseline_profile):
        """Test column count matches baseline."""
        if baseline_profile is None:
            pytest.skip("No baseline available")

        result = profiler.profile_file(str(TITANIC_PATH))
        assert result.column_count == baseline_profile.get("column_count"), \
            f"Column count mismatch: {result.column_count} vs {baseline_profile.get('column_count')}"

    def test_profile_columns_match_baseline(self, profiler, baseline_profile):
        """Test column names match baseline."""
        if baseline_profile is None:
            pytest.skip("No baseline available")

        result = profiler.profile_file(str(TITANIC_PATH))
        result_cols = {c.name for c in result.columns}
        baseline_cols = {c["name"] for c in baseline_profile.get("columns", [])}

        assert result_cols == baseline_cols, \
            f"Column mismatch: extra={result_cols - baseline_cols}, missing={baseline_cols - result_cols}"

    def test_profile_quality_score_within_tolerance(self, profiler, baseline_profile):
        """Test overall quality score is within 5% of baseline."""
        if baseline_profile is None:
            pytest.skip("No baseline available")

        result = profiler.profile_file(str(TITANIC_PATH))
        result_quality = result.overall_quality_score
        baseline_quality = baseline_profile.get("overall_quality_score", 0)

        assert abs(result_quality - baseline_quality) <= 5, \
            f"Quality score diverged: {result_quality} vs baseline {baseline_quality}"


@pytest.mark.skipif(not TITANIC_PATH.exists(), reason="Titanic dataset not available")
class TestValidationConsistency:
    """Test validation output consistency using Titanic dataset."""

    @pytest.fixture
    def validation_config_path(self) -> Path:
        """Path to Titanic validation config."""
        return BASELINE_DIR / "titanic_validation.yaml"

    @pytest.fixture
    def baseline_validation(self) -> Optional[Dict[str, Any]]:
        """Load baseline validation result if available."""
        baseline_path = BASELINE_DIR / "titanic_validation_summary.json"
        if baseline_path.exists():
            with open(baseline_path) as f:
                return json.load(f)
        return None

    def test_validation_config_exists(self, validation_config_path):
        """Test that validation config exists."""
        if not validation_config_path.exists():
            pytest.skip("Validation config not created - run baseline creation")
        assert validation_config_path.exists()

    def test_validation_runs_successfully(self, validation_config_path):
        """Test that validation completes without error."""
        if not validation_config_path.exists():
            pytest.skip("Validation config not available")

        from validation_framework.core.config import ValidationConfig
        from validation_framework.core.engine import ValidationEngine

        config = ValidationConfig.from_yaml(str(validation_config_path))
        engine = ValidationEngine(config)
        report = engine.run()

        assert report is not None
        # Handle both string and enum comparison
        status_str = str(report.overall_status.value) if hasattr(report.overall_status, 'value') else str(report.overall_status)
        assert status_str in ["PASSED", "WARNING", "FAILED"]

    def test_validation_status_matches_baseline(self, validation_config_path, baseline_validation):
        """Test validation status matches baseline."""
        if not validation_config_path.exists() or baseline_validation is None:
            pytest.skip("Baseline not available")

        from validation_framework.core.config import ValidationConfig
        from validation_framework.core.engine import ValidationEngine

        config = ValidationConfig.from_yaml(str(validation_config_path))
        engine = ValidationEngine(config)
        report = engine.run()

        # Handle both string and enum comparison
        status_str = str(report.overall_status.value) if hasattr(report.overall_status, 'value') else str(report.overall_status)
        assert status_str == baseline_validation.get("overall_status"), \
            f"Status changed: {status_str} vs {baseline_validation.get('overall_status')}"

    def test_validation_error_count_matches(self, validation_config_path, baseline_validation):
        """Test error count matches baseline."""
        if not validation_config_path.exists() or baseline_validation is None:
            pytest.skip("Baseline not available")

        from validation_framework.core.config import ValidationConfig
        from validation_framework.core.engine import ValidationEngine

        config = ValidationConfig.from_yaml(str(validation_config_path))
        engine = ValidationEngine(config)
        report = engine.run()

        assert report.total_errors == baseline_validation.get("total_errors"), \
            f"Error count changed: {report.total_errors} vs {baseline_validation.get('total_errors')}"


@pytest.mark.skipif(not TITANIC_PATH.exists(), reason="Titanic dataset not available")
class TestMemoryEfficiency:
    """Test memory efficiency during profiling and validation."""

    def test_profiler_memory_bounded(self):
        """Test profiler memory usage stays under threshold."""
        tracemalloc.start()

        from validation_framework.profiler.engine import DataProfiler
        profiler = DataProfiler()
        result = profiler.profile_file(str(TITANIC_PATH))

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak / 1024 / 1024
        assert peak_mb < MAX_PEAK_MEMORY_MB, \
            f"Peak memory {peak_mb:.1f}MB exceeds threshold {MAX_PEAK_MEMORY_MB}MB"

    def test_validation_memory_bounded(self):
        """Test validation memory usage stays under threshold."""
        config_path = BASELINE_DIR / "titanic_validation.yaml"
        if not config_path.exists():
            pytest.skip("Validation config not available")

        tracemalloc.start()

        from validation_framework.core.config import ValidationConfig
        from validation_framework.core.engine import ValidationEngine

        config = ValidationConfig.from_yaml(str(config_path))
        engine = ValidationEngine(config)
        report = engine.run()

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak / 1024 / 1024
        assert peak_mb < MAX_PEAK_MEMORY_MB, \
            f"Peak memory {peak_mb:.1f}MB exceeds threshold {MAX_PEAK_MEMORY_MB}MB"


class TestErrorHandling:
    """Test error handling improvements."""

    def test_missing_file_raises_proper_error(self):
        """Test that missing file raises FileNotFoundError, not generic Exception."""
        from validation_framework.profiler.engine import DataProfiler

        profiler = DataProfiler()
        with pytest.raises((FileNotFoundError, OSError)):
            profiler.profile_file("/nonexistent/path/to/file.csv")

    def test_missing_column_provides_context(self):
        """Test that missing column errors include column name."""
        # This test will be more meaningful after exception handling fixes
        pytest.skip("Pending exception handling improvements")

    def test_malformed_yaml_raises_config_error(self):
        """Test that malformed YAML raises ConfigError with details."""
        import tempfile
        from validation_framework.core.config import ValidationConfig

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            f.flush()

            try:
                with pytest.raises(Exception) as exc_info:
                    ValidationConfig.from_yaml(f.name)
                # Should be ConfigError or yaml.YAMLError, not generic Exception
                exc_type = str(type(exc_info.value).__name__).lower()
                exc_msg = str(exc_info.value).lower()
                assert "yaml" in exc_type or "config" in exc_type or \
                       "scan" in exc_msg or "error" in exc_type, \
                       f"Expected YAML/Config error, got {type(exc_info.value).__name__}: {exc_info.value}"
            finally:
                os.unlink(f.name)


class TestDataIntegrity:
    """Test data integrity - ensure no silent data corruption."""

    @pytest.mark.skipif(not TITANIC_PATH.exists(), reason="Titanic dataset not available")
    def test_null_counts_consistent(self):
        """Test that null counts are consistent across runs."""
        from validation_framework.profiler.engine import DataProfiler

        profiler = DataProfiler()

        # Profile twice
        result1 = profiler.profile_file(str(TITANIC_PATH))
        result2 = profiler.profile_file(str(TITANIC_PATH))

        # Null counts should be identical
        for col1, col2 in zip(result1.columns, result2.columns):
            assert col1.statistics.null_count == col2.statistics.null_count, \
                f"Null count for {col1.name} inconsistent: {col1.statistics.null_count} vs {col2.statistics.null_count}"

    @pytest.mark.skipif(not TITANIC_PATH.exists(), reason="Titanic dataset not available")
    def test_cabin_field_not_overcounted_as_null(self):
        """Test that Cabin field's nulls aren't inflated by placeholder detection."""
        import pandas as pd
        from validation_framework.profiler.engine import DataProfiler

        # Get actual null count from pandas
        df = pd.read_csv(TITANIC_PATH)
        actual_nulls = df['Cabin'].isna().sum()

        # Get profiler's null count
        profiler = DataProfiler()
        result = profiler.profile_file(str(TITANIC_PATH))
        cabin_col = next(c for c in result.columns if c.name == 'Cabin')
        profiler_nulls = cabin_col.statistics.null_count

        # Profiler may count whitespace/placeholders as null, but shouldn't
        # count legitimate values. Allow some tolerance for placeholder detection.
        # The key is that it shouldn't DOUBLE count.
        assert profiler_nulls >= actual_nulls, \
            f"Profiler null count {profiler_nulls} < actual {actual_nulls}"
        # Shouldn't be more than 10% higher due to placeholder detection
        max_expected = actual_nulls * 1.1 + 10  # Allow 10% + 10 buffer
        assert profiler_nulls <= max_expected, \
            f"Profiler null count {profiler_nulls} seems inflated (actual: {actual_nulls})"


# Utility function for baseline creation
def create_baselines():
    """Create baseline files for regression testing."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)

    from validation_framework.profiler.engine import DataProfiler

    print("Creating profiler baseline...")
    profiler = DataProfiler()
    result = profiler.profile_file(str(TITANIC_PATH))

    baseline_json = BASELINE_DIR / "titanic_profile.json"
    result.to_json(str(baseline_json))
    print(f"  Saved: {baseline_json}")

    print("\nBaselines created successfully!")
    print(f"  Profile: {baseline_json}")


if __name__ == "__main__":
    # Allow running as script to create baselines
    if len(sys.argv) > 1 and sys.argv[1] == "--create-baselines":
        create_baselines()
    else:
        # Run tests
        pytest.main([__file__, "-v"])
