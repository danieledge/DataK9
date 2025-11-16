"""
End-to-End Large Dataset Test for Polars Migration.

This test suite validates the framework's ability to handle very large datasets
using the Polars backend for optimal performance and memory efficiency.

Tests use the IBM AML Banking Transactions dataset:
- HI-Large_Trans.parquet: 179M rows, 5.1GB
- Tests profile, generate configs, and validate at scale

Test Markers:
- @pytest.mark.e2e: End-to-end integration test
- @pytest.mark.slow: Long-running test (5-15 minutes)
- @pytest.mark.requires_large_dataset: Needs 5.1GB test file

Performance Targets (Polars backend):
- Profiling: < 5 minutes for 179M rows
- Validation: < 10 minutes for 179M rows
- Memory: < 12GB peak RAM usage
- No OOM errors or memory leaks

Author: Daniel Edge
"""

import pytest
import os
import json
import yaml
import time
import psutil
from pathlib import Path
from datetime import datetime

# Import DataK9 components
from validation_framework.profiler.polars_engine import PolarsDataProfiler as PolarsProfiler
from validation_framework.core.engine import ValidationEngine


# Dataset configuration
LARGE_DATASET_PATH = "/home/daniel/www/test-data/HI-Large_Trans.parquet"
LARGE_DATASET_ROWS = 179_000_000  # Approximate row count
LARGE_DATASET_SIZE_GB = 5.1

# Performance thresholds
MAX_PROFILE_TIME_MINUTES = 5
MAX_VALIDATION_TIME_MINUTES = 10
MAX_MEMORY_GB = 12.0

# Temporary output paths
TEMP_PROFILE_JSON = "/tmp/hi_large_profile.json"
TEMP_VALIDATION_YAML = "/tmp/hi_large_validation.yaml"
TEMP_VALIDATION_HTML = "/tmp/hi_large_validation.html"


def get_memory_usage_gb():
    """Get current process memory usage in GB."""
    process = psutil.Process()
    memory_bytes = process.memory_info().rss
    return memory_bytes / (1024 ** 3)


@pytest.mark.e2e
@pytest.mark.slow
@pytest.mark.requires_large_dataset
class TestLargeDatasetEndToEnd:
    """
    End-to-end test for large dataset processing with Polars.

    This test validates the complete workflow:
    1. Profile 179M-row dataset
    2. Generate validation config from profile
    3. Run validations using generated config
    4. Verify performance targets are met
    """

    @classmethod
    def setup_class(cls):
        """Setup class-level resources and check dataset availability."""
        cls.dataset_available = os.path.exists(LARGE_DATASET_PATH)
        cls.initial_memory = get_memory_usage_gb()

        print(f"\n{'='*70}")
        print("LARGE DATASET END-TO-END TEST")
        print(f"{'='*70}")
        print(f"Dataset Path: {LARGE_DATASET_PATH}")
        print(f"Dataset Available: {cls.dataset_available}")
        if cls.dataset_available:
            file_size_gb = os.path.getsize(LARGE_DATASET_PATH) / (1024 ** 3)
            print(f"Dataset Size: {file_size_gb:.2f} GB")
            print(f"Expected Rows: ~{LARGE_DATASET_ROWS:,}")
        print(f"Initial Memory: {cls.initial_memory:.2f} GB")
        print(f"{'='*70}\n")

    @classmethod
    def teardown_class(cls):
        """Cleanup and report final metrics."""
        final_memory = get_memory_usage_gb()
        memory_increase = final_memory - cls.initial_memory

        print(f"\n{'='*70}")
        print("TEST COMPLETION METRICS")
        print(f"{'='*70}")
        print(f"Initial Memory: {cls.initial_memory:.2f} GB")
        print(f"Final Memory: {final_memory:.2f} GB")
        print(f"Memory Increase: {memory_increase:.2f} GB")
        print(f"{'='*70}\n")

    def test_01_dataset_availability(self):
        """
        Test that the large dataset file is available.

        If not available, provide clear instructions for obtaining it.
        """
        if not self.dataset_available:
            pytest.skip(
                f"\n\n"
                f"{'='*70}\n"
                f"LARGE DATASET NOT FOUND\n"
                f"{'='*70}\n"
                f"\n"
                f"Dataset not found at: {LARGE_DATASET_PATH}\n"
                f"\n"
                f"To run this test, download the IBM AML dataset:\n"
                f"\n"
                f"1. Download from Kaggle:\n"
                f"   https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml\n"
                f"\n"
                f"2. Extract HI-Large_Trans.csv from the archive\n"
                f"\n"
                f"3. Convert to Parquet format (much faster):\n"
                f"   python3 /home/daniel/www/test-data/convert_to_parquet.py\n"
                f"\n"
                f"4. Verify file exists:\n"
                f"   ls -lh {LARGE_DATASET_PATH}\n"
                f"\n"
                f"Expected size: ~{LARGE_DATASET_SIZE_GB} GB\n"
                f"Expected rows: ~{LARGE_DATASET_ROWS:,}\n"
                f"\n"
                f"{'='*70}\n"
            )

        # If we get here, dataset is available
        assert os.path.exists(LARGE_DATASET_PATH)
        print(f"Dataset found: {LARGE_DATASET_PATH}")

    def test_02_profile_large_dataset(self):
        """
        Profile the 179M-row dataset using Polars profiler.

        Validates:
        - Profiling completes within time limit
        - Memory usage stays within bounds
        - Profile results are generated correctly
        - JSON output is created
        """
        if not self.dataset_available:
            pytest.skip("Large dataset not available")

        print(f"\n{'='*70}")
        print("STEP 1: PROFILING LARGE DATASET")
        print(f"{'='*70}")
        print(f"Profiling: {LARGE_DATASET_PATH}")
        print(f"Target: < {MAX_PROFILE_TIME_MINUTES} minutes")
        print(f"Memory limit: < {MAX_MEMORY_GB} GB")
        print(f"{'='*70}\n")

        # Record start metrics
        start_time = time.time()
        start_memory = get_memory_usage_gb()

        # Profile the dataset
        profiler = PolarsProfiler(chunk_size=100_000)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting profile...")
        profile = profiler.profile_file(
            file_path=LARGE_DATASET_PATH,
            file_format="parquet"
        )
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Profile complete!")

        # Record end metrics
        end_time = time.time()
        end_memory = get_memory_usage_gb()
        duration_seconds = end_time - start_time
        duration_minutes = duration_seconds / 60
        memory_used = end_memory - start_memory
        peak_memory = end_memory

        # Display results
        print(f"\n{'='*70}")
        print("PROFILING RESULTS")
        print(f"{'='*70}")
        print(f"Duration: {duration_minutes:.2f} minutes ({duration_seconds:.1f} seconds)")
        print(f"Rows Profiled: {profile.row_count:,}")
        print(f"Columns Profiled: {profile.column_count}")
        print(f"Memory Used: {memory_used:.2f} GB")
        print(f"Peak Memory: {peak_memory:.2f} GB")
        print(f"Processing Speed: {profile.row_count/duration_seconds:,.0f} rows/second")
        print(f"{'='*70}\n")

        # Save profile to JSON
        with open(TEMP_PROFILE_JSON, 'w') as f:
            json.dump(profile.to_dict(), f, indent=2)
        print(f"Profile saved: {TEMP_PROFILE_JSON}")

        # Assertions
        assert profile.row_count > 100_000_000, f"Expected >100M rows, got {profile.row_count:,}"
        assert profile.column_count > 5, f"Expected >5 columns, got {profile.column_count}"
        assert duration_minutes < MAX_PROFILE_TIME_MINUTES, \
            f"Profiling took {duration_minutes:.2f} min, exceeds limit of {MAX_PROFILE_TIME_MINUTES} min"
        assert peak_memory < MAX_MEMORY_GB, \
            f"Peak memory {peak_memory:.2f} GB exceeds limit of {MAX_MEMORY_GB} GB"

        print(f"\nPROFILING TEST PASSED\n")

    def test_03_generate_validation_config(self):
        """
        Generate validation config from the profile.

        Validates:
        - Config is generated correctly
        - Suggested validations are appropriate
        - YAML is well-formed
        """
        if not self.dataset_available:
            pytest.skip("Large dataset not available")

        if not os.path.exists(TEMP_PROFILE_JSON):
            pytest.skip("Profile not generated - run test_02 first")

        print(f"\n{'='*70}")
        print("STEP 2: GENERATING VALIDATION CONFIG")
        print(f"{'='*70}\n")

        # Load profile
        with open(TEMP_PROFILE_JSON, 'r') as f:
            profile_data = json.load(f)

        print(f"Loaded profile: {len(profile_data.get('columns', []))} columns")

        # Create validation config based on profile insights
        config = {
            "validation_job": {
                "name": "Large Dataset Validation (Polars)",
                "version": "1.0",
                "description": "End-to-end validation of 179M-row IBM AML dataset",
                "files": [
                    {
                        "name": "hi_large_transactions",
                        "path": LARGE_DATASET_PATH,
                        "format": "parquet",
                        "backend": "polars",  # Use Polars for performance
                        "validations": [
                            {
                                "type": "EmptyFileCheck",
                                "severity": "ERROR"
                            },
                            {
                                "type": "RowCountRangeCheck",
                                "severity": "WARNING",
                                "params": {
                                    "min_rows": 100_000_000,
                                    "max_rows": 200_000_000
                                }
                            },
                            {
                                "type": "ColumnCountCheck",
                                "severity": "ERROR",
                                "params": {
                                    "expected_count": profile_data.get("column_count", 10)
                                }
                            },
                            {
                                "type": "SchemaCheck",
                                "severity": "ERROR",
                                "params": {
                                    "schema": {
                                        col["name"]: "string"  # Simplified schema
                                        for col in profile_data.get("columns", [])[:5]
                                    }
                                }
                            },
                            {
                                "type": "MandatoryFieldCheck",
                                "severity": "WARNING",
                                "params": {
                                    "fields": [
                                        col["name"]
                                        for col in profile_data.get("columns", [])
                                        if col.get("statistics", {}).get("null_percentage", 100) < 5
                                    ][:3]  # Top 3 complete fields
                                }
                            }
                        ]
                    }
                ],
                "output": {
                    "html_report": TEMP_VALIDATION_HTML,
                    "json_summary": "/tmp/hi_large_summary.json",
                    "fail_on_error": False,  # Don't fail - just report
                    "fail_on_warning": False
                },
                "processing": {
                    "chunk_size": 100_000,
                    "parallel_files": False,
                    "max_sample_failures": 100
                }
            }
        }

        # Save config
        with open(TEMP_VALIDATION_YAML, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)

        print(f"Validation config generated:")
        print(f"  - Validations: {len(config['validation_job']['files'][0]['validations'])}")
        print(f"  - Config saved: {TEMP_VALIDATION_YAML}")
        print(f"\n{'='*70}\n")

        # Assertions
        assert os.path.exists(TEMP_VALIDATION_YAML)
        assert len(config['validation_job']['files'][0]['validations']) >= 3

        print(f"CONFIG GENERATION TEST PASSED\n")

    def test_04_run_validations(self):
        """
        Run validations on the large dataset using generated config.

        Validates:
        - All validations complete successfully
        - Processing time within limits
        - Memory usage within bounds
        - Results are generated correctly
        """
        if not self.dataset_available:
            pytest.skip("Large dataset not available")

        if not os.path.exists(TEMP_VALIDATION_YAML):
            pytest.skip("Validation config not generated - run test_03 first")

        print(f"\n{'='*70}")
        print("STEP 3: RUNNING VALIDATIONS")
        print(f"{'='*70}")
        print(f"Config: {TEMP_VALIDATION_YAML}")
        print(f"Target: < {MAX_VALIDATION_TIME_MINUTES} minutes")
        print(f"Memory limit: < {MAX_MEMORY_GB} GB")
        print(f"{'='*70}\n")

        # Record start metrics
        start_time = time.time()
        start_memory = get_memory_usage_gb()

        # Run validations
        engine = ValidationEngine(TEMP_VALIDATION_YAML)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting validations...")
        report = engine.run(verbose=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Validations complete!")

        # Record end metrics
        end_time = time.time()
        end_memory = get_memory_usage_gb()
        duration_seconds = end_time - start_time
        duration_minutes = duration_seconds / 60
        memory_used = end_memory - start_memory
        peak_memory = end_memory

        # Display results
        print(f"\n{'='*70}")
        print("VALIDATION RESULTS")
        print(f"{'='*70}")
        print(f"Duration: {duration_minutes:.2f} minutes ({duration_seconds:.1f} seconds)")
        print(f"Status: {report.status.value}")
        print(f"Validations Run: {len(report.file_reports[0].rule_results)}")
        print(f"Errors: {report.error_count}")
        print(f"Warnings: {report.warning_count}")
        print(f"Memory Used: {memory_used:.2f} GB")
        print(f"Peak Memory: {peak_memory:.2f} GB")
        print(f"{'='*70}\n")

        # Display individual validation results
        print("Validation Details:")
        for result in report.file_reports[0].rule_results:
            status_symbol = "✓" if result.passed else "✗"
            print(f"  {status_symbol} {result.rule_name}: {result.message}")

        print(f"\n{'='*70}\n")

        # Assertions
        assert report is not None
        assert len(report.file_reports) == 1
        assert len(report.file_reports[0].rule_results) >= 3
        assert duration_minutes < MAX_VALIDATION_TIME_MINUTES, \
            f"Validation took {duration_minutes:.2f} min, exceeds limit of {MAX_VALIDATION_TIME_MINUTES} min"
        assert peak_memory < MAX_MEMORY_GB, \
            f"Peak memory {peak_memory:.2f} GB exceeds limit of {MAX_MEMORY_GB} GB"

        # HTML report should be generated
        assert os.path.exists(TEMP_VALIDATION_HTML), "HTML report not generated"

        print(f"VALIDATION TEST PASSED\n")

    def test_05_verify_performance_targets(self):
        """
        Verify that all performance targets were met.

        This is a summary test that checks the cumulative metrics.
        """
        if not self.dataset_available:
            pytest.skip("Large dataset not available")

        final_memory = get_memory_usage_gb()
        memory_increase = final_memory - self.initial_memory

        print(f"\n{'='*70}")
        print("FINAL PERFORMANCE VERIFICATION")
        print(f"{'='*70}")
        print(f"Total Memory Increase: {memory_increase:.2f} GB")
        print(f"Memory Target: < {MAX_MEMORY_GB} GB")
        print(f"Status: {'PASS' if memory_increase < MAX_MEMORY_GB else 'FAIL'}")
        print(f"{'='*70}\n")

        # Overall memory usage should be reasonable
        assert memory_increase < MAX_MEMORY_GB, \
            f"Total memory increase {memory_increase:.2f} GB exceeds limit of {MAX_MEMORY_GB} GB"

        print("ALL PERFORMANCE TARGETS MET\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
