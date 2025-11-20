"""
Comprehensive tests for SamplingValidationEngine.

This test module covers:
1. ReservoirSampler implementation (simpler version from sampling_engine)
2. SamplingValidationEngine initialization and configuration
3. Backend management and selection
4. Sampled vs full-scan validation execution
5. Confidence interval calculations
6. Error handling and edge cases
7. Report generation with sampling metadata

Author: Daniel Edge
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import time

from validation_framework.core.sampling_engine import (
    ReservoirSampler,
    SamplingValidationEngine,
)
from validation_framework.core.config import ValidationConfig
from validation_framework.core.results import Status, Severity, ValidationResult
from validation_framework.core.backend import DataFrameBackend


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def create_config_dict(job_name, files, chunk_size=50000, max_sample_failures=100):
    """Helper to create a valid ValidationConfig dictionary."""
    return {
        'validation_job': {
            'name': job_name,
            'files': files
        },
        'processing': {
            'chunk_size': chunk_size,
            'max_sample_failures': max_sample_failures
        }
    }


# ============================================================================
# RESERVOIR SAMPLER TESTS (SAMPLING ENGINE VERSION)
# ============================================================================


@pytest.mark.unit
class TestSamplingEngineReservoirSampler:
    """Test suite for ReservoirSampler in sampling_engine module."""

    def test_initialization(self):
        """Test basic sampler initialization."""
        sampler = ReservoirSampler(sample_size=100)

        assert sampler.sample_size == 100
        assert len(sampler.reservoir) == 0
        assert sampler.items_seen == 0

    def test_initialization_with_seed(self):
        """Test initialization with random seed."""
        sampler1 = ReservoirSampler(sample_size=50, random_seed=123)
        sampler2 = ReservoirSampler(sample_size=50, random_seed=123)

        # Add same data
        df = pd.DataFrame({'value': range(500)})
        sampler1.add_chunk(df)
        sampler2.add_chunk(df)

        # Should produce samples of correct size
        sample1 = sampler1.get_sample()
        sample2 = sampler2.get_sample()

        assert len(sample1) == 50
        assert len(sample2) == 50
        # With same seed, samples should be identical
        # Convert to sets for comparison (order may vary)
        set1 = set(sample1['value'].values)
        set2 = set(sample2['value'].values)
        assert set1 == set2, f"Same seed should produce identical samples, got {len(set1 & set2)} common elements"

    def test_add_chunk_simple(self):
        """Test adding a single chunk."""
        sampler = ReservoirSampler(sample_size=20)

        chunk = pd.DataFrame({'id': range(10), 'value': range(10, 20)})
        sampler.add_chunk(chunk)

        assert len(sampler.reservoir) == 10
        assert sampler.items_seen == 10

    def test_add_chunk_fills_reservoir_exactly(self):
        """Test filling reservoir to exact capacity."""
        sampler = ReservoirSampler(sample_size=15)

        chunk = pd.DataFrame({'value': range(15)})
        sampler.add_chunk(chunk)

        assert len(sampler.reservoir) == 15
        assert sampler.items_seen == 15

    def test_add_chunk_exceeds_capacity(self):
        """Test that reservoir doesn't exceed sample_size."""
        sampler = ReservoirSampler(sample_size=10, random_seed=42)

        chunk = pd.DataFrame({'value': range(100)})
        sampler.add_chunk(chunk)

        assert len(sampler.reservoir) == 10
        assert sampler.items_seen == 100

    def test_add_multiple_chunks_sequentially(self):
        """Test adding multiple chunks in sequence."""
        sampler = ReservoirSampler(sample_size=30, random_seed=42)

        for i in range(5):
            chunk = pd.DataFrame({'batch': [i] * 20, 'value': range(20)})
            sampler.add_chunk(chunk)

        assert len(sampler.reservoir) == 30
        assert sampler.items_seen == 100

    def test_get_sample_empty(self):
        """Test get_sample on empty reservoir."""
        sampler = ReservoirSampler(sample_size=10)

        sample = sampler.get_sample()

        assert isinstance(sample, pd.DataFrame)
        assert len(sample) == 0

    def test_get_sample_returns_dataframe(self):
        """Test that get_sample returns proper DataFrame."""
        sampler = ReservoirSampler(sample_size=10, random_seed=42)

        chunk = pd.DataFrame({
            'id': range(50),
            'name': [f'item_{i}' for i in range(50)],
            'value': range(100, 150)
        })
        sampler.add_chunk(chunk)

        sample = sampler.get_sample()

        assert isinstance(sample, pd.DataFrame)
        assert len(sample) == 10
        assert list(sample.columns) == ['id', 'name', 'value']

    def test_sampling_preserves_data_types(self):
        """Test that sampling preserves column data types."""
        sampler = ReservoirSampler(sample_size=5, random_seed=42)

        chunk = pd.DataFrame({
            'int_col': [1, 2, 3, 4, 5],
            'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
            'str_col': ['a', 'b', 'c', 'd', 'e']
        })
        sampler.add_chunk(chunk)

        sample = sampler.get_sample()

        # Data types should be preserved
        assert sample['int_col'].dtype in [np.int64, np.int32]
        assert sample['float_col'].dtype == np.float64
        assert sample['str_col'].dtype == object

    def test_large_scale_sampling(self):
        """Test reservoir sampling with large dataset."""
        sampler = ReservoirSampler(sample_size=100, random_seed=42)

        # Add 10,000 rows in chunks
        for i in range(100):
            chunk = pd.DataFrame({'value': range(i * 100, (i + 1) * 100)})
            sampler.add_chunk(chunk)

        sample = sampler.get_sample()

        assert len(sample) == 100
        assert sampler.items_seen == 10000


# ============================================================================
# SAMPLING VALIDATION ENGINE TESTS
# ============================================================================


@pytest.mark.unit
class TestSamplingValidationEngineInit:
    """Test suite for SamplingValidationEngine initialization."""

    def test_initialization_basic(self, temp_csv_file):
        """Test basic engine initialization."""
        config_dict = create_config_dict(

            "Test Job",

            [{'path': temp_csv_file, 'format': 'csv'}]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)

        assert engine.config == config
        assert engine.registry is not None
        assert engine.backend is not None

    def test_initialization_with_backend(self, temp_csv_file):
        """Test initialization with explicit backend."""
        config_dict = create_config_dict(

            "Test Job",

            [{'path': temp_csv_file}]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config, backend=DataFrameBackend.PANDAS)

        assert engine.backend == DataFrameBackend.PANDAS

    def test_initialization_auto_detects_backend(self, temp_csv_file):
        """Test that engine auto-detects backend when not specified."""
        config_dict = create_config_dict(

            "Test Job",

            [{'path': temp_csv_file}]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)

        # Should have auto-detected a backend
        assert engine.backend in [DataFrameBackend.PANDAS, DataFrameBackend.POLARS]

    def test_from_config_creates_engine(self, temp_config_file):
        """Test creating engine from YAML config file."""
        engine = SamplingValidationEngine.from_config(temp_config_file)

        assert isinstance(engine, SamplingValidationEngine)
        assert engine.config.job_name == "Test Validation Job"

    def test_from_config_with_backend(self, temp_config_file):
        """Test from_config with explicit backend."""
        engine = SamplingValidationEngine.from_config(
            temp_config_file,
            backend=DataFrameBackend.PANDAS
        )

        assert engine.backend == DataFrameBackend.PANDAS


@pytest.mark.integration
class TestSamplingEngineExecution:
    """Integration tests for engine execution."""

    def test_run_basic_validation(self, temp_csv_file):
        """Test running basic validation without sampling."""
        config_dict = create_config_dict(

            "Basic Test",

            [{
                'name': 'test_file',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {'type': 'EmptyFileCheck', 'severity': 'ERROR'}
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        assert report is not None
        assert report.job_name == "Basic Test"
        assert len(report.file_reports) == 1

    def test_run_with_sampling_enabled(self, temp_csv_file):
        """Test running validation with sampling enabled."""
        config_dict = create_config_dict(

            "Sampling Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {
                        'type': 'MandatoryFieldCheck',
                        'severity': 'ERROR',
                        'params': {'fields': ['id']},
                        'sampling': {
                            'enabled': True,
                            'sample_size': 10
                        }
                    }
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        assert len(report.file_reports) == 1
        file_report = report.file_reports[0]

        # Should have results
        assert len(file_report.validation_results) > 0

        # Check for sampling metadata
        for result in file_report.validation_results:
            if hasattr(result, 'is_sampled') and result.is_sampled:
                assert hasattr(result, 'sample_size')
                assert hasattr(result, 'population_size')

    def test_run_mixed_sampled_and_full_scan(self, temp_csv_file):
        """Test running with both sampled and full-scan validations."""
        config_dict = create_config_dict(

            "Mixed Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    # Full scan validation
                    {
                        'type': 'EmptyFileCheck',
                        'severity': 'ERROR'
                    },
                    # Sampled validation
                    {
                        'type': 'MandatoryFieldCheck',
                        'severity': 'ERROR',
                        'params': {'fields': ['id']},
                        'sampling': {
                            'enabled': True,
                            'sample_size': 5
                        }
                    }
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        file_report = report.file_reports[0]

        # Should have results from both validation types
        assert len(file_report.validation_results) >= 2

    def test_run_multiple_files(self, temp_csv_file, clean_dataframe):
        """Test running validation on multiple files."""
        # Create second file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            clean_dataframe.to_csv(f.name, index=False)
            temp_file2 = f.name

        try:
            config_dict = create_config_dict(

                "Multi File Test",

                [
                    {
                        'name': 'file1',
                        'path': temp_csv_file,
                        'format': 'csv',
                        'validations': [{'type': 'EmptyFileCheck', 'severity': 'ERROR'}]
                    },
                    {
                        'name': 'file2',
                        'path': temp_file2,
                        'format': 'csv',
                        'validations': [{'type': 'EmptyFileCheck', 'severity': 'ERROR'}]
                    }
                ]

            )

            config = ValidationConfig(config_dict)

            engine = SamplingValidationEngine(config)
            report = engine.run(verbose=False)

            assert len(report.file_reports) == 2
        finally:
            Path(temp_file2).unlink(missing_ok=True)

    def test_run_with_disabled_validation(self, temp_csv_file):
        """Test that disabled validations are skipped."""
        config_dict = create_config_dict(

            "Disabled Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {
                        'type': 'EmptyFileCheck',
                        'severity': 'ERROR',
                        'enabled': False  # Disabled
                    },
                    {
                        'type': 'MandatoryFieldCheck',
                        'severity': 'ERROR',
                        'params': {'fields': ['id']},
                        'enabled': True
                    }
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        file_report = report.file_reports[0]

        # Should only have 1 result (disabled validation skipped)
        assert len(file_report.validation_results) == 1
        assert file_report.validation_results[0].rule_name == 'MandatoryFieldCheck'

    def test_error_handling_missing_file(self):
        """Test graceful handling of missing files."""
        config_dict = create_config_dict(

            "Missing File Test",

            [{
                'name': 'missing',
                'path': '/nonexistent/file.csv',
                'format': 'csv',
                'validations': [{'type': 'EmptyFileCheck', 'severity': 'ERROR'}]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        # Should handle error gracefully
        assert len(report.file_reports) == 1
        file_report = report.file_reports[0]
        assert file_report.status == Status.FAILED

    def test_error_handling_unknown_validation(self, temp_csv_file):
        """Test handling of unknown validation types."""
        config_dict = create_config_dict(

            "Unknown Validation Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {'type': 'NonExistentValidation', 'severity': 'ERROR'}
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        # Should handle gracefully with error result
        file_report = report.file_reports[0]
        assert len(file_report.validation_results) > 0


@pytest.mark.integration
class TestSamplingEngineReportGeneration:
    """Test report generation functionality."""

    def test_generate_html_report(self, temp_csv_file):
        """Test HTML report generation."""
        config_dict = create_config_dict(

            "HTML Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [{'type': 'EmptyFileCheck', 'severity': 'ERROR'}]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            output_path = f.name

        try:
            engine.generate_html_report(report, output_path)

            # Verify file created
            assert Path(output_path).exists()
            assert Path(output_path).stat().st_size > 0
        finally:
            Path(output_path).unlink(missing_ok=True)

    def test_generate_json_report(self, temp_csv_file):
        """Test JSON report generation."""
        config_dict = create_config_dict(

            "JSON Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [{'type': 'EmptyFileCheck', 'severity': 'ERROR'}]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name

        try:
            engine.generate_json_report(report, output_path)

            # Verify file created
            assert Path(output_path).exists()
            assert Path(output_path).stat().st_size > 0

            # Verify it's valid JSON
            import json
            with open(output_path, 'r') as f:
                data = json.load(f)
                assert 'job_name' in data
        finally:
            Path(output_path).unlink(missing_ok=True)


@pytest.mark.performance
class TestSamplingEnginePerformance:
    """Performance tests for sampling engine."""

    def test_sampling_improves_performance_large_file(self, large_dataframe):
        """Test that sampling significantly improves performance on large files."""
        # Create temporary large file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            large_dataframe.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            # Config with sampling
            config_dict = create_config_dict(
                "Sampling Performance Test",
                [{
                    'name': 'large_file',
                    'path': temp_path,
                    'format': 'csv',
                    'validations': [
                        {
                            'type': 'MandatoryFieldCheck',
                            'severity': 'ERROR',
                            'params': {'fields': ['id']},
                            'sampling': {
                                'enabled': True,
                                'sample_size': 100
                            }
                        }
                    ]
                }],
                chunk_size=1000
            )
            config_sampled = ValidationConfig(config_dict)

            engine = SamplingValidationEngine(config_sampled)
            start = time.time()
            report = engine.run(verbose=False)
            duration = time.time() - start

            # Should complete quickly with sampling
            assert report is not None
            assert duration < 5  # Should be very fast with sampling

        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_memory_efficiency_with_sampling(self, large_dataframe):
        """Test that sampling maintains memory efficiency."""
        # Create temporary large file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            large_dataframe.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            config_dict = create_config_dict(
                "Memory Test",
                [{
                    'name': 'large',
                    'path': temp_path,
                    'format': 'csv',
                    'validations': [
                        {
                            'type': 'MandatoryFieldCheck',
                            'severity': 'ERROR',
                            'params': {'fields': ['id']},
                            'sampling': {
                                'enabled': True,
                                'sample_size': 50
                            }
                        }
                    ]
                }],
                chunk_size=1000
            )
            config = ValidationConfig(config_dict)

            engine = SamplingValidationEngine(config)

            # Should not raise MemoryError
            report = engine.run(verbose=False)
            assert report is not None

        finally:
            Path(temp_path).unlink(missing_ok=True)


@pytest.mark.unit
class TestSamplingEnginePrivateMethods:
    """Test private helper methods."""

    def test_execute_sampled_validation(self, temp_csv_file):
        """Test _execute_sampled_validation method."""
        config_dict = create_config_dict(

            "Test",

            [{'path': temp_csv_file}]

        )

        config = ValidationConfig(config_dict)
        engine = SamplingValidationEngine(config)

        # Create sample data
        sample_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50]
        })

        val_config = {
            'type': 'MandatoryFieldCheck',
            'severity': 'ERROR',
            'params': {'fields': ['id']}
        }

        context = {'max_sample_failures': 100}

        result = engine._execute_sampled_validation(
            val_config,
            sample_df,
            total_rows=100,
            context=context,
            verbose=False
        )

        assert result is not None
        assert isinstance(result, ValidationResult)
        if hasattr(result, 'is_sampled'):
            assert result.is_sampled is True
            assert result.sample_size == 5
            assert result.population_size == 100

    def test_execute_validation_error_handling(self, temp_csv_file):
        """Test error handling in _execute_validation."""
        config_dict = create_config_dict(

            "Test",

            [{'path': temp_csv_file}]

        )

        config = ValidationConfig(config_dict)
        engine = SamplingValidationEngine(config)

        val_config = {
            'type': 'NonExistentValidation',
            'severity': 'ERROR'
        }

        # Create mock loader
        from validation_framework.loaders.factory import LoaderFactory
        loader = LoaderFactory.create_loader(
            file_path=temp_csv_file,
            file_format='csv',
            chunk_size=1000
        )

        context = {'max_sample_failures': 100}

        # Should handle error gracefully
        result = engine._execute_validation(
            val_config,
            loader,
            context,
            sampled=False,
            verbose=False
        )

        assert result is not None
        assert result.passed is False


@pytest.mark.unit
class TestSamplingEngineEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_validations_list(self, temp_csv_file):
        """Test handling of empty validations list."""
        config_dict = create_config_dict(

            "Empty Validations Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': []
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        # Should complete without error
        assert len(report.file_reports) == 1
        file_report = report.file_reports[0]
        assert len(file_report.validation_results) == 0

    def test_all_validations_disabled(self, temp_csv_file):
        """Test when all validations are disabled."""
        config_dict = create_config_dict(

            "All Disabled Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {'type': 'EmptyFileCheck', 'severity': 'ERROR', 'enabled': False},
                    {'type': 'MandatoryFieldCheck', 'severity': 'ERROR',
                     'params': {'fields': ['id']}, 'enabled': False}
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        # Should have no results since all disabled
        file_report = report.file_reports[0]
        assert len(file_report.validation_results) == 0

    def test_very_small_sample_size(self, temp_csv_file):
        """Test with very small sample size."""
        config_dict = create_config_dict(

            "Small Sample Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {
                        'type': 'MandatoryFieldCheck',
                        'severity': 'ERROR',
                        'params': {'fields': ['id']},
                        'sampling': {
                            'enabled': True,
                            'sample_size': 1  # Very small
                        }
                    }
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = SamplingValidationEngine(config)
        report = engine.run(verbose=False)

        # Should handle small sample size
        assert len(report.file_reports) == 1
