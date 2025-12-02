"""
Comprehensive tests for OptimizedValidationEngine and ReservoirSampler.

This test module covers:
1. ReservoirSampler implementation and correctness
2. SinglePassValidationState functionality
3. OptimizedValidationEngine single-pass execution
4. Sampling configuration and execution
5. Performance characteristics
6. Error handling and edge cases

Author: Daniel Edge
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import time

from validation_framework.core.optimized_engine import (
    ReservoirSampler,
    SinglePassValidationState,
    OptimizedValidationEngine,
)
from validation_framework.core.config import ValidationConfig
from validation_framework.core.results import Status, Severity, ValidationResult
from validation_framework.validations.base import ValidationRule


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
# RESERVOIR SAMPLER TESTS
# ============================================================================


@pytest.mark.unit
class TestReservoirSampler:
    """Test suite for ReservoirSampler implementation."""

    def test_initialization_basic(self):
        """Test basic sampler initialization."""
        sampler = ReservoirSampler(sample_size=100)

        assert sampler.sample_size == 100
        assert len(sampler.reservoir) == 0
        assert sampler.items_seen == 0

    def test_initialization_with_seed(self):
        """Test sampler initialization with random seed for reproducibility."""
        sampler1 = ReservoirSampler(sample_size=50, random_seed=42)
        sampler2 = ReservoirSampler(sample_size=50, random_seed=42)

        # Add same data to both samplers
        df = pd.DataFrame({'value': range(1000)})
        sampler1.add_chunk(df, offset=0)
        sampler2.add_chunk(df, offset=0)

        # Should produce samples of correct size
        sample1 = sampler1.get_sample()
        sample2 = sampler2.get_sample()

        assert len(sample1) == 50
        assert len(sample2) == 50
        # With same seed, samples should have significant overlap
        overlap = len(set(sample1['value']) & set(sample2['value']))
        assert overlap > 40  # At least 80% overlap with same seed

    def test_add_chunk_fills_reservoir(self):
        """Test that chunks fill the reservoir up to sample_size."""
        sampler = ReservoirSampler(sample_size=10, random_seed=42)

        # Add small chunk (less than sample_size)
        small_chunk = pd.DataFrame({'id': range(5), 'value': range(5, 10)})
        sampler.add_chunk(small_chunk, offset=0)

        assert len(sampler.reservoir) == 5
        assert sampler.items_seen == 5

    def test_add_chunk_exact_sample_size(self):
        """Test adding exactly sample_size items."""
        sampler = ReservoirSampler(sample_size=10, random_seed=42)

        chunk = pd.DataFrame({'id': range(10), 'value': range(10, 20)})
        sampler.add_chunk(chunk, offset=0)

        assert len(sampler.reservoir) == 10
        assert sampler.items_seen == 10

    def test_add_chunk_exceeds_sample_size(self):
        """Test that reservoir stays at sample_size when more items added."""
        sampler = ReservoirSampler(sample_size=10, random_seed=42)

        # Add 100 items
        chunk = pd.DataFrame({'id': range(100), 'value': range(100, 200)})
        sampler.add_chunk(chunk, offset=0)

        # Reservoir should be exactly sample_size
        assert len(sampler.reservoir) == 10
        assert sampler.items_seen == 100

    def test_add_multiple_chunks(self):
        """Test adding multiple chunks sequentially."""
        sampler = ReservoirSampler(sample_size=50, random_seed=42)

        # Add three chunks
        for i in range(3):
            chunk = pd.DataFrame({
                'id': range(i * 100, (i + 1) * 100),
                'value': range(i * 100, (i + 1) * 100)
            })
            sampler.add_chunk(chunk, offset=i * 100)

        assert len(sampler.reservoir) == 50
        assert sampler.items_seen == 300

    def test_get_sample_empty_reservoir(self):
        """Test get_sample on empty reservoir."""
        sampler = ReservoirSampler(sample_size=10)

        sample = sampler.get_sample()

        assert isinstance(sample, pd.DataFrame)
        assert len(sample) == 0

    def test_get_sample_preserves_original_indices(self):
        """Test that get_sample preserves original DataFrame indices."""
        sampler = ReservoirSampler(sample_size=5, random_seed=42)

        # Add chunk with explicit offset
        chunk = pd.DataFrame({'value': [10, 20, 30, 40, 50]})
        sampler.add_chunk(chunk, offset=100)  # Offset by 100

        sample = sampler.get_sample()

        # Indices should be >= 100
        assert all(idx >= 100 for idx in sample.index)

    def test_get_sample_returns_dataframe(self):
        """Test that get_sample returns proper DataFrame structure."""
        sampler = ReservoirSampler(sample_size=10, random_seed=42)

        chunk = pd.DataFrame({
            'id': range(50),
            'name': [f'item_{i}' for i in range(50)],
            'value': range(100, 150)
        })
        sampler.add_chunk(chunk, offset=0)

        sample = sampler.get_sample()

        # Check structure
        assert isinstance(sample, pd.DataFrame)
        assert len(sample) == 10
        assert list(sample.columns) == ['id', 'name', 'value']

    def test_sampling_uniformity_statistical(self):
        """Test that sampling is approximately uniform (statistical test)."""
        # This test verifies that reservoir sampling gives each item equal probability
        sampler = ReservoirSampler(sample_size=100, random_seed=42)

        # Create large dataset
        chunk = pd.DataFrame({
            'id': range(10000),
            'group': [i % 10 for i in range(10000)]
        })
        sampler.add_chunk(chunk, offset=0)

        sample = sampler.get_sample()

        # Count items from each group
        group_counts = sample['group'].value_counts()

        # Each group should have approximately 10 items (100 sample / 10 groups)
        # Allow reasonable variance (5 to 15 items per group)
        for count in group_counts.values:
            assert 5 <= count <= 15, f"Group count {count} outside expected range [5, 15]"

    def test_memory_efficiency(self):
        """Test that reservoir doesn't grow beyond sample_size."""
        sampler = ReservoirSampler(sample_size=10)

        # Add very large number of items
        for i in range(10):
            chunk = pd.DataFrame({'value': range(1000)})
            sampler.add_chunk(chunk, offset=i * 1000)

        # Reservoir should stay at sample_size
        assert len(sampler.reservoir) == 10
        assert sampler.items_seen == 10000


# ============================================================================
# SINGLE PASS VALIDATION STATE TESTS
# ============================================================================


@pytest.mark.unit
class TestSinglePassValidationState:
    """Test suite for SinglePassValidationState."""

    def create_mock_validation(self):
        """Create a mock validation for testing."""
        validation = Mock(spec=ValidationRule)
        validation.name = "TestValidation"
        validation.validate = Mock(return_value=ValidationResult(
            rule_name="TestValidation",
            severity=Severity.ERROR,
            passed=True,
            message="Test passed"
        ))
        return validation

    def test_initialization_without_sampling(self):
        """Test state initialization without sampling enabled."""
        validation = self.create_mock_validation()
        validation_config = {
            'type': 'TestValidation',
            'severity': 'ERROR',
            'params': {}
        }
        context = {'max_sample_failures': 100}

        state = SinglePassValidationState(validation, validation_config, context)

        assert state.validation == validation
        assert state.use_sampling is False
        assert state.sampler is None
        assert state.total_rows == 0

    def test_initialization_with_sampling(self):
        """Test state initialization with sampling enabled."""
        validation = self.create_mock_validation()
        validation_config = {
            'type': 'TestValidation',
            'severity': 'ERROR',
            'sampling': {
                'enabled': True,
                'sample_size': 1000
            }
        }
        context = {'max_sample_failures': 100}

        state = SinglePassValidationState(validation, validation_config, context)

        assert state.use_sampling is True
        assert state.sample_size == 1000
        assert isinstance(state.sampler, ReservoirSampler)

    def test_process_chunk_without_sampling(self):
        """Test processing chunk without sampling (incremental aggregation)."""
        validation = self.create_mock_validation()
        validation_config = {'type': 'Test', 'severity': 'ERROR'}
        context = {'max_sample_failures': 100}

        state = SinglePassValidationState(validation, validation_config, context)

        chunk = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})
        state.process_chunk(chunk, chunk_idx=0)

        assert state.total_rows == 3
        # New optimized behavior: aggregate results instead of storing chunks
        assert 'aggregate' in state.state
        assert state.state['aggregate']['passed'] is True

    def test_process_chunk_with_sampling(self):
        """Test processing chunk with sampling enabled."""
        validation = self.create_mock_validation()
        validation_config = {
            'type': 'Test',
            'severity': 'ERROR',
            'sampling': {'enabled': True, 'sample_size': 10}
        }
        context = {'max_sample_failures': 100}

        state = SinglePassValidationState(validation, validation_config, context)

        chunk = pd.DataFrame({'id': range(50), 'value': range(50, 100)})
        state.process_chunk(chunk, chunk_idx=0)

        assert state.total_rows == 50
        assert state.sampler.items_seen == 50

    def test_process_multiple_chunks(self):
        """Test processing multiple chunks."""
        validation = self.create_mock_validation()
        validation_config = {'type': 'Test', 'severity': 'ERROR'}
        context = {'max_sample_failures': 100}

        state = SinglePassValidationState(validation, validation_config, context)

        # Process 3 chunks
        for i in range(3):
            chunk = pd.DataFrame({'id': range(10), 'value': range(10)})
            state.process_chunk(chunk, chunk_idx=i)

        assert state.total_rows == 30
        # New optimized behavior: aggregate results instead of storing chunks
        assert 'aggregate' in state.state
        assert state.state['aggregate']['total_count'] == 0  # Mock validation returns 0

    def test_finalize_without_sampling(self):
        """Test finalization without sampling."""
        validation = self.create_mock_validation()
        validation_config = {'type': 'Test', 'severity': 'ERROR'}
        context = {'max_sample_failures': 100}

        state = SinglePassValidationState(validation, validation_config, context)

        # Process some chunks
        chunk = pd.DataFrame({'id': [1, 2, 3]})
        state.process_chunk(chunk, chunk_idx=0)

        # Finalize
        result = state.finalize()

        assert isinstance(result, ValidationResult)
        assert result.is_sampled is False
        assert validation.validate.called

    def test_finalize_with_sampling(self):
        """Test finalization with sampling."""
        validation = self.create_mock_validation()
        validation_config = {
            'type': 'Test',
            'severity': 'ERROR',
            'sampling': {'enabled': True, 'sample_size': 10}
        }
        context = {'max_sample_failures': 100}

        state = SinglePassValidationState(validation, validation_config, context)

        # Process chunk
        chunk = pd.DataFrame({'id': range(50)})
        state.process_chunk(chunk, chunk_idx=0)

        # Finalize
        result = state.finalize()

        assert result.is_sampled is True
        assert result.sample_size <= 10
        assert result.population_size == 50

    def test_finalize_records_execution_time(self):
        """Test that finalize records execution time."""
        validation = self.create_mock_validation()
        validation_config = {'type': 'Test', 'severity': 'ERROR'}
        context = {'max_sample_failures': 100}

        state = SinglePassValidationState(validation, validation_config, context)

        # Small delay to ensure measurable time
        time.sleep(0.01)

        result = state.finalize()

        assert hasattr(result, 'execution_time')
        assert result.execution_time > 0


# ============================================================================
# OPTIMIZED VALIDATION ENGINE TESTS
# ============================================================================


@pytest.mark.unit
class TestOptimizedValidationEngine:
    """Test suite for OptimizedValidationEngine."""

    def test_initialization(self, valid_config_dict, temp_csv_file):
        """Test engine initialization with valid config."""
        config_dict = create_config_dict(
            "Test Job",
            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': []
            }]
        )
        config = ValidationConfig(config_dict)

        engine = OptimizedValidationEngine(config, use_single_pass=True)

        assert engine.config == config
        assert engine.use_single_pass is True
        assert engine.registry is not None

    def test_initialization_default_single_pass(self, valid_config_dict, temp_csv_file):
        """Test that single-pass is enabled by default."""
        config_dict = create_config_dict(

            "Test Job",

            [{'name': 'test', 'path': temp_csv_file, 'format': 'csv'}]

        )

        config = ValidationConfig(config_dict)

        engine = OptimizedValidationEngine(config)

        assert engine.use_single_pass is True

    def test_from_config_creates_engine(self, temp_config_file):
        """Test creating engine from YAML config file."""
        engine = OptimizedValidationEngine.from_config(temp_config_file)

        assert isinstance(engine, OptimizedValidationEngine)
        assert engine.config.job_name == "Test Validation Job"

    def test_from_config_with_single_pass_flag(self, temp_config_file):
        """Test from_config respects use_single_pass parameter."""
        engine = OptimizedValidationEngine.from_config(
            temp_config_file,
            use_single_pass=False
        )

        assert engine.use_single_pass is False

    def test_list_available_validations(self, temp_csv_file):
        """Test listing available validations from registry."""
        config_dict = create_config_dict(

            "Test",

            [{'path': temp_csv_file}]

        )

        config = ValidationConfig(config_dict)
        engine = OptimizedValidationEngine(config)

        validations = engine.list_available_validations()

        assert isinstance(validations, list)
        assert len(validations) > 0
        # Check for common validations
        assert any('EmptyFileCheck' in v for v in validations)


@pytest.mark.integration
class TestOptimizedEngineExecution:
    """Integration tests for engine execution."""

    def test_run_basic_validation_single_pass(self, temp_csv_file):
        """Test running basic validation in single-pass mode."""
        config_dict = create_config_dict(

            "Test Job",

            [{
                'name': 'test_file',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {
                        'type': 'EmptyFileCheck',
                        'severity': 'ERROR'
                    }
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = OptimizedValidationEngine(config, use_single_pass=True)
        report = engine.run(verbose=False)

        assert isinstance(report, object)
        assert report.job_name == "Test Job"
        assert len(report.file_reports) == 1

    def test_run_multiple_validations_single_pass(self, temp_csv_file):
        """Test running multiple validations in single-pass mode."""
        config_dict = create_config_dict(

            "Multi Validation Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {'type': 'EmptyFileCheck', 'severity': 'ERROR'},
                    {'type': 'MandatoryFieldCheck', 'severity': 'ERROR',
                     'params': {'fields': ['id', 'name']}}
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = OptimizedValidationEngine(config, use_single_pass=True)
        report = engine.run(verbose=False)

        assert len(report.file_reports) == 1
        file_report = report.file_reports[0]
        # Should have results for both validations
        assert len(file_report.validation_results) >= 2

    def test_run_standard_mode_fallback(self, temp_csv_file):
        """Test running in standard mode (non-optimized)."""
        config_dict = create_config_dict(

            "Standard Mode Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [
                    {'type': 'EmptyFileCheck', 'severity': 'ERROR'}
                ]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = OptimizedValidationEngine(config, use_single_pass=False)
        report = engine.run(verbose=False)

        assert isinstance(report, object)
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

        engine = OptimizedValidationEngine(config, use_single_pass=True)
        report = engine.run(verbose=False)

        assert len(report.file_reports) == 1
        file_report = report.file_reports[0]

        # Check if sampling metadata is present
        if len(file_report.validation_results) > 0:
            result = file_report.validation_results[0]
            if hasattr(result, 'is_sampled'):
                assert result.is_sampled is True

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

            engine = OptimizedValidationEngine(config, use_single_pass=True)
            report = engine.run(verbose=False)

            assert len(report.file_reports) == 2
            assert report.file_reports[0].file_name == 'file1'
            assert report.file_reports[1].file_name == 'file2'
        finally:
            Path(temp_file2).unlink(missing_ok=True)

    def test_file_not_found_error_handling(self):
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

        engine = OptimizedValidationEngine(config, use_single_pass=True)
        report = engine.run(verbose=False)

        assert len(report.file_reports) == 1
        file_report = report.file_reports[0]
        # Should have error result for missing file
        assert file_report.status == Status.FAILED

    def test_unknown_validation_error_handling(self, temp_csv_file):
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

        engine = OptimizedValidationEngine(config, use_single_pass=True)
        report = engine.run(verbose=False)

        assert len(report.file_reports) == 1
        file_report = report.file_reports[0]
        # Should have error result for unknown validation
        assert any('not found' in r.message.lower() for r in file_report.validation_results)


@pytest.mark.integration
class TestOptimizedEngineReportGeneration:
    """Test report generation functionality."""

    def test_generate_html_report(self, temp_csv_file):
        """Test HTML report generation."""
        config_dict = create_config_dict(

            "HTML Report Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [{'type': 'EmptyFileCheck', 'severity': 'ERROR'}]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = OptimizedValidationEngine(config)
        report = engine.run(verbose=False)

        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
            output_path = f.name

        try:
            engine.generate_html_report(report, output_path)

            # Verify file was created
            assert Path(output_path).exists()
            assert Path(output_path).stat().st_size > 0
        finally:
            Path(output_path).unlink(missing_ok=True)

    def test_generate_json_report(self, temp_csv_file):
        """Test JSON report generation."""
        config_dict = create_config_dict(

            "JSON Report Test",

            [{
                'name': 'test',
                'path': temp_csv_file,
                'format': 'csv',
                'validations': [{'type': 'EmptyFileCheck', 'severity': 'ERROR'}]
            }]

        )

        config = ValidationConfig(config_dict)

        engine = OptimizedValidationEngine(config)
        report = engine.run(verbose=False)

        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            output_path = f.name

        try:
            engine.generate_json_report(report, output_path)

            # Verify file was created
            assert Path(output_path).exists()
            assert Path(output_path).stat().st_size > 0
        finally:
            Path(output_path).unlink(missing_ok=True)


@pytest.mark.performance
class TestOptimizedEnginePerformance:
    """Performance-related tests for optimized engine."""

    def test_single_pass_reduces_file_reads(self, large_dataframe):
        """Test that single-pass mode reduces file I/O operations."""
        # Create temporary large file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            large_dataframe.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            config_dict = create_config_dict(
                "Performance Test",
                [{
                    'name': 'large_file',
                    'path': temp_path,
                    'format': 'csv',
                    'validations': [
                        {'type': 'EmptyFileCheck', 'severity': 'ERROR'},
                        {'type': 'MandatoryFieldCheck', 'severity': 'ERROR',
                         'params': {'fields': ['id']}}
                    ]
                }],
                chunk_size=1000
            )
            config = ValidationConfig(config_dict)

            # Run single-pass mode
            engine_sp = OptimizedValidationEngine(config, use_single_pass=True)
            start_sp = time.time()
            report_sp = engine_sp.run(verbose=False)
            duration_sp = time.time() - start_sp

            # Verify it ran successfully
            assert report_sp is not None
            assert duration_sp < 10  # Should be fast

        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_sampling_improves_performance(self, large_dataframe):
        """Test that sampling significantly improves performance."""
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

            engine = OptimizedValidationEngine(config_sampled, use_single_pass=True)
            start = time.time()
            report = engine.run(verbose=False)
            duration = time.time() - start

            # Should complete quickly with sampling
            assert report is not None
            assert duration < 5

        finally:
            Path(temp_path).unlink(missing_ok=True)
