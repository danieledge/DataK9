"""
Tests for memory monitoring in the optimized validation engine.

Ensures that memory safety checks are properly implemented to prevent
OOM crashes during validation of large datasets.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


class TestMemoryMonitoring:
    """Tests for memory monitoring in OptimizedValidationEngine."""

    def test_engine_has_memory_thresholds(self):
        """Test that engine has memory threshold class attributes."""
        from validation_framework.core.optimized_engine import OptimizedValidationEngine

        assert hasattr(OptimizedValidationEngine, 'MEMORY_WARNING_THRESHOLD')
        assert hasattr(OptimizedValidationEngine, 'MEMORY_CRITICAL_THRESHOLD')
        assert hasattr(OptimizedValidationEngine, 'MEMORY_CHECK_INTERVAL')

        # Check reasonable defaults
        assert OptimizedValidationEngine.MEMORY_WARNING_THRESHOLD == 70
        assert OptimizedValidationEngine.MEMORY_CRITICAL_THRESHOLD == 80
        assert OptimizedValidationEngine.MEMORY_CHECK_INTERVAL == 5

    def test_engine_has_disable_memory_check_option(self):
        """Test that engine can be initialized with disable_memory_check."""
        from validation_framework.core.optimized_engine import OptimizedValidationEngine
        from validation_framework.core.config import ValidationConfig

        # Create a minimal config
        config = MagicMock(spec=ValidationConfig)
        config.job_name = "test"
        config.files = []
        config.validations = []

        # Test default (memory check enabled)
        engine = OptimizedValidationEngine(config, disable_memory_check=False)
        assert engine.disable_memory_check == False

        # Test disabled
        engine_disabled = OptimizedValidationEngine(config, disable_memory_check=True)
        assert engine_disabled.disable_memory_check == True

    def test_check_memory_safety_method_exists(self):
        """Test that _check_memory_safety method exists."""
        from validation_framework.core.optimized_engine import OptimizedValidationEngine

        assert hasattr(OptimizedValidationEngine, '_check_memory_safety')
        assert callable(getattr(OptimizedValidationEngine, '_check_memory_safety'))

    def test_check_memory_safety_returns_true_normally(self):
        """Test that _check_memory_safety returns True when memory is OK."""
        from validation_framework.core.optimized_engine import OptimizedValidationEngine
        from validation_framework.core.config import ValidationConfig

        config = MagicMock(spec=ValidationConfig)
        config.job_name = "test"
        config.files = []
        config.validations = []

        engine = OptimizedValidationEngine(config)

        # Mock psutil to return normal memory usage
        with patch('validation_framework.core.optimized_engine.psutil') as mock_psutil:
            mock_memory = MagicMock()
            mock_memory.percent = 50  # 50% usage - safe
            mock_psutil.virtual_memory.return_value = mock_memory

            mock_process = MagicMock()
            mock_process.memory_info.return_value.rss = 100 * 1024 * 1024  # 100MB
            mock_psutil.Process.return_value = mock_process

            # Should return True (safe to continue)
            # Note: Only checks every MEMORY_CHECK_INTERVAL chunks
            result = engine._check_memory_safety(chunk_idx=0, row_count=1000)
            assert result == True

    def test_check_memory_safety_raises_on_critical(self):
        """Test that _check_memory_safety raises MemoryError at critical threshold."""
        from validation_framework.core.optimized_engine import OptimizedValidationEngine
        from validation_framework.core.config import ValidationConfig

        config = MagicMock(spec=ValidationConfig)
        config.job_name = "test"
        config.files = []
        config.validations = []

        engine = OptimizedValidationEngine(config)

        # Mock psutil to return critical memory usage
        with patch('validation_framework.core.optimized_engine.psutil') as mock_psutil:
            # Configure mock memory info
            mock_memory = MagicMock()
            mock_memory.percent = 85  # 85% usage - above critical threshold
            mock_memory.available = 500 * 1024 * 1024  # 500MB available
            mock_psutil.virtual_memory.return_value = mock_memory

            # Configure mock process
            mock_mem_info = MagicMock()
            mock_mem_info.rss = 4000 * 1024 * 1024  # 4GB
            mock_process = MagicMock()
            mock_process.memory_info.return_value = mock_mem_info
            mock_psutil.Process.return_value = mock_process

            # Should raise MemoryError
            with pytest.raises(MemoryError) as exc_info:
                engine._check_memory_safety(chunk_idx=0, row_count=1000000)

            assert "critical threshold" in str(exc_info.value).lower()

    def test_check_memory_safety_skips_when_disabled(self):
        """Test that _check_memory_safety does nothing when disabled."""
        from validation_framework.core.optimized_engine import OptimizedValidationEngine
        from validation_framework.core.config import ValidationConfig

        config = MagicMock(spec=ValidationConfig)
        config.job_name = "test"
        config.files = []
        config.validations = []

        engine = OptimizedValidationEngine(config, disable_memory_check=True)

        # Even with critical memory, should return True when disabled
        with patch('validation_framework.core.optimized_engine.psutil') as mock_psutil:
            mock_memory = MagicMock()
            mock_memory.percent = 95  # 95% usage - very critical
            mock_psutil.virtual_memory.return_value = mock_memory

            # Should still return True (disabled)
            result = engine._check_memory_safety(chunk_idx=0, row_count=1000000)
            assert result == True

    def test_check_memory_safety_respects_interval(self):
        """Test that memory is only checked every N chunks."""
        from validation_framework.core.optimized_engine import OptimizedValidationEngine
        from validation_framework.core.config import ValidationConfig

        config = MagicMock(spec=ValidationConfig)
        config.job_name = "test"
        config.files = []
        config.validations = []

        engine = OptimizedValidationEngine(config)
        interval = engine.MEMORY_CHECK_INTERVAL

        with patch('validation_framework.core.optimized_engine.psutil') as mock_psutil:
            # Configure proper mock returns
            mock_memory = MagicMock()
            mock_memory.percent = 50  # Safe memory usage
            mock_psutil.virtual_memory.return_value = mock_memory

            mock_mem_info = MagicMock()
            mock_mem_info.rss = 100 * 1024 * 1024  # 100MB
            mock_process = MagicMock()
            mock_process.memory_info.return_value = mock_mem_info
            mock_psutil.Process.return_value = mock_process

            # Check on chunk 0 (interval index)
            result = engine._check_memory_safety(chunk_idx=0, row_count=1000)
            assert mock_psutil.virtual_memory.called

            mock_psutil.reset_mock()
            mock_psutil.virtual_memory.return_value = mock_memory
            mock_psutil.Process.return_value = mock_process

            # Check on chunk 1 (not interval index)
            result = engine._check_memory_safety(chunk_idx=1, row_count=2000)
            assert not mock_psutil.virtual_memory.called

            mock_psutil.reset_mock()
            mock_psutil.virtual_memory.return_value = mock_memory
            mock_psutil.Process.return_value = mock_process

            # Check on chunk at interval (e.g., chunk 5 if interval=5)
            result = engine._check_memory_safety(chunk_idx=interval, row_count=5000)
            assert mock_psutil.virtual_memory.called


class TestMemoryMonitoringPsutilUnavailable:
    """Tests for memory monitoring when psutil is not available."""

    def test_graceful_handling_without_psutil(self):
        """Test that memory monitoring works gracefully without psutil."""
        from validation_framework.core.optimized_engine import OptimizedValidationEngine
        from validation_framework.core.config import ValidationConfig

        config = MagicMock(spec=ValidationConfig)
        config.job_name = "test"
        config.files = []
        config.validations = []

        engine = OptimizedValidationEngine(config)

        # Mock PSUTIL_AVAILABLE to False
        with patch('validation_framework.core.optimized_engine.PSUTIL_AVAILABLE', False):
            # Should return True (skip check when psutil unavailable)
            result = engine._check_memory_safety(chunk_idx=0, row_count=1000)
            assert result == True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
