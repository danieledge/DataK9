"""
Integration test for Phase 2 profiler enhancements.

Tests that temporal analysis, PII detection, and enhanced correlation
work correctly when enabled via DataProfiler flags.

Author: Daniel Edge
Date: 2025-11-22
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from validation_framework.profiler.engine import DataProfiler


class TestPhase2Integration:
    """Integration tests for Phase 2 profiler enhancements."""

    @pytest.fixture
    def sample_data_with_datetime(self):
        """Create sample data with datetime column."""
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        return pd.DataFrame({
            'date': dates,
            'value': np.random.randn(100),
            'category': np.random.choice(['A', 'B', 'C'], 100)
        })

    @pytest.fixture
    def sample_data_with_pii(self):
        """Create sample data with PII."""
        return pd.DataFrame({
            'customer_id': [f'CUST{i:04d}' for i in range(50)],
            'email': [f'user{i}@example.com' for i in range(50)],
            'phone': [f'555-{i:04d}' for i in range(50)],
            'amount': np.random.uniform(10, 1000, 50)
        })

    @pytest.fixture
    def sample_data_with_correlation(self):
        """Create sample data with correlated columns."""
        x = np.linspace(0, 10, 100)
        return pd.DataFrame({
            'x': x,
            'y_linear': 2 * x + 3 + np.random.normal(0, 1, 100),
            'y_independent': np.random.normal(50, 10, 100),
            'z': x ** 2
        })

    def test_temporal_analysis_enabled(self, sample_data_with_datetime):
        """Test that temporal analysis runs when enabled."""
        profiler = DataProfiler(
            enable_temporal_analysis=True,
            enable_pii_detection=False,
            enable_enhanced_correlation=False
        )

        result = profiler.profile_dataframe(
            sample_data_with_datetime,
            name='test_temporal'
        )

        # Check that temporal analysis was performed
        date_column = [col for col in result.columns if col.name == 'date'][0]

        assert date_column.temporal_analysis is not None, "Temporal analysis should be present"
        assert 'frequency' in date_column.temporal_analysis
        assert 'date_range' in date_column.temporal_analysis
        assert 'gaps' in date_column.temporal_analysis

    def test_temporal_analysis_disabled(self, sample_data_with_datetime):
        """Test that temporal analysis doesn't run when disabled."""
        profiler = DataProfiler(
            enable_temporal_analysis=False,
            enable_pii_detection=False,
            enable_enhanced_correlation=False
        )

        result = profiler.profile_dataframe(
            sample_data_with_datetime,
            name='test_no_temporal'
        )

        # Check that temporal analysis was NOT performed
        date_column = [col for col in result.columns if col.name == 'date'][0]

        assert date_column.temporal_analysis is None, "Temporal analysis should be None when disabled"

    def test_pii_detection_enabled(self, sample_data_with_pii):
        """Test that PII detection runs when enabled."""
        profiler = DataProfiler(
            enable_temporal_analysis=False,
            enable_pii_detection=True,
            enable_enhanced_correlation=False
        )

        result = profiler.profile_dataframe(
            sample_data_with_pii,
            name='test_pii'
        )

        # Check that PII was detected
        email_column = [col for col in result.columns if col.name == 'email'][0]
        phone_column = [col for col in result.columns if col.name == 'phone'][0]

        assert email_column.pii_info is not None, "PII info should be present"
        assert email_column.pii_info.get('detected') is True, "Email should be detected as PII"

        assert phone_column.pii_info is not None, "PII info should be present"
        assert phone_column.pii_info.get('detected') is True, "Phone should be detected as PII"

        # Check dataset privacy risk
        assert result.dataset_privacy_risk is not None, "Dataset privacy risk should be calculated"
        assert 'risk_score' in result.dataset_privacy_risk
        assert 'risk_level' in result.dataset_privacy_risk

    def test_pii_detection_disabled(self, sample_data_with_pii):
        """Test that PII detection doesn't run when disabled."""
        profiler = DataProfiler(
            enable_temporal_analysis=False,
            enable_pii_detection=False,
            enable_enhanced_correlation=False
        )

        result = profiler.profile_dataframe(
            sample_data_with_pii,
            name='test_no_pii'
        )

        # Check that PII detection was NOT performed
        email_column = [col for col in result.columns if col.name == 'email'][0]

        assert email_column.pii_info is None, "PII info should be None when disabled"
        assert result.dataset_privacy_risk is None, "Dataset privacy risk should be None when disabled"

    def test_enhanced_correlation_enabled(self, sample_data_with_correlation):
        """Test that enhanced correlation runs when enabled."""
        profiler = DataProfiler(
            enable_temporal_analysis=False,
            enable_pii_detection=False,
            enable_enhanced_correlation=True
        )

        result = profiler.profile_dataframe(
            sample_data_with_correlation,
            name='test_correlation'
        )

        # Check that enhanced correlation was performed
        assert result.enhanced_correlations is not None, "Enhanced correlations should be present"
        assert 'correlation_pairs' in result.enhanced_correlations
        assert 'methods_used' in result.enhanced_correlations

        # Should have detected correlation between x and y_linear
        pairs = result.enhanced_correlations['correlation_pairs']
        assert len(pairs) > 0, "Should have detected some correlations"

        # Check that correlations list includes enhanced data
        enhanced_correlations = [c for c in result.correlations if c.strength is not None]
        assert len(enhanced_correlations) > 0, "Correlations should include enhanced data"

    def test_enhanced_correlation_disabled(self, sample_data_with_correlation):
        """Test that enhanced correlation doesn't run when disabled."""
        profiler = DataProfiler(
            enable_temporal_analysis=False,
            enable_pii_detection=False,
            enable_enhanced_correlation=False
        )

        result = profiler.profile_dataframe(
            sample_data_with_correlation,
            name='test_no_correlation'
        )

        # Check that enhanced correlation was NOT performed
        assert result.enhanced_correlations is None, "Enhanced correlations should be None when disabled"

    def test_all_enhancements_enabled(self):
        """Test that all enhancements can be enabled together."""
        # Create data with datetime, PII, and correlations
        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        data = pd.DataFrame({
            'date': dates,
            'email': [f'user{i}@example.com' for i in range(100)],
            'amount': np.random.uniform(10, 1000, 100),
            'tax': None  # Will be filled with correlated values
        })
        data['tax'] = data['amount'] * 0.2 + np.random.normal(0, 5, 100)

        profiler = DataProfiler(
            enable_temporal_analysis=True,
            enable_pii_detection=True,
            enable_enhanced_correlation=True
        )

        result = profiler.profile_dataframe(data, name='test_all')

        # Verify temporal analysis
        date_column = [col for col in result.columns if col.name == 'date'][0]
        assert date_column.temporal_analysis is not None

        # Verify PII detection
        email_column = [col for col in result.columns if col.name == 'email'][0]
        assert email_column.pii_info is not None
        assert email_column.pii_info.get('detected') is True
        assert result.dataset_privacy_risk is not None

        # Verify enhanced correlation
        assert result.enhanced_correlations is not None
        # Should detect correlation between amount and tax
        pairs = result.enhanced_correlations['correlation_pairs']
        amount_tax_pair = [p for p in pairs if
                          (p['column1'] == 'amount' and p['column2'] == 'tax') or
                          (p['column1'] == 'tax' and p['column2'] == 'amount')]
        assert len(amount_tax_pair) > 0, "Should detect correlation between amount and tax"

    def test_json_serialization(self, sample_data_with_pii):
        """Test that ProfileResult with Phase 2 data can be serialized to JSON."""
        profiler = DataProfiler(
            enable_temporal_analysis=True,
            enable_pii_detection=True,
            enable_enhanced_correlation=True
        )

        result = profiler.profile_dataframe(sample_data_with_pii, name='test_json')

        # Convert to dict (simulates JSON serialization)
        result_dict = result.to_dict()

        # Verify structure
        assert 'columns' in result_dict
        assert 'enhanced_correlations' in result_dict or result.enhanced_correlations is None
        assert 'dataset_privacy_risk' in result_dict or result.dataset_privacy_risk is None

        # Verify column data
        for col_dict in result_dict['columns']:
            assert 'name' in col_dict
            # pii_info and temporal_analysis are optional
            if 'pii_info' in col_dict:
                assert isinstance(col_dict['pii_info'], dict)
            if 'temporal_analysis' in col_dict:
                assert isinstance(col_dict['temporal_analysis'], dict)

    def test_backward_compatibility(self, sample_data_with_correlation):
        """Test that disabling all enhancements works like before Phase 2."""
        profiler = DataProfiler(
            enable_temporal_analysis=False,
            enable_pii_detection=False,
            enable_enhanced_correlation=False
        )

        result = profiler.profile_dataframe(
            sample_data_with_correlation,
            name='test_backward_compat'
        )

        # Verify that Phase 2 fields are None/not present
        for column in result.columns:
            assert column.temporal_analysis is None
            assert column.pii_info is None

        assert result.enhanced_correlations is None
        assert result.dataset_privacy_risk is None

        # Verify that basic profiling still works
        assert result.row_count == 100
        assert result.column_count == 4
        assert len(result.columns) == 4
        assert result.overall_quality_score >= 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
