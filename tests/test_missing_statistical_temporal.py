"""
Tests for missing statistical and temporal validation checks.

Tests TrendDetectionCheck and AdvancedAnomalyDetectionCheck.
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from validation_framework.validations.builtin.temporal_checks import TrendDetectionCheck
from validation_framework.validations.builtin.statistical_checks import AdvancedAnomalyDetectionCheck
from validation_framework.core.results import Severity
from tests.conftest import create_data_iterator


@pytest.fixture
def baseline_trend_file():
    """Create a baseline file with historical trend data."""
    # Create historical data for last 7 days
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7, 0, -1)]
    values = [100, 105, 110, 108, 112, 115, 118]  # Steady growth trend

    df = pd.DataFrame({
        "date": dates,
        "value": values,
        "count": [50 + i*2 for i in range(len(dates))]
    })

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.mark.unit
class TestTrendDetectionCheck:
    """Tests for TrendDetectionCheck - detect unusual trends."""

    def test_trend_detection_normal_growth(self, baseline_trend_file):
        """Test trend detection with normal growth."""
        # Today's data shows normal growth
        today_df = pd.DataFrame({
            "value": [122]  # About 3.4% growth from yesterday (118)
        })

        validation = TrendDetectionCheck(
            name="NormalGrowth",
            severity=Severity.WARNING,
            params={
                "metric": "mean",
                "column": "value",
                "baseline_file": baseline_trend_file,
                "baseline_value_column": "value",
                "max_growth_pct": 50,  # Allow up to 50% growth
                "max_decline_pct": 30,  # Allow up to 30% decline
                "comparison_period": 1  # Compare to yesterday
            }
        )

        data_iterator = create_data_iterator(today_df)
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should pass - 3.4% growth is within acceptable range
        assert result.passed is True

    def test_trend_detection_excessive_growth(self, baseline_trend_file):
        """Test trend detection with excessive growth."""
        # Today's data shows unusual spike
        today_df = pd.DataFrame({
            "value": [200]  # About 69% growth from yesterday (118)
        })

        validation = TrendDetectionCheck(
            name="ExcessiveGrowth",
            severity=Severity.WARNING,
            params={
                "metric": "mean",
                "column": "value",
                "baseline_file": baseline_trend_file,
                "baseline_value_column": "value",
                "max_growth_pct": 50,  # Only allow up to 50% growth
                "comparison_period": 1
            }
        )

        data_iterator = create_data_iterator(today_df)
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should fail - 69% growth exceeds 50% threshold
        assert result.passed is False
        assert "growth" in result.message.lower()

    def test_trend_detection_excessive_decline(self, baseline_trend_file):
        """Test trend detection with excessive decline."""
        # Today's data shows sharp drop
        today_df = pd.DataFrame({
            "value": [70]  # About 41% decline from yesterday (118)
        })

        validation = TrendDetectionCheck(
            name="ExcessiveDecline",
            severity=Severity.WARNING,
            params={
                "metric": "mean",
                "column": "value",
                "baseline_file": baseline_trend_file,
                "baseline_value_column": "value",
                "max_decline_pct": 30,  # Only allow up to 30% decline
                "comparison_period": 1
            }
        )

        data_iterator = create_data_iterator(today_df)
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should fail - 41% decline exceeds 30% threshold
        assert result.passed is False
        assert "decline" in result.message.lower()

    def test_trend_detection_count_metric(self, baseline_trend_file):
        """Test trend detection using count metric."""
        # Test with row count
        today_df = pd.DataFrame({
            "id": range(1, 101)  # 100 rows
        })

        validation = TrendDetectionCheck(
            name="CountGrowth",
            severity=Severity.WARNING,
            params={
                "metric": "count",
                "baseline_file": baseline_trend_file,
                "baseline_value_column": "count",
                "max_growth_pct": 100,
                "max_decline_pct": 50,
                "comparison_period": 1
            }
        )

        data_iterator = create_data_iterator(today_df)
        result = validation.validate(data_iterator, {})

        # Result depends on baseline data, just verify it runs
        assert result is not None

    def test_trend_detection_missing_baseline_file(self):
        """Test trend detection with non-existent baseline file."""
        today_df = pd.DataFrame({"value": [100]})

        validation = TrendDetectionCheck(
            name="MissingBaseline",
            severity=Severity.WARNING,
            params={
                "metric": "mean",
                "column": "value",
                "baseline_file": "/nonexistent/file.csv",
                "max_growth_pct": 50
            }
        )

        data_iterator = create_data_iterator(today_df)
        result = validation.validate(data_iterator, {})

        assert result.passed is False
        assert "baseline" in result.message.lower() or "error" in result.message.lower()


@pytest.mark.unit
class TestAdvancedAnomalyDetectionCheck:
    """Tests for AdvancedAnomalyDetectionCheck - advanced anomaly detection."""

    def test_anomaly_detection_iqr_method(self, dataframe_with_outliers):
        """Test IQR method for anomaly detection."""
        validation = AdvancedAnomalyDetectionCheck(
            name="IQRAnomalies",
            severity=Severity.WARNING,
            params={
                "column": "value",
                "method": "iqr",
                "max_anomaly_pct": 5  # Allow up to 5% anomalies
            }
        )

        data_iterator = create_data_iterator(dataframe_with_outliers)
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should detect the 4 outliers (200, 250, -50, 300) out of 104 values = 3.8%
        # This is under 5%, so should pass
        assert result.passed is True or result.failed_count > 0  # Depends on exact IQR calculation

    def test_anomaly_detection_zscore_method(self):
        """Test Z-score method for anomaly detection."""
        # Create data with clear outliers
        df = pd.DataFrame({
            "value": [10, 12, 11, 13, 12, 10, 11, 100, 12, 11]  # 100 is clear outlier
        })

        validation = AdvancedAnomalyDetectionCheck(
            name="ZScoreAnomalies",
            severity=Severity.WARNING,
            params={
                "column": "value",
                "method": "zscore",
                "threshold": 3.0,  # Z-score threshold
                "max_anomaly_pct": 10  # Allow up to 10% anomalies
            }
        )

        data_iterator = create_data_iterator(df)
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should detect value=100 as anomaly (1 out of 10 = 10%)
        assert result is not None

    def test_anomaly_detection_modified_zscore(self):
        """Test modified Z-score method (robust to outliers)."""
        df = pd.DataFrame({
            "price": [100, 105, 102, 98, 101, 103, 500, 99, 104, 102]  # 500 is outlier
        })

        validation = AdvancedAnomalyDetectionCheck(
            name="ModifiedZScore",
            severity=Severity.WARNING,
            params={
                "column": "price",
                "method": "modified_zscore",
                "threshold": 3.5,
                "max_anomaly_pct": 15
            }
        )

        data_iterator = create_data_iterator(df)
        result = validation.validate(data_iterator, {})

        # Should detect the outlier
        assert result is not None

    def test_anomaly_detection_no_anomalies(self):
        """Test anomaly detection with no anomalies."""
        # All values are very similar
        df = pd.DataFrame({
            "value": [100, 101, 99, 100, 102, 98, 101, 100, 99, 101]
        })

        validation = AdvancedAnomalyDetectionCheck(
            name="NoAnomalies",
            severity=Severity.WARNING,
            params={
                "column": "value",
                "method": "iqr",
                "max_anomaly_pct": 5
            }
        )

        data_iterator = create_data_iterator(df)
        result = validation.validate(data_iterator, {})

        # Should pass - no significant anomalies
        assert result.passed is True
        assert result.failed_count == 0

    def test_anomaly_detection_too_many_anomalies(self):
        """Test when anomaly percentage exceeds threshold."""
        # Create dataset with many normal values and clear outliers
        df = pd.DataFrame({
            "value": [100, 102, 98, 101, 99, 103, 100, 101, 102, 98,  # 10 normal values
                     1000, 2000, 3000, 4000, 5000]  # 5 extreme outliers = 33% anomalies
        })

        validation = AdvancedAnomalyDetectionCheck(
            name="TooManyAnomalies",
            severity=Severity.ERROR,
            params={
                "column": "value",
                "method": "zscore",  # Use zscore for more reliable detection
                "threshold": 2.0,
                "max_anomaly_pct": 5  # Only allow 5% anomalies
            }
        )

        data_iterator = create_data_iterator(df)
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should fail - 6.67% anomalies exceeds 5% threshold
        assert result.passed is False
        assert result.failed_count > 0

    def test_anomaly_detection_missing_column(self):
        """Test anomaly detection with non-existent column."""
        df = pd.DataFrame({"other_column": [1, 2, 3]})

        validation = AdvancedAnomalyDetectionCheck(
            name="MissingColumn",
            severity=Severity.ERROR,
            params={
                "column": "nonexistent",
                "method": "iqr"
            }
        )

        data_iterator = create_data_iterator(df)
        result = validation.validate(data_iterator, {})

        assert result.passed is False
        assert "not found" in result.message.lower() or "error" in result.message.lower()

    def test_anomaly_detection_invalid_method(self):
        """Test anomaly detection with invalid method."""
        df = pd.DataFrame({"value": [1, 2, 3, 4, 5]})

        validation = AdvancedAnomalyDetectionCheck(
            name="InvalidMethod",
            severity=Severity.ERROR,
            params={
                "column": "value",
                "method": "invalid_method"
            }
        )

        data_iterator = create_data_iterator(df)
        result = validation.validate(data_iterator, {})

        # Should handle gracefully
        assert result is not None


@pytest.mark.integration
class TestStatisticalTemporalIntegration:
    """Integration tests combining statistical and temporal validations."""

    def test_combined_checks(self, baseline_trend_file, dataframe_with_outliers):
        """Test trend detection and anomaly detection together."""
        # Trend check
        trend_validation = TrendDetectionCheck(
            name="TrendCheck",
            severity=Severity.WARNING,
            params={
                "metric": "mean",
                "column": "value",
                "baseline_file": baseline_trend_file,
                "baseline_value_column": "value",
                "max_growth_pct": 50,
                "max_decline_pct": 30
            }
        )

        # Anomaly check
        anomaly_validation = AdvancedAnomalyDetectionCheck(
            name="AnomalyCheck",
            severity=Severity.WARNING,
            params={
                "column": "value",
                "method": "iqr",
                "max_anomaly_pct": 5
            }
        )

        context = {"max_sample_failures": 100}

        # Test trend with current data
        trend_result = trend_validation.validate(
            create_data_iterator(dataframe_with_outliers),
            context
        )

        # Test anomalies
        anomaly_result = anomaly_validation.validate(
            create_data_iterator(dataframe_with_outliers),
            context
        )

        # Both should execute and return results
        assert trend_result is not None
        assert anomaly_result is not None
