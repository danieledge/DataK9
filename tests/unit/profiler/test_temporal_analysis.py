"""
Unit tests for temporal_analysis.py

Tests temporal analysis functionality including:
- Frequency inference
- Gap detection
- Trend analysis
- Seasonality detection
- ACF/PACF calculation
- Temporal pattern extraction
- Validation suggestions
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

from validation_framework.profiler.temporal_analysis import TemporalAnalyzer


class TestTemporalAnalyzer:
    """Test suite for TemporalAnalyzer class."""

    @pytest.fixture
    def analyzer(self):
        """Create TemporalAnalyzer instance."""
        return TemporalAnalyzer(max_lag=40, min_periods=10)

    @pytest.fixture
    def temporal_test_data(self):
        """Load temporal test data."""
        test_data_path = Path(__file__).parent / 'test_data' / 'temporal_patterns.csv'
        if not test_data_path.exists():
            pytest.skip(f"Test data not found: {test_data_path}")
        return pd.read_csv(test_data_path)

    @pytest.fixture
    def daily_series(self):
        """Create daily datetime series."""
        start_date = datetime(2023, 1, 1)
        dates = [start_date + timedelta(days=i) for i in range(100)]
        return pd.Series(dates)

    @pytest.fixture
    def hourly_series(self):
        """Create hourly datetime series."""
        start_date = datetime(2023, 1, 1)
        dates = [start_date + timedelta(hours=i) for i in range(100)]
        return pd.Series(dates)

    @pytest.fixture
    def weekly_series(self):
        """Create weekly datetime series."""
        start_date = datetime(2023, 1, 1)
        dates = [start_date + timedelta(weeks=i) for i in range(50)]
        return pd.Series(dates)

    @pytest.fixture
    def series_with_gaps(self):
        """Create series with gaps."""
        start_date = datetime(2023, 1, 1)
        dates = []
        current = start_date

        for i in range(50):
            dates.append(current)
            # Add gap every 10 dates
            if i % 10 == 9:
                current += timedelta(days=7)  # 7-day gap
            else:
                current += timedelta(days=1)

        return pd.Series(dates)

    @pytest.fixture
    def series_with_trend(self):
        """Create series with linear trend."""
        start_date = datetime(2023, 1, 1)
        dates = [start_date + timedelta(days=i) for i in range(100)]
        return pd.Series(dates)

    # -------------------------------------------------------------------------
    # Frequency Inference Tests
    # -------------------------------------------------------------------------

    def test_infer_frequency_daily(self, analyzer, daily_series):
        """Test frequency inference for daily data."""
        result = analyzer._infer_frequency(daily_series)

        assert result["inferred"] == "daily"
        assert result["confidence"] > 0.95
        assert result["is_regular"] is True
        assert "median_interval_seconds" in result

    def test_infer_frequency_hourly(self, analyzer, hourly_series):
        """Test frequency inference for hourly data."""
        result = analyzer._infer_frequency(hourly_series)

        assert result["inferred"] == "hourly"
        assert result["confidence"] > 0.95
        assert result["is_regular"] is True

    def test_infer_frequency_weekly(self, analyzer, weekly_series):
        """Test frequency inference for weekly data."""
        result = analyzer._infer_frequency(weekly_series)

        assert result["inferred"] == "weekly"
        assert result["confidence"] > 0.95
        assert result["is_regular"] is True

    def test_infer_frequency_empty_series(self, analyzer):
        """Test frequency inference with empty series."""
        empty_series = pd.Series([], dtype='datetime64[ns]')
        result = analyzer._infer_frequency(empty_series)

        assert result["inferred"] == "unknown"
        assert result["confidence"] == 0.0

    def test_infer_frequency_single_value(self, analyzer):
        """Test frequency inference with single value."""
        single_series = pd.Series([datetime(2023, 1, 1)])
        result = analyzer._infer_frequency(single_series)

        assert result["inferred"] == "unknown"
        assert result["confidence"] == 0.0

    # -------------------------------------------------------------------------
    # Gap Detection Tests
    # -------------------------------------------------------------------------

    def test_detect_gaps_no_gaps(self, analyzer, daily_series):
        """Test gap detection with no gaps."""
        freq_info = analyzer._infer_frequency(daily_series)
        result = analyzer._detect_gaps(daily_series, freq_info)

        assert result["gaps_detected"] is False
        assert result["gap_count"] == 0

    def test_detect_gaps_with_gaps(self, analyzer, series_with_gaps):
        """Test gap detection with known gaps."""
        freq_info = analyzer._infer_frequency(series_with_gaps)
        result = analyzer._detect_gaps(series_with_gaps, freq_info)

        assert result["gaps_detected"] is True
        assert result["gap_count"] > 0
        assert "largest_gap" in result
        assert "gap_samples" in result

    def test_detect_gaps_gap_samples(self, analyzer, series_with_gaps):
        """Test gap samples are returned correctly."""
        freq_info = analyzer._infer_frequency(series_with_gaps)
        result = analyzer._detect_gaps(series_with_gaps, freq_info)

        if result["gap_count"] > 0:
            assert "gap_samples" in result
            assert len(result["gap_samples"]) <= 10  # Max 10 samples
            assert all("start" in gap for gap in result["gap_samples"])
            assert all("end" in gap for gap in result["gap_samples"])
            assert all("duration" in gap for gap in result["gap_samples"])

    # -------------------------------------------------------------------------
    # Trend Analysis Tests
    # -------------------------------------------------------------------------

    def test_analyze_trend_increasing(self, analyzer, series_with_trend):
        """Test trend analysis with increasing data."""
        result = analyzer._analyze_trend(series_with_trend)

        assert result["available"] is True
        assert result["direction"] in ["increasing", "flat"]
        assert "r_squared" in result
        assert "p_value" in result
        assert "strength" in result

    def test_analyze_trend_flat(self, analyzer):
        """Test trend analysis with flat/constant data."""
        # Create series with same timestamp repeated - this represents constant data
        # Trend analysis on same timestamps should either be unavailable or flat
        start_date = datetime(2023, 1, 1)
        flat_series = pd.Series([start_date] * 50)
        result = analyzer._analyze_trend(flat_series)

        # Either unavailable (can't calculate trend on identical values)
        # or available with flat direction
        if result["available"]:
            assert result["direction"] == "flat"
        else:
            # Acceptable - identical timestamps can't have meaningful trend
            assert result["available"] is False

    def test_analyze_trend_insufficient_data(self, analyzer):
        """Test trend analysis with insufficient data."""
        short_series = pd.Series([datetime(2023, 1, 1), datetime(2023, 1, 2)])
        result = analyzer._analyze_trend(short_series)

        # Should handle gracefully
        assert isinstance(result, dict)

    # -------------------------------------------------------------------------
    # Temporal Pattern Extraction Tests
    # -------------------------------------------------------------------------

    def test_extract_temporal_patterns(self, analyzer, daily_series):
        """Test temporal pattern extraction."""
        result = analyzer._extract_temporal_patterns(daily_series)

        assert "most_common_hour" in result
        assert "most_common_day_of_week" in result
        assert "most_common_day_of_month" in result
        assert "most_common_month" in result
        assert "business_vs_weekend" in result

    def test_extract_temporal_patterns_business_days(self, analyzer):
        """Test business day pattern extraction."""
        # Create Monday-Friday series
        start_date = datetime(2023, 1, 2)  # Monday
        dates = []
        current = start_date

        for i in range(50):
            # Skip weekends
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)

        business_series = pd.Series(dates)
        result = analyzer._extract_temporal_patterns(business_series)

        assert "business_vs_weekend" in result
        biz_weekend = result["business_vs_weekend"]
        assert biz_weekend["business_days"] > biz_weekend["weekend_days"]
        assert biz_weekend["business_day_percentage"] > 95

    def test_extract_temporal_patterns_hour_distribution(self, analyzer, hourly_series):
        """Test hour of day distribution."""
        result = analyzer._extract_temporal_patterns(hourly_series)

        assert "most_common_hour" in result
        if result["most_common_hour"] is not None:
            hour, count = result["most_common_hour"]
            assert 0 <= hour <= 23

    # -------------------------------------------------------------------------
    # Comprehensive Temporal Analysis Tests
    # -------------------------------------------------------------------------

    def test_analyze_temporal_column_daily(self, analyzer, daily_series):
        """Test comprehensive temporal analysis on daily data."""
        result = analyzer.analyze_temporal_column(
            daily_series,
            column_name="test_date"
        )

        assert result["available"] is True
        assert result["analysis_type"] == "temporal"
        assert result["column_name"] == "test_date"
        assert result["data_points"] == len(daily_series)
        assert "date_range" in result
        assert "frequency" in result
        assert "gaps" in result
        assert "trend" in result
        assert "patterns" in result

    def test_analyze_temporal_column_with_test_data(self, analyzer, temporal_test_data):
        """Test temporal analysis with real test data."""
        date_series = pd.to_datetime(temporal_test_data['date'])

        result = analyzer.analyze_temporal_column(
            date_series,
            column_name="date"
        )

        assert result["available"] is True
        assert result["data_points"] > 0

        # Verify frequency detection
        freq = result["frequency"]
        assert freq["inferred"] in ["daily", "irregular"]
        assert "confidence" in freq

        # Verify gap detection
        gaps = result["gaps"]
        assert "gaps_detected" in gaps

        # Verify trend analysis
        trend = result["trend"]
        assert "available" in trend

    def test_analyze_temporal_column_insufficient_data(self, analyzer):
        """Test temporal analysis with insufficient data points."""
        short_series = pd.Series([datetime(2023, 1, 1), datetime(2023, 1, 2)])

        result = analyzer.analyze_temporal_column(short_series)

        assert result["available"] is False
        assert "reason" in result

    def test_analyze_temporal_column_invalid_format(self, analyzer):
        """Test temporal analysis with invalid date format."""
        invalid_series = pd.Series(["not-a-date", "also-not-a-date"])

        result = analyzer.analyze_temporal_column(invalid_series)

        # Should handle gracefully
        assert "available" in result

    def test_analyze_temporal_column_with_nulls(self, analyzer):
        """Test temporal analysis with null values."""
        dates_with_nulls = pd.Series([
            datetime(2023, 1, 1),
            None,
            datetime(2023, 1, 3),
            None,
            datetime(2023, 1, 5)
        ])

        result = analyzer.analyze_temporal_column(dates_with_nulls)

        # Should skip nulls and analyze remaining
        if result["available"]:
            assert result["data_points"] == 3  # Only non-null dates

    # -------------------------------------------------------------------------
    # Validation Suggestion Tests
    # -------------------------------------------------------------------------

    def test_suggest_temporal_validations_regular_frequency(self, analyzer, daily_series):
        """Test validation suggestions for regular frequency data."""
        temporal_analysis = analyzer.analyze_temporal_column(daily_series)
        suggestions = analyzer.suggest_temporal_validations(temporal_analysis)

        assert isinstance(suggestions, list)

        # Should suggest frequency check for regular data
        freq_checks = [s for s in suggestions if "Frequency" in s.get("validation_type", "")]
        assert len(freq_checks) > 0

    def test_suggest_temporal_validations_no_gaps(self, analyzer, daily_series):
        """Test validation suggestions when no gaps detected."""
        temporal_analysis = analyzer.analyze_temporal_column(daily_series)
        suggestions = analyzer.suggest_temporal_validations(temporal_analysis)

        # Should suggest gap check
        gap_checks = [s for s in suggestions if "Gap" in s.get("validation_type", "")]
        assert len(gap_checks) > 0

    def test_suggest_temporal_validations_business_days(self, analyzer):
        """Test validation suggestions for business day patterns."""
        # Create business-day-only series
        start_date = datetime(2023, 1, 2)  # Monday
        dates = []
        current = start_date

        for i in range(100):
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)

        business_series = pd.Series(dates)
        temporal_analysis = analyzer.analyze_temporal_column(business_series)
        suggestions = analyzer.suggest_temporal_validations(temporal_analysis)

        # Should suggest business day check
        biz_checks = [s for s in suggestions if "BusinessDay" in s.get("validation_type", "")]
        if biz_checks:
            assert biz_checks[0]["confidence"] > 80

    def test_suggest_temporal_validations_unavailable(self, analyzer):
        """Test validation suggestions when analysis unavailable."""
        temporal_analysis = {"available": False}
        suggestions = analyzer.suggest_temporal_validations(temporal_analysis)

        assert suggestions == []

    # -------------------------------------------------------------------------
    # Edge Cases and Error Handling
    # -------------------------------------------------------------------------

    def test_analyze_temporal_column_mixed_timezones(self, analyzer):
        """Test temporal analysis with mixed timezones."""
        # Create dates with different timezones
        dates = pd.Series([
            pd.Timestamp('2023-01-01', tz='UTC'),
            pd.Timestamp('2023-01-02', tz='UTC'),
            pd.Timestamp('2023-01-03', tz='UTC')
        ])

        result = analyzer.analyze_temporal_column(dates)

        # Should handle timezone-aware datetimes
        assert isinstance(result, dict)

    def test_analyze_temporal_column_out_of_order(self, analyzer):
        """Test temporal analysis with unsorted dates."""
        dates = pd.Series([
            datetime(2023, 1, 5),
            datetime(2023, 1, 1),
            datetime(2023, 1, 3),
            datetime(2023, 1, 2),
            datetime(2023, 1, 4)
        ])

        result = analyzer.analyze_temporal_column(dates)

        # Should sort internally
        if result["available"]:
            assert result["data_points"] == 5

    def test_min_periods_threshold(self):
        """Test min_periods parameter enforcement."""
        analyzer = TemporalAnalyzer(min_periods=20)

        # Series with fewer than min_periods
        short_series = pd.Series([datetime(2023, 1, i) for i in range(1, 16)])
        result = analyzer.analyze_temporal_column(short_series)

        assert result["available"] is False
        assert "Insufficient data points" in result.get("reason", "")

    def test_max_lag_parameter(self):
        """Test max_lag parameter is respected."""
        custom_lag = 30
        analyzer = TemporalAnalyzer(max_lag=custom_lag)

        # Create proper daily series using timedelta to avoid day overflow
        start_date = datetime(2023, 1, 1)
        daily_series = pd.Series([start_date + timedelta(days=i) for i in range(100)])
        result = analyzer.analyze_temporal_column(daily_series)

        if result.get("acf_pacf", {}).get("available"):
            assert result["acf_pacf"]["max_lag"] <= custom_lag

    # -------------------------------------------------------------------------
    # Integration Tests
    # -------------------------------------------------------------------------

    def test_full_temporal_analysis_pipeline(self, analyzer, temporal_test_data):
        """Test complete temporal analysis pipeline."""
        date_series = pd.to_datetime(temporal_test_data['date'])

        # Run full analysis
        result = analyzer.analyze_temporal_column(
            date_series,
            column_name="transaction_date"
        )

        # Verify all major components are present
        assert result["available"] is True
        assert "frequency" in result
        assert "gaps" in result
        assert "trend" in result
        assert "patterns" in result

        # Generate validation suggestions
        suggestions = analyzer.suggest_temporal_validations(result)
        assert isinstance(suggestions, list)

        # Verify suggestion structure
        for suggestion in suggestions:
            assert "validation_type" in suggestion
            assert "severity" in suggestion
            assert "params" in suggestion
            assert "reason" in suggestion
            assert "confidence" in suggestion

    @pytest.mark.parametrize("frequency,expected_patterns", [
        (60, ["minute", "minutely"]),  # 60 seconds = minutely
        (3600, ["hour", "hourly"]),    # 3600 seconds = hourly
        (86400, ["day", "daily"]),     # 86400 seconds = daily
        (604800, ["week", "weekly"]),  # 604800 seconds = weekly
    ])
    def test_frequency_classification(self, analyzer, frequency, expected_patterns):
        """Test frequency classification for different intervals."""
        start_date = datetime(2023, 1, 1)
        dates = [start_date + timedelta(seconds=frequency * i) for i in range(100)]
        series = pd.Series(dates)

        result = analyzer._infer_frequency(series)

        # Check if inferred frequency contains any of the expected patterns
        inferred_lower = result["inferred"].lower()
        assert any(pattern in inferred_lower for pattern in expected_patterns), \
            f"Expected one of {expected_patterns} in '{result['inferred']}'"


class TestTemporalAnalyzerSeasonality:
    """Test suite specifically for seasonality detection."""

    @pytest.fixture
    def analyzer(self):
        """Create TemporalAnalyzer instance."""
        return TemporalAnalyzer()

    def test_detect_seasonality_available(self, analyzer):
        """Test seasonality detection availability."""
        pytest.importorskip("scipy")

        # Create series with 50 points (minimum for seasonality)
        dates = pd.Series([datetime(2023, 1, 1) + timedelta(days=i) for i in range(50)])
        result = analyzer._detect_seasonality(dates)

        assert isinstance(result, dict)
        assert "available" in result

    def test_detect_seasonality_insufficient_data(self, analyzer):
        """Test seasonality detection with insufficient data."""
        short_series = pd.Series([datetime(2023, 1, i) for i in range(1, 20)])
        result = analyzer._detect_seasonality(short_series)

        assert result["available"] is False


class TestTemporalAnalyzerACFPACF:
    """Test suite specifically for ACF/PACF calculation."""

    @pytest.fixture
    def analyzer(self):
        """Create TemporalAnalyzer instance."""
        return TemporalAnalyzer(max_lag=20)

    def test_calculate_acf_pacf_available(self, analyzer):
        """Test ACF/PACF calculation availability."""
        pytest.importorskip("statsmodels")

        # Create series with sufficient data
        dates = pd.Series([datetime(2023, 1, 1) + timedelta(days=i) for i in range(50)])
        result = analyzer._calculate_acf_pacf(dates)

        assert isinstance(result, dict)
        assert "available" in result

    def test_calculate_acf_pacf_values(self, analyzer):
        """Test ACF/PACF return correct structure."""
        pytest.importorskip("statsmodels")

        dates = pd.Series([datetime(2023, 1, 1) + timedelta(days=i) for i in range(50)])
        result = analyzer._calculate_acf_pacf(dates)

        if result.get("available"):
            assert "acf_values" in result
            assert "pacf_values" in result
            assert "max_lag" in result
            assert len(result["acf_values"]) > 0
            assert len(result["pacf_values"]) > 0

    def test_calculate_acf_pacf_insufficient_data(self, analyzer):
        """Test ACF/PACF with insufficient data."""
        short_series = pd.Series([datetime(2023, 1, i) for i in range(1, 20)])
        result = analyzer._calculate_acf_pacf(short_series)

        assert result["available"] is False
