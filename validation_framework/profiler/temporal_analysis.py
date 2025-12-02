"""
Temporal analysis for time-series data profiling.

Provides comprehensive time-series analysis including:
- Autocorrelation (ACF) and Partial Autocorrelation (PACF)
- Seasonality detection using Fourier transform
- Trend analysis and change point detection
- Temporal pattern recognition and gap detection
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
import re

from validation_framework.profiler.backend_aware_base import BackendAwareProfiler

try:
    from statsmodels.tsa.stattools import acf, pacf
    from statsmodels.tsa.seasonal import seasonal_decompose
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False
    logging.warning("statsmodels not available - temporal analysis will be limited")

try:
    from scipy.fft import fft, fftfreq
    from scipy.stats import linregress
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logging.warning("scipy not available - advanced temporal features disabled")

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None


logger = logging.getLogger(__name__)


class TemporalAnalyzer(BackendAwareProfiler):
    """
    Analyze time-series patterns in date/timestamp columns.

    Provides ACF/PACF analysis, seasonality detection, trend analysis,
    and temporal pattern recognition.
    """

    def __init__(self, max_lag: int = 40, min_periods: int = 10):
        """
        Initialize temporal analyzer.

        Args:
            max_lag: Maximum lag for ACF/PACF calculation (default: 40)
            min_periods: Minimum number of periods required for analysis (default: 10)
        """
        self.max_lag = max_lag
        self.min_periods = min_periods

    def analyze_temporal_column(
        self,
        series: Any,
        datetime_format: Optional[str] = None,
        column_name: str = "timestamp"
    ) -> Dict[str, Any]:
        """
        Comprehensive temporal analysis of a date/timestamp column.

        Args:
            series: Date/timestamp series (Polars or pandas)
            datetime_format: Optional datetime format string
            column_name: Column name for logging

        Returns:
            Dict with temporal analysis results including:
                - frequency: Inferred frequency (hourly, daily, weekly, etc.)
                - gaps: Missing timestamps
                - trend: Trend analysis results
                - seasonality: Seasonality detection results
                - acf_pacf: Autocorrelation analysis (if enough data points)
        """
        logger.info(f"Starting temporal analysis for column: {column_name}")

        result = {
            "available": True,
            "analysis_type": "temporal",
            "column_name": column_name
        }

        # Convert to pandas datetime series for analysis
        try:
            if self.is_polars(series):
                # Convert Polars to pandas for temporal analysis
                dt_series = series.to_pandas()
            else:
                dt_series = series.copy()

            # Ensure datetime type
            if not pd.api.types.is_datetime64_any_dtype(dt_series):
                dt_series = pd.to_datetime(dt_series, format=datetime_format, errors='coerce')

            # Remove nulls
            dt_series = dt_series.dropna()

            if len(dt_series) < self.min_periods:
                result["available"] = False
                result["reason"] = f"Insufficient data points ({len(dt_series)} < {self.min_periods})"
                return result

            # Sort timestamps
            dt_series = dt_series.sort_values().reset_index(drop=True)

            result["data_points"] = len(dt_series)
            result["date_range"] = {
                "start": dt_series.min().isoformat(),
                "end": dt_series.max().isoformat(),
                "span_days": (dt_series.max() - dt_series.min()).days
            }

        except Exception as e:
            logger.warning(f"Failed to convert column to datetime: {e}")
            result["available"] = False
            result["reason"] = f"Datetime conversion failed: {str(e)}"
            return result

        # Infer frequency
        result["frequency"] = self._infer_frequency(dt_series)

        # Detect gaps
        result["gaps"] = self._detect_gaps(dt_series, result["frequency"])

        # Trend analysis
        result["trend"] = self._analyze_trend(dt_series)

        # Seasonality detection (if scipy available and enough data)
        if SCIPY_AVAILABLE and len(dt_series) >= 30:
            result["seasonality"] = self._detect_seasonality(dt_series)

        # ACF/PACF analysis (if statsmodels available and enough data)
        if STATSMODELS_AVAILABLE and len(dt_series) >= 30:
            result["acf_pacf"] = self._calculate_acf_pacf(dt_series)

        # Temporal patterns
        result["patterns"] = self._extract_temporal_patterns(dt_series)

        logger.info(f"Temporal analysis complete for {column_name}")
        return result

    def _infer_frequency(self, dt_series: pd.Series) -> Dict[str, Any]:
        """
        Infer the frequency of timestamps.

        Args:
            dt_series: Sorted datetime series

        Returns:
            Dict with inferred frequency and confidence
        """
        if len(dt_series) < 2:
            return {"inferred": "unknown", "confidence": 0.0}

        # Calculate time differences
        diffs = dt_series.diff().dropna()

        if len(diffs) == 0:
            return {"inferred": "unknown", "confidence": 0.0}

        # Get median difference (more robust than mean)
        median_diff = diffs.median()

        # Classify frequency based on median difference
        seconds = median_diff.total_seconds()

        frequency_mapping = [
            (1, "second", "secondly"),
            (60, "minute", "minutely"),
            (3600, "hour", "hourly"),
            (86400, "day", "daily"),
            (604800, "week", "weekly"),
            (2592000, "month", "monthly"),  # ~30 days
            (31536000, "year", "yearly")
        ]

        inferred_freq = "irregular"
        for threshold, label, description in frequency_mapping:
            if seconds <= threshold * 1.5:  # Allow 50% tolerance
                inferred_freq = description
                break

        # Calculate consistency (how many diffs are close to median)
        tolerance = median_diff * 0.2  # 20% tolerance
        consistent_diffs = sum(abs(diff - median_diff) <= tolerance for diff in diffs)
        confidence = consistent_diffs / len(diffs) if len(diffs) > 0 else 0.0

        return {
            "inferred": inferred_freq,
            "median_interval": str(median_diff),
            "median_interval_seconds": seconds,
            "confidence": round(confidence, 3),
            "is_regular": confidence > 0.8
        }

    def _detect_gaps(
        self,
        dt_series: pd.Series,
        frequency_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Detect gaps in the time series.

        Args:
            dt_series: Sorted datetime series
            frequency_info: Frequency information from _infer_frequency

        Returns:
            Dict with gap detection results
        """
        if len(dt_series) < 2:
            return {"gaps_detected": False, "gap_count": 0}

        diffs = dt_series.diff().dropna()

        # Define gap threshold as 2x median interval
        median_seconds = frequency_info.get("median_interval_seconds", 86400)
        gap_threshold = timedelta(seconds=median_seconds * 2)

        # Find gaps
        gaps = diffs[diffs > gap_threshold]

        result = {
            "gaps_detected": len(gaps) > 0,
            "gap_count": len(gaps),
            "largest_gap": str(gaps.max()) if len(gaps) > 0 else None,
            "total_gap_time": str(gaps.sum()) if len(gaps) > 0 else None
        }

        # Sample gaps (up to 10)
        if len(gaps) > 0:
            gap_samples = []
            for idx in gaps.head(10).index:
                gap_samples.append({
                    "start": dt_series[idx - 1].isoformat(),
                    "end": dt_series[idx].isoformat(),
                    "duration": str(diffs[idx])
                })
            result["gap_samples"] = gap_samples

        return result

    def _analyze_trend(self, dt_series: pd.Series) -> Dict[str, Any]:
        """
        Analyze trend in the time series.

        Args:
            dt_series: Sorted datetime series

        Returns:
            Dict with trend analysis results
        """
        if not SCIPY_AVAILABLE or len(dt_series) < 3:
            return {"available": False}

        try:
            # Convert timestamps to numeric (seconds since first timestamp)
            x = (dt_series - dt_series.min()).dt.total_seconds().values
            y = np.arange(len(dt_series))  # Index as y-value for trend

            # Check if all x values are identical (can't calculate regression)
            if np.all(x == x[0]):
                return {
                    "available": True,
                    "direction": "constant",
                    "slope": 0.0,
                    "r_squared": 0.0,
                    "p_value": 1.0,
                    "is_significant": False,
                    "strength": "none",
                    "note": "All timestamp values are identical"
                }

            # Linear regression
            slope, intercept, r_value, p_value, std_err = linregress(x, y)

            # Interpret trend
            if abs(slope) < 1e-10:
                trend_direction = "flat"
            elif slope > 0:
                trend_direction = "increasing"
            else:
                trend_direction = "decreasing"

            return {
                "available": True,
                "direction": trend_direction,
                "slope": float(slope),
                "r_squared": float(r_value ** 2),
                "p_value": float(p_value),
                "is_significant": p_value < 0.05,
                "strength": "strong" if r_value ** 2 > 0.8 else "moderate" if r_value ** 2 > 0.5 else "weak"
            }

        except Exception as e:
            logger.warning(f"Trend analysis failed: {e}")
            return {"available": False, "error": str(e)}

    def _detect_seasonality(self, dt_series: pd.Series) -> Dict[str, Any]:
        """
        Detect seasonality using Fourier transform.

        Args:
            dt_series: Sorted datetime series

        Returns:
            Dict with seasonality detection results
        """
        if not SCIPY_AVAILABLE or len(dt_series) < 30:
            return {"available": False}

        try:
            # Create value series (index position) for FFT
            y = np.arange(len(dt_series))

            # Apply FFT
            fft_values = fft(y)
            frequencies = fftfreq(len(y))

            # Get magnitude spectrum
            magnitude = np.abs(fft_values)

            # Find dominant frequencies (exclude DC component at index 0)
            # Only look at positive frequencies
            positive_freq_idx = frequencies > 0
            positive_freqs = frequencies[positive_freq_idx]
            positive_magnitudes = magnitude[positive_freq_idx]

            # Find top 3 peaks
            top_indices = np.argsort(positive_magnitudes)[-3:][::-1]

            dominant_frequencies = []
            for idx in top_indices:
                freq = positive_freqs[idx]
                mag = positive_magnitudes[idx]

                # Convert frequency to period (number of data points)
                if freq > 0:
                    period = 1 / freq
                    dominant_frequencies.append({
                        "frequency": float(freq),
                        "period_data_points": float(period),
                        "magnitude": float(mag)
                    })

            return {
                "available": True,
                "dominant_frequencies": dominant_frequencies,
                "seasonality_detected": len(dominant_frequencies) > 0
            }

        except Exception as e:
            logger.warning(f"Seasonality detection failed: {e}")
            return {"available": False, "error": str(e)}

    def _calculate_acf_pacf(
        self,
        dt_series: pd.Series,
        alpha: float = 0.05
    ) -> Dict[str, Any]:
        """
        Calculate ACF and PACF for the time series.

        Args:
            dt_series: Sorted datetime series
            alpha: Significance level (default: 0.05)

        Returns:
            Dict with ACF/PACF values and significant lags
        """
        if not STATSMODELS_AVAILABLE or len(dt_series) < 30:
            return {"available": False}

        try:
            # Use index as the series for ACF/PACF
            # (We're analyzing the regularity of timestamps, not values)
            series_numeric = np.arange(len(dt_series))

            # Calculate max lag (minimum of max_lag or 1/3 of data length)
            max_lag_calc = min(self.max_lag, len(dt_series) // 3)

            # Calculate ACF
            acf_values = acf(series_numeric, nlags=max_lag_calc, alpha=alpha)
            acf_coef = acf_values[0] if isinstance(acf_values, tuple) else acf_values

            # Calculate PACF
            pacf_values = pacf(series_numeric, nlags=max_lag_calc, alpha=alpha)
            pacf_coef = pacf_values[0] if isinstance(pacf_values, tuple) else pacf_values

            # Find significant lags (|coefficient| > 2/sqrt(n))
            threshold = 2 / np.sqrt(len(dt_series))

            significant_acf_lags = [
                {"lag": i, "value": float(acf_coef[i])}
                for i in range(1, len(acf_coef))
                if abs(acf_coef[i]) > threshold
            ]

            significant_pacf_lags = [
                {"lag": i, "value": float(pacf_coef[i])}
                for i in range(1, len(pacf_coef))
                if abs(pacf_coef[i]) > threshold
            ]

            return {
                "available": True,
                "max_lag": max_lag_calc,
                "acf_values": [float(v) for v in acf_coef[:11]],  # First 10 lags
                "pacf_values": [float(v) for v in pacf_coef[:11]],
                "significant_acf_lags": significant_acf_lags[:10],  # Top 10
                "significant_pacf_lags": significant_pacf_lags[:10],
                "threshold": float(threshold)
            }

        except Exception as e:
            logger.warning(f"ACF/PACF calculation failed: {e}")
            return {"available": False, "error": str(e)}

    def _extract_temporal_patterns(self, dt_series: pd.Series) -> Dict[str, Any]:
        """
        Extract temporal patterns from timestamps.

        Args:
            dt_series: Sorted datetime series

        Returns:
            Dict with temporal pattern analysis
        """
        try:
            # Extract temporal components
            patterns = {
                "hour_of_day": {},
                "day_of_week": {},
                "day_of_month": {},
                "month_of_year": {},
                "quarter": {}
            }

            # Count occurrences by temporal component
            for ts in dt_series:
                patterns["hour_of_day"][ts.hour] = patterns["hour_of_day"].get(ts.hour, 0) + 1
                patterns["day_of_week"][ts.strftime("%A")] = patterns["day_of_week"].get(ts.strftime("%A"), 0) + 1
                patterns["day_of_month"][ts.day] = patterns["day_of_month"].get(ts.day, 0) + 1
                patterns["month_of_year"][ts.strftime("%B")] = patterns["month_of_year"].get(ts.strftime("%B"), 0) + 1
                patterns["quarter"][f"Q{(ts.month-1)//3 + 1}"] = patterns["quarter"].get(f"Q{(ts.month-1)//3 + 1}", 0) + 1

            # Identify most common patterns
            result = {
                "most_common_hour": max(patterns["hour_of_day"].items(), key=lambda x: x[1]) if patterns["hour_of_day"] else None,
                "most_common_day_of_week": max(patterns["day_of_week"].items(), key=lambda x: x[1]) if patterns["day_of_week"] else None,
                "most_common_day_of_month": max(patterns["day_of_month"].items(), key=lambda x: x[1]) if patterns["day_of_month"] else None,
                "most_common_month": max(patterns["month_of_year"].items(), key=lambda x: x[1]) if patterns["month_of_year"] else None,
                "most_common_quarter": max(patterns["quarter"].items(), key=lambda x: x[1]) if patterns["quarter"] else None
            }

            # Business day analysis
            business_days = sum(1 for ts in dt_series if ts.weekday() < 5)
            weekend_days = len(dt_series) - business_days

            result["business_vs_weekend"] = {
                "business_days": business_days,
                "weekend_days": weekend_days,
                "business_day_percentage": round(100 * business_days / len(dt_series), 2) if len(dt_series) > 0 else 0
            }

            return result

        except Exception as e:
            logger.warning(f"Pattern extraction failed: {e}")
            return {"error": str(e)}

    def suggest_temporal_validations(
        self,
        temporal_analysis: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Suggest temporal validations based on analysis results.

        Args:
            temporal_analysis: Results from analyze_temporal_column

        Returns:
            List of validation suggestions
        """
        suggestions = []

        if not temporal_analysis.get("available", False):
            return suggestions

        # Frequency consistency validation
        freq_info = temporal_analysis.get("frequency", {})
        if freq_info.get("is_regular", False):
            suggestions.append({
                "validation_type": "TemporalFrequencyCheck",
                "severity": "WARNING",
                "params": {
                    "expected_frequency": freq_info["inferred"],
                    "tolerance": 0.2
                },
                "reason": f"Data has regular {freq_info['inferred']} frequency with {freq_info['confidence']:.0%} consistency",
                "confidence": freq_info["confidence"] * 100
            })

        # Gap detection validation
        gaps = temporal_analysis.get("gaps", {})
        if not gaps.get("gaps_detected", False):
            suggestions.append({
                "validation_type": "TemporalGapCheck",
                "severity": "WARNING",
                "params": {
                    "max_gap": freq_info.get("median_interval", "1 day")
                },
                "reason": "No significant gaps detected in timeline",
                "confidence": 85.0
            })

        # Recency check
        date_range = temporal_analysis.get("date_range", {})
        if date_range:
            suggestions.append({
                "validation_type": "RecencyCheck",
                "severity": "WARNING",
                "params": {
                    "max_days_old": 90
                },
                "reason": f"Data spans from {date_range['start']} to {date_range['end']}",
                "confidence": 75.0
            })

        # Business day pattern
        patterns = temporal_analysis.get("patterns", {})
        biz_weekend = patterns.get("business_vs_weekend", {})
        if biz_weekend.get("business_day_percentage", 0) > 95:
            suggestions.append({
                "validation_type": "BusinessDayCheck",
                "severity": "INFO",
                "params": {
                    "require_business_days": True
                },
                "reason": f"{biz_weekend['business_day_percentage']:.0f}% of dates are business days",
                "confidence": 90.0
            })

        return suggestions
