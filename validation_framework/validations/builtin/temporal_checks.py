"""
Temporal and historical validation rules.

These validations compare current data against historical baselines:
- Baseline comparison (row counts, sums, averages)
- Trend detection (growth/decline rates)
- Seasonality validation
- Data freshness checks with historical context
"""

from typing import Iterator, Dict, Any, List
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from validation_framework.validations.base import DataValidationRule, FileValidationRule, ValidationResult
import logging

logger = logging.getLogger(__name__)


class BaselineComparisonCheck(DataValidationRule):
    """
    Compares current data metrics against historical baseline.

    Validates that current aggregates (count, sum, mean, etc.) are within
    acceptable range compared to historical average.

    Configuration:
        params:
            metric (str, required): Metric to compare: count, sum, mean, min, max
            column (str, optional): Column to aggregate (not needed for count)
            baseline_file (str, required): Path to historical baseline file (CSV with date and value columns)
            baseline_date_column (str, optional): Date column in baseline file. Default: "date"
            baseline_value_column (str, optional): Value column in baseline file. Default: "value"
            lookback_days (int, optional): Number of days to average for baseline. Default: 30
            tolerance_pct (float, required): Allowed deviation percentage from baseline

    Example YAML:
        # Validate row count is within 20% of 30-day average
        - type: "BaselineComparisonCheck"
          severity: "WARNING"
          params:
            metric: "count"
            baseline_file: "historical_counts.csv"
            lookback_days: 30
            tolerance_pct: 20

        # Validate total sales within 15% of historical average
        - type: "BaselineComparisonCheck"
          severity: "ERROR"
          params:
            metric: "sum"
            column: "sale_amount"
            baseline_file: "sales_baseline.csv"
            baseline_date_column: "date"
            baseline_value_column: "total_sales"
            lookback_days: 90
            tolerance_pct: 15
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        metric = self.params.get("metric", "?")
        column = self.params.get("column", "rows")
        tolerance = self.params.get("tolerance_pct", "?")

        return f"Compares {metric}({column}) against historical baseline (±{tolerance}%)"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Compare current metric against historical baseline.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Must contain 'file_path' or 'base_path' for resolving baseline file

        Returns:
            ValidationResult indicating if current value is within acceptable range
        """
        try:
            # Get parameters
            metric = self.params.get("metric")
            column = self.params.get("column")
            baseline_file = self.params.get("baseline_file")
            date_col = self.params.get("baseline_date_column", "date")
            value_col = self.params.get("baseline_value_column", "value")
            lookback_days = self.params.get("lookback_days", 30)
            tolerance_pct = self.params.get("tolerance_pct")

            # Validate required parameters
            if not metric:
                return self._create_result(
                    passed=False,
                    message="Parameter 'metric' is required",
                    failed_count=1,
                )

            if metric != "count" and not column:
                return self._create_result(
                    passed=False,
                    message="Parameter 'column' is required for non-count metrics",
                    failed_count=1,
                )

            if not baseline_file:
                return self._create_result(
                    passed=False,
                    message="Parameter 'baseline_file' is required",
                    failed_count=1,
                )

            if tolerance_pct is None:
                return self._create_result(
                    passed=False,
                    message="Parameter 'tolerance_pct' is required",
                    failed_count=1,
                )

            # Calculate current metric
            current_value = self._calculate_metric(data_iterator, metric, column)

            if current_value is None:
                return self._create_result(
                    passed=False,
                    message=f"Failed to calculate current {metric}",
                    failed_count=1,
                )

            # Load and calculate baseline
            baseline_path = self._resolve_path(baseline_file, context)
            if not Path(baseline_path).exists():
                return self._create_result(
                    passed=False,
                    message=f"Baseline file not found: {baseline_path}",
                    failed_count=1,
                )

            baseline_value = self._calculate_baseline(
                baseline_path,
                date_col,
                value_col,
                lookback_days
            )

            if baseline_value is None:
                return self._create_result(
                    passed=False,
                    message="Failed to calculate baseline",
                    failed_count=1,
                )

            # Compare against baseline
            deviation_pct = abs((current_value - baseline_value) / baseline_value * 100)
            within_tolerance = deviation_pct <= tolerance_pct

            if within_tolerance:
                return self._create_result(
                    passed=True,
                    message=f"Current {metric} ({current_value:.2f}) is within {tolerance_pct}% of baseline ({baseline_value:.2f}). Deviation: {deviation_pct:.2f}%",
                    total_count=1,
                )
            else:
                return self._create_result(
                    passed=False,
                    message=f"Current {metric} ({current_value:.2f}) deviates {deviation_pct:.2f}% from baseline ({baseline_value:.2f}), exceeding {tolerance_pct}% tolerance",
                    failed_count=1,
                    total_count=1,
                )

        except Exception as e:
            logger.error(f"Error in BaselineComparisonCheck: {str(e)}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error comparing against baseline: {str(e)}",
                failed_count=1,
            )

    def _calculate_metric(self, data_iterator: Iterator[pd.DataFrame], metric: str, column: str = None) -> float:
        """Calculate metric from data."""
        try:
            if metric == "count":
                total = 0
                for chunk in data_iterator:
                    total += len(chunk)
                return total

            elif metric == "sum":
                total = 0
                for chunk in data_iterator:
                    if column not in chunk.columns:
                        logger.error(f"Column '{column}' not found")
                        return None
                    total += chunk[column].sum()
                return total

            elif metric == "mean":
                total_sum = 0
                total_count = 0
                for chunk in data_iterator:
                    if column not in chunk.columns:
                        logger.error(f"Column '{column}' not found")
                        return None
                    valid_values = chunk[column].dropna()
                    total_sum += valid_values.sum()
                    total_count += len(valid_values)
                return total_sum / total_count if total_count > 0 else 0

            elif metric == "min":
                min_val = float('inf')
                for chunk in data_iterator:
                    if column not in chunk.columns:
                        logger.error(f"Column '{column}' not found")
                        return None
                    chunk_min = chunk[column].min()
                    if pd.notna(chunk_min) and chunk_min < min_val:
                        min_val = chunk_min
                return min_val if min_val != float('inf') else None

            elif metric == "max":
                max_val = float('-inf')
                for chunk in data_iterator:
                    if column not in chunk.columns:
                        logger.error(f"Column '{column}' not found")
                        return None
                    chunk_max = chunk[column].max()
                    if pd.notna(chunk_max) and chunk_max > max_val:
                        max_val = chunk_max
                return max_val if max_val != float('-inf') else None

            else:
                logger.error(f"Unsupported metric: {metric}")
                return None

        except Exception as e:
            logger.error(f"Error calculating metric: {str(e)}")
            return None

    def _calculate_baseline(
        self,
        baseline_file: str,
        date_col: str,
        value_col: str,
        lookback_days: int
    ) -> float:
        """Calculate baseline from historical data."""
        try:
            # Load baseline data
            baseline_df = pd.read_csv(baseline_file)

            # Validate columns exist
            if date_col not in baseline_df.columns:
                logger.error(f"Date column '{date_col}' not found in baseline file")
                return None

            if value_col not in baseline_df.columns:
                logger.error(f"Value column '{value_col}' not found in baseline file")
                return None

            # Parse dates
            baseline_df[date_col] = pd.to_datetime(baseline_df[date_col])

            # Filter to lookback period
            cutoff_date = datetime.now() - timedelta(days=lookback_days)
            recent_data = baseline_df[baseline_df[date_col] >= cutoff_date]

            if len(recent_data) == 0:
                logger.warning(f"No baseline data found in last {lookback_days} days")
                # Fall back to all available data
                recent_data = baseline_df

            # Calculate average
            baseline_value = recent_data[value_col].mean()
            return baseline_value

        except Exception as e:
            logger.error(f"Error calculating baseline: {str(e)}")
            return None

    def _resolve_path(self, file_path: str, context: Dict[str, Any]) -> str:
        """Resolve file path."""
        path = Path(file_path)

        if path.is_absolute():
            return str(path)

        current_file = context.get("file_path")
        if current_file:
            base_dir = Path(current_file).parent
            resolved = base_dir / path
            if resolved.exists():
                return str(resolved)

        base_path = context.get("base_path")
        if base_path:
            resolved = Path(base_path) / path
            if resolved.exists():
                return str(resolved)

        return str(path)


class TrendDetectionCheck(DataValidationRule):
    """
    Detects unusual trends in data compared to historical patterns.

    Validates that growth/decline rates are within expected ranges based on
    historical trends.

    Configuration:
        params:
            metric (str, required): Metric to track: count, sum, mean
            column (str, optional): Column to aggregate (not needed for count)
            baseline_file (str, required): Historical data file
            baseline_date_column (str, optional): Date column. Default: "date"
            baseline_value_column (str, optional): Value column. Default: "value"
            max_growth_pct (float, optional): Maximum allowed growth percentage
            max_decline_pct (float, optional): Maximum allowed decline percentage
            comparison_period (int, optional): Days to compare against. Default: 1 (yesterday)

    Example YAML:
        # Detect unusual growth in customer count
        - type: "TrendDetectionCheck"
          severity: "WARNING"
          params:
            metric: "count"
            baseline_file: "daily_customer_counts.csv"
            max_growth_pct: 50  # Alert if >50% growth
            max_decline_pct: 30  # Alert if >30% decline
            comparison_period: 1  # Compare to yesterday

        # Detect unusual sales changes
        - type: "TrendDetectionCheck"
          severity: "WARNING"
          params:
            metric: "sum"
            column: "sale_amount"
            baseline_file: "daily_sales.csv"
            max_growth_pct: 100
            max_decline_pct: 50
            comparison_period: 7  # Compare to 7 days ago
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        metric = self.params.get("metric", "?")
        column = self.params.get("column", "rows")

        return f"Detects unusual trends in {metric}({column})"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Detect unusual trends.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Must contain 'file_path' or 'base_path'

        Returns:
            ValidationResult indicating if trend is normal
        """
        try:
            # Get parameters
            metric = self.params.get("metric")
            column = self.params.get("column")
            baseline_file = self.params.get("baseline_file")
            date_col = self.params.get("baseline_date_column", "date")
            value_col = self.params.get("baseline_value_column", "value")
            max_growth = self.params.get("max_growth_pct")
            max_decline = self.params.get("max_decline_pct")
            comparison_period = self.params.get("comparison_period", 1)

            # Validate required parameters
            if not metric:
                return self._create_result(
                    passed=False,
                    message="Parameter 'metric' is required",
                    failed_count=1,
                )

            if not baseline_file:
                return self._create_result(
                    passed=False,
                    message="Parameter 'baseline_file' is required",
                    failed_count=1,
                )

            if max_growth is None and max_decline is None:
                return self._create_result(
                    passed=False,
                    message="At least one of 'max_growth_pct' or 'max_decline_pct' must be specified",
                    failed_count=1,
                )

            # Calculate current metric
            current_value = self._calculate_metric(data_iterator, metric, column)

            if current_value is None:
                return self._create_result(
                    passed=False,
                    message=f"Failed to calculate current {metric}",
                    failed_count=1,
                )

            # Load historical value for comparison
            baseline_path = self._resolve_path(baseline_file, context)
            if not Path(baseline_path).exists():
                return self._create_result(
                    passed=False,
                    message=f"Baseline file not found: {baseline_path}",
                    failed_count=1,
                )

            comparison_value = self._get_historical_value(
                baseline_path,
                date_col,
                value_col,
                comparison_period
            )

            if comparison_value is None or comparison_value == 0:
                return self._create_result(
                    passed=False,
                    message=f"No historical data found for {comparison_period} days ago",
                    failed_count=1,
                )

            # Calculate growth/decline percentage
            change_pct = ((current_value - comparison_value) / comparison_value) * 100

            # Check against thresholds
            issues = []

            if max_growth is not None and change_pct > max_growth:
                issues.append(f"Growth of {change_pct:.2f}% exceeds maximum {max_growth}%")

            if max_decline is not None and change_pct < -max_decline:
                issues.append(f"Decline of {abs(change_pct):.2f}% exceeds maximum {max_decline}%")

            if issues:
                return self._create_result(
                    passed=False,
                    message=f"Unusual trend detected: {'; '.join(issues)}. Current: {current_value:.2f}, Comparison: {comparison_value:.2f}",
                    failed_count=1,
                    total_count=1,
                )

            return self._create_result(
                passed=True,
                message=f"Trend is normal: {change_pct:+.2f}% change. Current: {current_value:.2f}, Comparison: {comparison_value:.2f}",
                total_count=1,
            )

        except Exception as e:
            logger.error(f"Error in TrendDetectionCheck: {str(e)}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error detecting trends: {str(e)}",
                failed_count=1,
            )

    def _calculate_metric(self, data_iterator: Iterator[pd.DataFrame], metric: str, column: str = None) -> float:
        """Calculate metric (same as BaselineComparisonCheck)."""
        try:
            if metric == "count":
                total = 0
                for chunk in data_iterator:
                    total += len(chunk)
                return total

            elif metric in ["sum", "mean", "min", "max"]:
                # Similar implementation as BaselineComparisonCheck
                if metric == "sum":
                    total = 0
                    for chunk in data_iterator:
                        if column not in chunk.columns:
                            return None
                        total += chunk[column].sum()
                    return total

                elif metric == "mean":
                    total_sum = 0
                    total_count = 0
                    for chunk in data_iterator:
                        if column not in chunk.columns:
                            return None
                        valid_values = chunk[column].dropna()
                        total_sum += valid_values.sum()
                        total_count += len(valid_values)
                    return total_sum / total_count if total_count > 0 else 0

            return None

        except Exception as e:
            logger.error(f"Error calculating metric: {str(e)}")
            return None

    def _get_historical_value(
        self,
        baseline_file: str,
        date_col: str,
        value_col: str,
        days_ago: int
    ) -> float:
        """Get historical value from N days ago."""
        try:
            # Load baseline data
            baseline_df = pd.read_csv(baseline_file)

            # Parse dates
            baseline_df[date_col] = pd.to_datetime(baseline_df[date_col])

            # Find value from N days ago
            target_date = datetime.now() - timedelta(days=days_ago)

            # Find closest date (within 1 day tolerance)
            baseline_df['date_diff'] = (baseline_df[date_col] - target_date).abs()
            closest = baseline_df[baseline_df['date_diff'] <= timedelta(days=1)].sort_values('date_diff').head(1)

            if len(closest) == 0:
                logger.warning(f"No historical data found near {target_date.date()}")
                return None

            return closest[value_col].iloc[0]

        except Exception as e:
            logger.error(f"Error getting historical value: {str(e)}")
            return None

    def _resolve_path(self, file_path: str, context: Dict[str, Any]) -> str:
        """Resolve file path."""
        path = Path(file_path)

        if path.is_absolute():
            return str(path)

        current_file = context.get("file_path")
        if current_file:
            base_dir = Path(current_file).parent
            resolved = base_dir / path
            if resolved.exists():
                return str(resolved)

        base_path = context.get("base_path")
        if base_path:
            resolved = Path(base_path) / path
            if resolved.exists():
                return str(resolved)

        return str(path)
