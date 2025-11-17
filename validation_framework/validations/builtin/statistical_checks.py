"""
Advanced statistical validation rules.

These validations use statistical methods to detect data quality issues:
- Distribution testing (normality, uniformity)
- Correlation validation
- Seasonality detection
- Advanced anomaly detection

Author: Daniel Edge
"""

from typing import Iterator, Dict, Any
import pandas as pd
import numpy as np
from validation_framework.validations.base import DataValidationRule, ValidationResult
from validation_framework.core.exceptions import (
    ColumnNotFoundError,
    ParameterValidationError
)
from validation_framework.core.constants import (
    MAX_SAMPLE_FAILURES,
    MIN_SAMPLE_SIZE_FOR_STATS
)
import logging

logger = logging.getLogger(__name__)


class DistributionCheck(DataValidationRule):
    """
    Validates that data follows an expected statistical distribution.

    Useful for detecting when data patterns change unexpectedly.

    Configuration:
        params:
            column (str, required): Column to check
            expected_distribution (str, required): Expected distribution type: normal, uniform, exponential
            significance_level (float, optional): Significance level for statistical test. Default: 0.05
            min_sample_size (int, optional): Minimum samples required for test. Default: 30

    Example YAML:
        # Validate age follows normal distribution
        - type: "DistributionCheck"
          severity: "WARNING"
          params:
            column: "age"
            expected_distribution: "normal"
            significance_level: 0.05

        # Validate random IDs are uniformly distributed
        - type: "DistributionCheck"
          severity: "WARNING"
          params:
            column: "random_id"
            expected_distribution: "uniform"
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        column = self.params.get("column", "?")
        dist = self.params.get("expected_distribution", "?")

        return f"Validates that {column} follows {dist} distribution"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Test if data follows expected distribution.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult indicating if distribution test passed
        """
        try:
            # Import scipy for statistical tests
            try:
                from scipy import stats
            except ImportError:
                return self._create_result(
                    passed=False,
                    message="scipy is required for distribution tests. Install with: pip install scipy",
                    failed_count=1,
                )

            # Get parameters
            column = self.params.get("column")
            expected_dist = self.params.get("expected_distribution")
            alpha = self.params.get("significance_level", 0.05)
            min_samples = self.params.get("min_sample_size", MIN_SAMPLE_SIZE_FOR_STATS)

            # Validate required parameters
            if not column:
                raise ParameterValidationError(
                    "Parameter 'column' is required",
                    validation_name=self.name,
                    parameter="column",
                    value=None
                )

            if not expected_dist:
                raise ParameterValidationError(
                    "Parameter 'expected_distribution' is required",
                    validation_name=self.name,
                    parameter="expected_distribution",
                    value=None
                )

            # Collect all data (needed for distribution testing)
            all_data = []
            for chunk in data_iterator:
                if column not in chunk.columns:
                    raise ColumnNotFoundError(
                        validation_name=self.name,
                        column=column,
                        available_columns=list(chunk.columns)
                    )

                # Apply conditional filter if specified
                if self.condition:
                    mask = self._evaluate_condition(chunk)
                    chunk = chunk[mask]

                values = chunk[column].dropna()
                all_data.extend(values.tolist())

            if len(all_data) < min_samples:
                return self._create_result(
                    passed=False,
                    message=f"Insufficient data for distribution test: {len(all_data)} samples (minimum: {min_samples})",
                    failed_count=1,
                )

            # Perform distribution test
            data_array = np.array(all_data)

            if expected_dist.lower() == "normal":
                # Shapiro-Wilk test for normality
                if len(data_array) > 5000:
                    # Use Kolmogorov-Smirnov for large samples
                    statistic, p_value = stats.kstest(data_array, 'norm', args=(np.mean(data_array), np.std(data_array)))
                    test_name = "Kolmogorov-Smirnov"
                else:
                    statistic, p_value = stats.shapiro(data_array)
                    test_name = "Shapiro-Wilk"

            elif expected_dist.lower() == "uniform":
                # Kolmogorov-Smirnov test for uniformity
                statistic, p_value = stats.kstest(data_array, 'uniform',
                                                   args=(np.min(data_array), np.ptp(data_array)))
                test_name = "Kolmogorov-Smirnov"

            elif expected_dist.lower() == "exponential":
                # Kolmogorov-Smirnov test for exponential
                statistic, p_value = stats.kstest(data_array, 'expon',
                                                   args=(np.min(data_array), np.mean(data_array) - np.min(data_array)))
                test_name = "Kolmogorov-Smirnov"

            else:
                return self._create_result(
                    passed=False,
                    message=f"Unsupported distribution type: {expected_dist}. Supported: normal, uniform, exponential",
                    failed_count=1,
                )

            # Interpret results (p-value > alpha means distribution is likely correct)
            if p_value > alpha:
                return self._create_result(
                    passed=True,
                    message=f"Data follows {expected_dist} distribution ({test_name} p-value: {p_value:.4f}, alpha: {alpha})",
                    total_count=len(all_data),
                )
            else:
                return self._create_result(
                    passed=False,
                    message=f"Data does NOT follow {expected_dist} distribution ({test_name} p-value: {p_value:.4f}, alpha: {alpha})",
                    failed_count=1,
                    total_count=len(all_data),
                )

        except Exception as e:
            logger.error(f"Error in DistributionCheck: {str(e)}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error testing distribution: {str(e)}",
                failed_count=1,
            )


class CorrelationCheck(DataValidationRule):
    """
    Validates correlation between two columns is within expected range.

    Useful for detecting when expected relationships between variables break down.

    Configuration:
        params:
            column1 (str, required): First column
            column2 (str, required): Second column
            expected_correlation (float, optional): Expected correlation coefficient (-1 to 1)
            min_correlation (float, optional): Minimum acceptable correlation
            max_correlation (float, optional): Maximum acceptable correlation
            correlation_type (str, optional): Type: pearson, spearman, kendall. Default: pearson
            tolerance (float, optional): Tolerance for expected_correlation. Default: 0.1

    Example YAML:
        # Validate price and quantity have negative correlation
        - type: "CorrelationCheck"
          severity: "WARNING"
          params:
            column1: "price"
            column2: "quantity_sold"
            min_correlation: -0.8
            max_correlation: -0.3

        # Validate temperature and ice_cream_sales are positively correlated
        - type: "CorrelationCheck"
          severity: "WARNING"
          params:
            column1: "temperature"
            column2: "ice_cream_sales"
            expected_correlation: 0.7
            tolerance: 0.15
            correlation_type: "spearman"
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        col1 = self.params.get("column1", "?")
        col2 = self.params.get("column2", "?")

        return f"Validates correlation between {col1} and {col2}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Check correlation between columns.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult indicating if correlation is acceptable
        """
        try:
            # Import scipy
            try:
                from scipy import stats
            except ImportError:
                return self._create_result(
                    passed=False,
                    message="scipy is required for correlation tests. Install with: pip install scipy",
                    failed_count=1,
                )

            # Get parameters
            column1 = self.params.get("column1")
            column2 = self.params.get("column2")
            expected_corr = self.params.get("expected_correlation")
            min_corr = self.params.get("min_correlation")
            max_corr = self.params.get("max_correlation")
            corr_type = self.params.get("correlation_type", "pearson")
            tolerance = self.params.get("tolerance", 0.1)

            # Validate required parameters
            if not column1 or not column2:
                return self._create_result(
                    passed=False,
                    message="Parameters 'column1' and 'column2' are required",
                    failed_count=1,
                )

            if expected_corr is None and min_corr is None and max_corr is None:
                return self._create_result(
                    passed=False,
                    message="At least one of 'expected_correlation', 'min_correlation', or 'max_correlation' is required",
                    failed_count=1,
                )

            # Collect data
            col1_data = []
            col2_data = []

            for chunk in data_iterator:
                # Check columns exist
                if column1 not in chunk.columns:
                    raise ColumnNotFoundError(
                        validation_name=self.name,
                        column=column1,
                        available_columns=list(chunk.columns)
                    )

                if column2 not in chunk.columns:
                    raise ColumnNotFoundError(
                        validation_name=self.name,
                        column=column2,
                        available_columns=list(chunk.columns)
                    )

                # Apply conditional filter if specified
                if self.condition:
                    mask = self._evaluate_condition(chunk)
                    chunk = chunk[mask]

                # Get valid pairs (both non-null)
                valid_data = chunk[[column1, column2]].dropna()
                col1_data.extend(valid_data[column1].tolist())
                col2_data.extend(valid_data[column2].tolist())

            if len(col1_data) < 3:
                return self._create_result(
                    passed=False,
                    message=f"Insufficient data for correlation test: {len(col1_data)} pairs (minimum: 3)",
                    failed_count=1,
                )

            # Calculate correlation
            if corr_type.lower() == "pearson":
                correlation, p_value = stats.pearsonr(col1_data, col2_data)
            elif corr_type.lower() == "spearman":
                correlation, p_value = stats.spearmanr(col1_data, col2_data)
            elif corr_type.lower() == "kendall":
                correlation, p_value = stats.kendalltau(col1_data, col2_data)
            else:
                return self._create_result(
                    passed=False,
                    message=f"Unsupported correlation type: {corr_type}. Supported: pearson, spearman, kendall",
                    failed_count=1,
                )

            # Check against thresholds
            issues = []

            if expected_corr is not None:
                if abs(correlation - expected_corr) > tolerance:
                    issues.append(f"Correlation {correlation:.3f} deviates from expected {expected_corr:.3f} by more than {tolerance}")

            if min_corr is not None and correlation < min_corr:
                issues.append(f"Correlation {correlation:.3f} is below minimum {min_corr}")

            if max_corr is not None and correlation > max_corr:
                issues.append(f"Correlation {correlation:.3f} exceeds maximum {max_corr}")

            if issues:
                return self._create_result(
                    passed=False,
                    message=f"Correlation check failed: {'; '.join(issues)} (p-value: {p_value:.4f})",
                    failed_count=1,
                    total_count=len(col1_data),
                )

            return self._create_result(
                passed=True,
                message=f"{corr_type.capitalize()} correlation between {column1} and {column2}: {correlation:.3f} (p-value: {p_value:.4f})",
                total_count=len(col1_data),
            )

        except Exception as e:
            logger.error(f"Error in CorrelationCheck: {str(e)}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error checking correlation: {str(e)}",
                failed_count=1,
            )


class AdvancedAnomalyDetectionCheck(DataValidationRule):
    """
    Detects anomalies using multiple statistical methods.

    More sophisticated than basic outlier detection, uses multiple techniques:
    - IQR (Interquartile Range) method
    - Z-score method
    - Modified Z-score (robust to outliers)
    - Isolation Forest (machine learning)

    Configuration:
        params:
            column (str, required): Column to check for anomalies
            method (str, optional): Detection method: iqr, zscore, modified_zscore, isolation_forest. Default: iqr
            threshold (float, optional): Threshold for method. Default varies by method
            max_anomaly_pct (float, optional): Maximum acceptable percentage of anomalies. Default: 5

    Example YAML:
        # Detect price anomalies using IQR method
        - type: "AdvancedAnomalyDetectionCheck"
          severity: "WARNING"
          params:
            column: "price"
            method: "iqr"
            max_anomaly_pct: 2

        # Detect transaction amount anomalies using Isolation Forest
        - type: "AdvancedAnomalyDetectionCheck"
          severity: "WARNING"
          params:
            column: "transaction_amount"
            method: "isolation_forest"
            threshold: 0.1
            max_anomaly_pct: 5
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        column = self.params.get("column", "?")
        method = self.params.get("method", "iqr")

        return f"Detects anomalies in {column} using {method} method"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Detect anomalies in data.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult indicating anomalies found
        """
        try:
            # Get parameters
            column = self.params.get("column")
            method = self.params.get("method", "iqr")
            threshold = self.params.get("threshold")
            max_anomaly_pct = self.params.get("max_anomaly_pct", 5)

            # Validate required parameters
            if not column:
                return self._create_result(
                    passed=False,
                    message="Parameter 'column' is required",
                    failed_count=1,
                )

            # Collect all data
            all_data = []
            row_indices = []

            for chunk_idx, chunk in enumerate(data_iterator):
                if column not in chunk.columns:
                    raise ColumnNotFoundError(
                        validation_name=self.name,
                        column=column,
                        available_columns=list(chunk.columns)
                    )

                # Apply conditional filter if specified
                if self.condition:
                    mask = self._evaluate_condition(chunk)
                    chunk = chunk[mask]

                values = chunk[column].dropna()
                all_data.extend(values.tolist())
                row_indices.extend(values.index.tolist())

            if len(all_data) < 3:
                return self._create_result(
                    passed=False,
                    message=f"Insufficient data for anomaly detection: {len(all_data)} values",
                    failed_count=1,
                )

            # Detect anomalies based on method
            data_array = np.array(all_data)

            if method.lower() == "iqr":
                anomaly_mask = self._detect_iqr_anomalies(data_array, threshold)

            elif method.lower() == "zscore":
                anomaly_mask = self._detect_zscore_anomalies(data_array, threshold or 3.0)

            elif method.lower() == "modified_zscore":
                anomaly_mask = self._detect_modified_zscore_anomalies(data_array, threshold or 3.5)

            elif method.lower() == "isolation_forest":
                anomaly_mask = self._detect_isolation_forest_anomalies(data_array, threshold or 0.1)

            else:
                return self._create_result(
                    passed=False,
                    message=f"Unsupported method: {method}. Supported: iqr, zscore, modified_zscore, isolation_forest",
                    failed_count=1,
                )

            if anomaly_mask is None:
                return self._create_result(
                    passed=False,
                    message=f"Anomaly detection failed with method: {method}",
                    failed_count=1,
                )

            # Count anomalies
            anomaly_count = np.sum(anomaly_mask)
            anomaly_pct = (anomaly_count / len(data_array)) * 100

            # Collect sample anomalies
            sample_anomalies = []
            anomaly_indices = np.where(anomaly_mask)[0]
            for idx in anomaly_indices[:10]:  # Max 10 samples
                sample_anomalies.append({
                    "row_index": int(row_indices[idx]),
                    "value": float(data_array[idx]),
                    "reason": f"Detected as anomaly by {method} method"
                })

            # Check against threshold
            if anomaly_pct > max_anomaly_pct:
                return self._create_result(
                    passed=False,
                    message=f"Found {anomaly_count} anomalies ({anomaly_pct:.2f}%) exceeding {max_anomaly_pct}% threshold",
                    failed_count=anomaly_count,
                    total_count=len(data_array),
                    sample_failures=sample_anomalies,
                )

            return self._create_result(
                passed=True,
                message=f"Anomaly percentage ({anomaly_pct:.2f}%) is within acceptable range ({max_anomaly_pct}%)",
                total_count=len(data_array),
            )

        except Exception as e:
            logger.error(f"Error in AdvancedAnomalyDetectionCheck: {str(e)}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error detecting anomalies: {str(e)}",
                failed_count=1,
            )

    def _detect_iqr_anomalies(self, data: np.ndarray, threshold: float = None) -> np.ndarray:
        """Detect anomalies using IQR method."""
        q1 = np.percentile(data, 25)
        q3 = np.percentile(data, 75)
        iqr = q3 - q1

        multiplier = threshold if threshold else 1.5
        lower_bound = q1 - (multiplier * iqr)
        upper_bound = q3 + (multiplier * iqr)

        return (data < lower_bound) | (data > upper_bound)

    def _detect_zscore_anomalies(self, data: np.ndarray, threshold: float = 3.0) -> np.ndarray:
        """Detect anomalies using Z-score method."""
        mean = np.mean(data)
        std = np.std(data)

        if std == 0:
            return np.zeros(len(data), dtype=bool)

        z_scores = np.abs((data - mean) / std)
        return z_scores > threshold

    def _detect_modified_zscore_anomalies(self, data: np.ndarray, threshold: float = 3.5) -> np.ndarray:
        """Detect anomalies using Modified Z-score (robust to outliers)."""
        median = np.median(data)
        mad = np.median(np.abs(data - median))

        if mad == 0:
            return np.zeros(len(data), dtype=bool)

        modified_z_scores = 0.6745 * (data - median) / mad
        return np.abs(modified_z_scores) > threshold

    def _detect_isolation_forest_anomalies(self, data: np.ndarray, contamination: float = 0.1) -> np.ndarray:
        """Detect anomalies using Isolation Forest."""
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            logger.error("scikit-learn is required for Isolation Forest. Install with: pip install scikit-learn")
            return None

        # Reshape for sklearn
        data_reshaped = data.reshape(-1, 1)

        # Train Isolation Forest
        clf = IsolationForest(contamination=contamination, random_state=42)
        predictions = clf.fit_predict(data_reshaped)

        # -1 indicates anomaly, 1 indicates normal
        return predictions == -1
