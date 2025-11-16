"""
Advanced statistical analysis for data profiling using scipy and statsmodels.

Provides:
- Goodness-of-fit tests (Shapiro-Wilk, Anderson-Darling, Kolmogorov-Smirnov)
- Distribution fitting with AIC/BIC selection
- Enhanced correlation analysis (Spearman, Kendall)
- Statistical significance testing
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import logging

try:
    from scipy import stats
    from scipy.stats import (
        norm, expon, uniform, gamma, lognorm, beta,
        chi2, t as tdist, f as fdist
    )
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logging.warning("scipy not available - advanced statistical tests disabled")

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.neighbors import LocalOutlierFactor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logging.warning("scikit-learn not available - ML anomaly detection disabled")

logger = logging.getLogger(__name__)


class StatisticalTests:
    """Perform statistical hypothesis tests on data."""

    @staticmethod
    def test_normality(data: List[float], alpha: float = 0.05) -> Dict[str, Any]:
        """
        Test if data follows a normal distribution using multiple methods.

        Args:
            data: Numeric data to test
            alpha: Significance level (default 0.05)

        Returns:
            Dict with test results
        """
        if not SCIPY_AVAILABLE or len(data) < 3:
            return {"available": False}

        results = {}

        # Sample if too large (Shapiro-Wilk limited to 5000 samples)
        sample_data = data if len(data) <= 5000 else np.random.choice(data, 5000, replace=False)

        try:
            # Shapiro-Wilk test
            stat, p_value = stats.shapiro(sample_data)
            results['shapiro_wilk'] = {
                'statistic': float(stat),
                'p_value': float(p_value),
                'is_normal': p_value > alpha,
                'method': 'Shapiro-Wilk'
            }
        except Exception as e:
            logger.warning(f"Shapiro-Wilk test failed: {e}")

        try:
            # Anderson-Darling test
            result = stats.anderson(data, dist='norm')
            # Find critical value for alpha
            critical_idx = {0.15: 0, 0.10: 1, 0.05: 2, 0.025: 3, 0.01: 4}.get(alpha, 2)
            critical_value = result.critical_values[critical_idx]
            results['anderson_darling'] = {
                'statistic': float(result.statistic),
                'critical_value': float(critical_value),
                'is_normal': result.statistic < critical_value,
                'method': 'Anderson-Darling'
            }
        except Exception as e:
            logger.warning(f"Anderson-Darling test failed: {e}")

        try:
            # Kolmogorov-Smirnov test
            # Standardize data
            standardized = (np.array(data) - np.mean(data)) / np.std(data)
            stat, p_value = stats.kstest(standardized, 'norm')
            results['kolmogorov_smirnov'] = {
                'statistic': float(stat),
                'p_value': float(p_value),
                'is_normal': p_value > alpha,
                'method': 'Kolmogorov-Smirnov'
            }
        except Exception as e:
            logger.warning(f"Kolmogorov-Smirnov test failed: {e}")

        # Consensus: is normal if majority of tests agree
        if results:
            normal_votes = sum(1 for test in results.values() if test.get('is_normal', False))
            results['consensus'] = {
                'is_normal': normal_votes >= len(results) / 2,
                'confidence': normal_votes / len(results) if len(results) > 0 else 0
            }

        results['available'] = True
        return results


class DistributionFitter:
    """Fit distributions to data and select best fit using AIC/BIC."""

    # Candidate distributions to try
    DISTRIBUTIONS = {
        'normal': norm,
        'lognormal': lognorm,
        'exponential': expon,
        'gamma': gamma,
        'uniform': uniform,
        'beta': beta
    }

    @staticmethod
    def fit_distributions(data: List[float], top_n: int = 3) -> List[Dict[str, Any]]:
        """
        Fit multiple distributions and rank by AIC.

        Args:
            data: Numeric data to fit
            top_n: Number of top distributions to return

        Returns:
            List of fitted distributions with parameters and goodness-of-fit
        """
        if not SCIPY_AVAILABLE or len(data) < 10:
            return []

        data_array = np.array(data)
        results = []

        for dist_name, dist_class in DistributionFitter.DISTRIBUTIONS.items():
            try:
                # Fit distribution
                params = dist_class.fit(data_array)

                # Calculate log-likelihood
                log_likelihood = np.sum(dist_class.logpdf(data_array, *params))

                # Calculate AIC and BIC
                k = len(params)  # Number of parameters
                n = len(data_array)
                aic = 2 * k - 2 * log_likelihood
                bic = k * np.log(n) - 2 * log_likelihood

                # Kolmogorov-Smirnov test
                ks_stat, ks_p_value = stats.kstest(data_array, dist_name, args=params)

                results.append({
                    'distribution': dist_name,
                    'parameters': {
                        'params': [float(p) for p in params],
                        'num_params': k
                    },
                    'goodness_of_fit': {
                        'aic': float(aic),
                        'bic': float(bic),
                        'ks_statistic': float(ks_stat),
                        'ks_p_value': float(ks_p_value),
                        'log_likelihood': float(log_likelihood)
                    }
                })

            except Exception as e:
                logger.debug(f"Failed to fit {dist_name}: {e}")
                continue

        # Sort by AIC (lower is better)
        results.sort(key=lambda x: x['goodness_of_fit']['aic'])

        return results[:top_n]

    @staticmethod
    def get_theoretical_percentiles(
        best_fit: Dict[str, Any],
        percentiles: List[float] = [1, 5, 25, 50, 75, 95, 99]
    ) -> Dict[str, float]:
        """
        Get theoretical percentiles from fitted distribution.

        Args:
            best_fit: Best fit distribution info from fit_distributions
            percentiles: List of percentiles to calculate

        Returns:
            Dict mapping percentile to value
        """
        if not best_fit or not SCIPY_AVAILABLE:
            return {}

        dist_name = best_fit['distribution']
        params = best_fit['parameters']['params']
        dist_class = DistributionFitter.DISTRIBUTIONS.get(dist_name)

        if not dist_class:
            return {}

        try:
            theoretical = {}
            for p in percentiles:
                value = dist_class.ppf(p / 100.0, *params)
                theoretical[f'p{p}'] = float(value)
            return theoretical
        except Exception as e:
            logger.warning(f"Failed to calculate theoretical percentiles: {e}")
            return {}


class EnhancedCorrelation:
    """Calculate multiple types of correlations with statistical significance."""

    @staticmethod
    def calculate_correlations(
        df: pd.DataFrame,
        methods: List[str] = ['pearson', 'spearman', 'kendall'],
        min_correlation: float = 0.5,
        alpha: float = 0.05
    ) -> List[Dict[str, Any]]:
        """
        Calculate correlations using multiple methods with p-values.

        Args:
            df: DataFrame with numeric columns
            methods: List of correlation methods to use
            min_correlation: Minimum correlation to include
            alpha: Significance level for p-values

        Returns:
            List of significant correlations
        """
        if not SCIPY_AVAILABLE or df.shape[1] < 2:
            return []

        results = []

        # Get numeric columns only
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        if len(numeric_cols) < 2:
            return []

        # Calculate for each pair of columns
        for i, col1 in enumerate(numeric_cols):
            for j, col2 in enumerate(numeric_cols):
                if i >= j:  # Skip self-correlation and duplicates
                    continue

                # Get clean data (remove NaNs)
                data1 = df[col1].dropna()
                data2 = df[col2].dropna()

                # Align the two series
                common_idx = data1.index.intersection(data2.index)
                if len(common_idx) < 3:
                    continue

                x = data1.loc[common_idx].values
                y = data2.loc[common_idx].values

                correlation_results = {}

                # Pearson correlation
                if 'pearson' in methods:
                    try:
                        corr, p_value = stats.pearsonr(x, y)
                        if abs(corr) >= min_correlation and p_value < alpha:
                            correlation_results['pearson'] = {
                                'correlation': float(corr),
                                'p_value': float(p_value),
                                'significant': p_value < alpha
                            }
                    except Exception as e:
                        logger.debug(f"Pearson correlation failed for {col1}-{col2}: {e}")

                # Spearman correlation (rank-based, handles non-linear monotonic)
                if 'spearman' in methods:
                    try:
                        corr, p_value = stats.spearmanr(x, y)
                        if abs(corr) >= min_correlation and p_value < alpha:
                            correlation_results['spearman'] = {
                                'correlation': float(corr),
                                'p_value': float(p_value),
                                'significant': p_value < alpha
                            }
                    except Exception as e:
                        logger.debug(f"Spearman correlation failed for {col1}-{col2}: {e}")

                # Kendall's tau (rank-based, robust to outliers)
                if 'kendall' in methods:
                    try:
                        corr, p_value = stats.kendalltau(x, y)
                        if abs(corr) >= min_correlation and p_value < alpha:
                            correlation_results['kendall'] = {
                                'correlation': float(corr),
                                'p_value': float(p_value),
                                'significant': p_value < alpha
                            }
                    except Exception as e:
                        logger.debug(f"Kendall correlation failed for {col1}-{col2}: {e}")

                # Add to results if any method found significant correlation
                if correlation_results:
                    results.append({
                        'column1': col1,
                        'column2': col2,
                        'methods': correlation_results,
                        'sample_size': len(common_idx)
                    })

        return results


class MLAnomalyDetection:
    """Machine learning-based anomaly detection."""

    @staticmethod
    def detect_multivariate_outliers(
        df: pd.DataFrame,
        contamination: float = 0.05,
        methods: List[str] = ['isolation_forest', 'lof']
    ) -> Dict[str, Any]:
        """
        Detect outliers using ML methods.

        Args:
            df: DataFrame with numeric columns
            contamination: Expected proportion of outliers
            methods: List of methods to use

        Returns:
            Dict with outlier detection results
        """
        if not SKLEARN_AVAILABLE or df.shape[0] < 10:
            return {'available': False}

        # Get numeric columns only
        numeric_df = df.select_dtypes(include=[np.number]).dropna()

        if numeric_df.shape[1] < 2 or numeric_df.shape[0] < 10:
            return {'available': False}

        results = {'available': True, 'methods': {}}

        # Isolation Forest
        if 'isolation_forest' in methods:
            try:
                clf = IsolationForest(contamination=contamination, random_state=42)
                predictions = clf.fit_predict(numeric_df)
                outlier_mask = predictions == -1
                outlier_indices = np.where(outlier_mask)[0]

                results['methods']['isolation_forest'] = {
                    'outlier_count': int(np.sum(outlier_mask)),
                    'outlier_percentage': float(100 * np.sum(outlier_mask) / len(predictions)),
                    'outlier_indices': outlier_indices.tolist()[:100],  # Limit to 100
                    'method': 'Isolation Forest'
                }
            except Exception as e:
                logger.warning(f"Isolation Forest failed: {e}")

        # Local Outlier Factor
        if 'lof' in methods:
            try:
                clf = LocalOutlierFactor(contamination=contamination)
                predictions = clf.fit_predict(numeric_df)
                outlier_mask = predictions == -1
                outlier_indices = np.where(outlier_mask)[0]

                results['methods']['lof'] = {
                    'outlier_count': int(np.sum(outlier_mask)),
                    'outlier_percentage': float(100 * np.sum(outlier_mask) / len(predictions)),
                    'outlier_indices': outlier_indices.tolist()[:100],
                    'method': 'Local Outlier Factor'
                }
            except Exception as e:
                logger.warning(f"LOF failed: {e}")

        # Consensus outliers (detected by multiple methods)
        if len(results['methods']) > 1:
            all_outliers = set()
            for method_result in results['methods'].values():
                all_outliers.update(method_result['outlier_indices'])

            # Outliers detected by majority of methods
            outlier_counts = {}
            for method_result in results['methods'].values():
                for idx in method_result['outlier_indices']:
                    outlier_counts[idx] = outlier_counts.get(idx, 0) + 1

            consensus_outliers = [
                idx for idx, count in outlier_counts.items()
                if count >= len(results['methods']) / 2
            ]

            results['consensus'] = {
                'outlier_count': len(consensus_outliers),
                'outlier_indices': consensus_outliers[:100],
                'method': 'Consensus (majority vote)'
            }

        return results
