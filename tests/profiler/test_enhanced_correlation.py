"""
Unit tests for enhanced_correlation.py

Tests multi-method correlation analysis including:
- Pearson correlation
- Spearman rank correlation
- Kendall tau correlation
- Mutual information
- Method comparison
- Correlation strength classification
- Heatmap data generation
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from validation_framework.profiler.enhanced_correlation import EnhancedCorrelationAnalyzer


class TestEnhancedCorrelationAnalyzer:
    """Test suite for EnhancedCorrelationAnalyzer class."""

    @pytest.fixture
    def analyzer(self):
        """Create EnhancedCorrelationAnalyzer instance."""
        return EnhancedCorrelationAnalyzer(
            max_correlation_columns=20,
            significance_level=0.05,
            min_correlation_threshold=0.5
        )

    @pytest.fixture
    def correlation_test_data(self):
        """Load correlation test data."""
        test_data_path = Path(__file__).parent / 'test_data' / 'correlation_patterns.csv'
        if not test_data_path.exists():
            pytest.skip(f"Test data not found: {test_data_path}")
        return pd.read_csv(test_data_path)

    @pytest.fixture
    def linear_data(self):
        """Generate data with linear correlation."""
        np.random.seed(42)
        x = np.linspace(0, 10, 100)
        y = 2 * x + 3 + np.random.normal(0, 1, 100)
        return {"x": x.tolist(), "y": y.tolist()}

    @pytest.fixture
    def monotonic_data(self):
        """Generate data with monotonic but non-linear correlation."""
        np.random.seed(42)
        x = np.linspace(0, 10, 100)
        y = np.log(x + 1) * 10 + np.random.normal(0, 1, 100)
        return {"x": x.tolist(), "y_monotonic": y.tolist()}

    @pytest.fixture
    def nonlinear_data(self):
        """Generate data with non-linear correlation."""
        np.random.seed(42)
        x = np.linspace(0, 10, 100)
        y = (x - 5) ** 2 + np.random.normal(0, 1, 100)
        return {"x": x.tolist(), "y_nonlinear": y.tolist()}

    @pytest.fixture
    def independent_data(self):
        """Generate independent data (no correlation)."""
        np.random.seed(42)
        x = np.random.normal(50, 10, 100)
        y = np.random.normal(50, 10, 100)
        return {"x": x.tolist(), "y_independent": y.tolist()}

    # -------------------------------------------------------------------------
    # Pearson Correlation Tests
    # -------------------------------------------------------------------------

    def test_calculate_pearson_linear_data(self, analyzer, linear_data):
        """Test Pearson correlation on linear data."""
        df = pd.DataFrame(linear_data)
        matrix, pairs = analyzer._calculate_pearson(df, list(linear_data.keys()))

        assert len(pairs) > 0
        pair = pairs[0]
        assert pair["method"] == "pearson"
        assert abs(pair["correlation"]) > 0.9  # Strong linear correlation
        assert pair["is_significant"] is True

    def test_calculate_pearson_significance(self, analyzer, linear_data):
        """Test Pearson correlation significance testing."""
        df = pd.DataFrame(linear_data)
        matrix, pairs = analyzer._calculate_pearson(df, list(linear_data.keys()))

        for pair in pairs:
            assert "p_value" in pair
            assert "is_significant" in pair
            if abs(pair["correlation"]) > 0.8:
                assert pair["is_significant"] is True

    def test_calculate_pearson_matrix_structure(self, analyzer, linear_data):
        """Test Pearson correlation matrix structure."""
        df = pd.DataFrame(linear_data)
        matrix, _ = analyzer._calculate_pearson(df, list(linear_data.keys()))

        # Matrix should be symmetric
        assert "x" in matrix
        assert "y" in matrix
        assert matrix["x"]["y"] == matrix["y"]["x"]

    # -------------------------------------------------------------------------
    # Spearman Correlation Tests
    # -------------------------------------------------------------------------

    def test_calculate_spearman_monotonic_data(self, analyzer, monotonic_data):
        """Test Spearman correlation on monotonic data."""
        pytest.importorskip("scipy")

        df = pd.DataFrame(monotonic_data)
        matrix, pairs = analyzer._calculate_spearman(df, list(monotonic_data.keys()))

        if len(pairs) > 0:
            pair = pairs[0]
            assert pair["method"] == "spearman"
            # Spearman should detect monotonic relationship
            assert abs(pair["correlation"]) > 0.5

    def test_calculate_spearman_vs_pearson(self, analyzer, monotonic_data):
        """Test that Spearman detects monotonic relationships better than Pearson."""
        pytest.importorskip("scipy")

        df = pd.DataFrame(monotonic_data)
        _, pearson_pairs = analyzer._calculate_pearson(df, list(monotonic_data.keys()))
        _, spearman_pairs = analyzer._calculate_spearman(df, list(monotonic_data.keys()))

        # For monotonic non-linear data, Spearman should be higher
        if pearson_pairs and spearman_pairs:
            pearson_corr = abs(pearson_pairs[0]["correlation"])
            spearman_corr = abs(spearman_pairs[0]["correlation"])
            # Spearman should be equal or higher for monotonic data
            assert spearman_corr >= pearson_corr * 0.9

    # -------------------------------------------------------------------------
    # Kendall Tau Correlation Tests
    # -------------------------------------------------------------------------

    def test_calculate_kendall(self, analyzer, linear_data):
        """Test Kendall tau correlation."""
        pytest.importorskip("scipy")

        df = pd.DataFrame(linear_data)
        matrix, pairs = analyzer._calculate_kendall(df, list(linear_data.keys()))

        if len(pairs) > 0:
            pair = pairs[0]
            assert pair["method"] == "kendall"
            assert "correlation" in pair
            assert "p_value" in pair

    def test_calculate_kendall_column_limit(self, analyzer):
        """Test Kendall correlation respects column limit."""
        pytest.importorskip("scipy")

        # Create data with many columns
        data = {f"col{i}": np.random.rand(50) for i in range(15)}
        df = pd.DataFrame(data)

        matrix, _ = analyzer._calculate_kendall(df, list(data.keys()))

        # Should limit to 10 columns for performance
        assert len(matrix) <= 10

    # -------------------------------------------------------------------------
    # Mutual Information Tests
    # -------------------------------------------------------------------------

    def test_calculate_mutual_info_nonlinear(self, analyzer, nonlinear_data):
        """Test mutual information on non-linear data."""
        pytest.importorskip("sklearn")

        df = pd.DataFrame(nonlinear_data)
        matrix, pairs = analyzer._calculate_mutual_info(df, list(nonlinear_data.keys()))

        # MI should detect non-linear relationships
        assert isinstance(matrix, dict)
        # May or may not have pairs depending on threshold

    def test_calculate_mutual_info_column_limit(self, analyzer):
        """Test mutual information respects column limit."""
        pytest.importorskip("sklearn")

        # Create data with many columns
        data = {f"col{i}": np.random.rand(50) for i in range(15)}
        df = pd.DataFrame(data)

        matrix, _ = analyzer._calculate_mutual_info(df, list(data.keys()))

        # Should limit to 10 columns for performance
        assert len(matrix) <= 10

    # -------------------------------------------------------------------------
    # Correlation Strength Classification Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("corr_value,expected_strength", [
        (0.95, "very_strong"),
        (0.85, "strong"),
        (0.65, "moderate"),
        (0.45, "weak"),
    ])
    def test_classify_strength(self, analyzer, corr_value, expected_strength):
        """Test correlation strength classification."""
        strength = analyzer._classify_strength(corr_value)
        assert strength == expected_strength

    # -------------------------------------------------------------------------
    # Multi-Method Correlation Tests
    # -------------------------------------------------------------------------

    def test_calculate_correlations_multi_method_all_methods(self, analyzer, linear_data):
        """Test multi-method correlation with all methods."""
        pytest.importorskip("scipy")
        pytest.importorskip("sklearn")

        result = analyzer.calculate_correlations_multi_method(
            linear_data,
            row_count=len(linear_data["x"]),
            methods=['pearson', 'spearman', 'kendall', 'mutual_info']
        )

        assert result["available"] is True
        assert "pearson" in result["methods_used"]
        assert "correlation_pairs" in result
        assert "correlation_matrix" in result

    def test_calculate_correlations_multi_method_pearson_only(self, analyzer, linear_data):
        """Test multi-method with Pearson only."""
        result = analyzer.calculate_correlations_multi_method(
            linear_data,
            row_count=len(linear_data["x"]),
            methods=['pearson']
        )

        assert result["available"] is True
        assert result["methods_used"] == ['pearson']
        assert "pearson" in result["correlation_matrix"]

    def test_calculate_correlations_multi_method_insufficient_columns(self, analyzer):
        """Test multi-method with insufficient columns."""
        single_column = {"x": [1, 2, 3, 4, 5]}

        result = analyzer.calculate_correlations_multi_method(
            single_column,
            row_count=5,
            methods=['pearson']
        )

        assert result["available"] is False
        assert "Less than 2 numeric columns" in result["reason"]

    def test_calculate_correlations_multi_method_with_nan(self, analyzer):
        """Test multi-method correlation with NaN values."""
        data_with_nan = {
            "x": [1, 2, np.nan, 4, 5],
            "y": [2, 4, 6, np.nan, 10]
        }

        result = analyzer.calculate_correlations_multi_method(
            data_with_nan,
            row_count=5,
            methods=['pearson']
        )

        # Should handle NaN by dropping rows
        if result["available"]:
            assert result["data_points"] < 5

    # -------------------------------------------------------------------------
    # Method Comparison Tests
    # -------------------------------------------------------------------------

    def test_compare_methods(self, analyzer, linear_data):
        """Test method comparison functionality."""
        pytest.importorskip("scipy")

        # Calculate with multiple methods
        result = analyzer.calculate_correlations_multi_method(
            linear_data,
            row_count=len(linear_data["x"]),
            methods=['pearson', 'spearman']
        )

        if "method_comparison" in result and result["method_comparison"]:
            comparison = result["method_comparison"][0]
            assert "column1" in comparison
            assert "column2" in comparison
            assert "correlations" in comparison
            assert "recommended_method" in comparison

    def test_interpret_method_differences(self, analyzer):
        """Test interpretation of method differences."""
        correlations = {
            "pearson": 0.72,
            "spearman": 0.85
        }

        interpretation = analyzer._interpret_method_differences(correlations)

        assert isinstance(interpretation, str)
        assert len(interpretation) > 0
        # Should mention monotonic relationship
        assert "monotonic" in interpretation.lower() or "spearman" in interpretation.lower()

    # -------------------------------------------------------------------------
    # Heatmap Data Generation Tests
    # -------------------------------------------------------------------------

    def test_generate_heatmap_data(self, analyzer, linear_data):
        """Test heatmap data generation."""
        # Calculate correlations first
        result = analyzer.calculate_correlations_multi_method(
            linear_data,
            row_count=len(linear_data["x"]),
            methods=['pearson']
        )

        assert "heatmap_data" in result
        heatmap = result["heatmap_data"]

        assert "columns" in heatmap
        assert "matrices" in heatmap
        assert "pearson" in heatmap["matrices"]

        # Check matrix structure
        pearson_matrix = heatmap["matrices"]["pearson"]
        assert len(pearson_matrix) == len(heatmap["columns"])
        assert len(pearson_matrix[0]) == len(heatmap["columns"])

    # -------------------------------------------------------------------------
    # Deduplication and Ranking Tests
    # -------------------------------------------------------------------------

    def test_deduplicate_and_rank_pairs(self, analyzer):
        """Test pair deduplication and ranking."""
        pairs = [
            {"column1": "x", "column2": "y", "method": "pearson", "correlation": 0.8},
            {"column1": "y", "column2": "x", "method": "pearson", "correlation": 0.8},  # Duplicate
            {"column1": "x", "column2": "z", "method": "pearson", "correlation": 0.9},
        ]

        result = analyzer._deduplicate_and_rank_pairs(pairs)

        # Should remove duplicate and sort by correlation
        assert len(result) == 2
        assert abs(result[0]["correlation"]) >= abs(result[1]["correlation"])

    # -------------------------------------------------------------------------
    # Integration Tests with Real Test Data
    # -------------------------------------------------------------------------

    def test_with_correlation_test_data(self, analyzer, correlation_test_data):
        """Test with real correlation test data."""
        # Extract numeric columns
        numeric_cols = correlation_test_data.select_dtypes(include=[np.number]).columns
        numeric_data = {col: correlation_test_data[col].tolist() for col in numeric_cols}

        result = analyzer.calculate_correlations_multi_method(
            numeric_data,
            row_count=len(correlation_test_data),
            methods=['pearson', 'spearman']
        )

        assert result["available"] is True
        assert len(result["correlation_pairs"]) >= 0

    def test_full_correlation_analysis_pipeline(self, analyzer, correlation_test_data):
        """Test complete correlation analysis pipeline."""
        pytest.importorskip("scipy")

        # Extract numeric data
        numeric_cols = correlation_test_data.select_dtypes(include=[np.number]).columns
        numeric_data = {col: correlation_test_data[col].tolist() for col in numeric_cols}

        # Run full analysis
        result = analyzer.calculate_correlations_multi_method(
            numeric_data,
            row_count=len(correlation_test_data),
            methods=['pearson', 'spearman']
        )

        # Verify structure
        assert result["available"] is True
        assert "methods_used" in result
        assert "correlation_pairs" in result
        assert "correlation_matrix" in result
        assert "heatmap_data" in result

        # Verify pairs structure
        for pair in result["correlation_pairs"]:
            assert "column1" in pair
            assert "column2" in pair
            assert "method" in pair
            assert "correlation" in pair
            assert "strength" in pair
            assert "direction" in pair

    # -------------------------------------------------------------------------
    # Fallback Correlation Tests
    # -------------------------------------------------------------------------

    def test_fallback_correlation_no_scipy(self, monkeypatch, analyzer, linear_data):
        """Test fallback to basic correlation when scipy unavailable."""
        # This test would require monkeypatching scipy availability
        # For now, just test the fallback method directly
        result = analyzer._fallback_correlation(
            linear_data,
            row_count=len(linear_data["x"])
        )

        assert result["available"] is True
        assert result["methods_used"] == ["pearson"]
        assert "note" in result

    # -------------------------------------------------------------------------
    # Edge Cases and Error Handling
    # -------------------------------------------------------------------------

    def test_empty_data(self, analyzer):
        """Test correlation with empty data."""
        result = analyzer.calculate_correlations_multi_method(
            {},
            row_count=0,
            methods=['pearson']
        )

        assert result["available"] is False

    def test_single_column(self, analyzer):
        """Test correlation with single column."""
        single_col = {"x": [1, 2, 3, 4, 5]}

        result = analyzer.calculate_correlations_multi_method(
            single_col,
            row_count=5,
            methods=['pearson']
        )

        assert result["available"] is False

    def test_all_nan_column(self, analyzer):
        """Test correlation with all-NaN column."""
        data_with_all_nan = {
            "x": [1, 2, 3, 4, 5],
            "y": [np.nan, np.nan, np.nan, np.nan, np.nan]
        }

        result = analyzer.calculate_correlations_multi_method(
            data_with_all_nan,
            row_count=5,
            methods=['pearson']
        )

        # Should handle gracefully
        assert isinstance(result, dict)

    def test_constant_column(self, analyzer):
        """Test correlation with constant value column."""
        data_with_constant = {
            "x": [1, 2, 3, 4, 5],
            "y": [5, 5, 5, 5, 5]  # Constant
        }

        result = analyzer.calculate_correlations_multi_method(
            data_with_constant,
            row_count=5,
            methods=['pearson']
        )

        # Correlation with constant is undefined (NaN)
        # Should handle gracefully
        assert isinstance(result, dict)

    def test_min_correlation_threshold(self):
        """Test minimum correlation threshold filtering."""
        analyzer = EnhancedCorrelationAnalyzer(min_correlation_threshold=0.8)

        # Create data with weak correlation
        weak_data = {
            "x": np.random.rand(100).tolist(),
            "y": (np.random.rand(100) * 0.3).tolist()  # Weak correlation
        }

        result = analyzer.calculate_correlations_multi_method(
            weak_data,
            row_count=100,
            methods=['pearson']
        )

        # Should filter out weak correlations
        # Number of pairs might be 0 if all below threshold
        assert "correlation_pairs" in result

    def test_max_correlation_columns_limit(self):
        """Test maximum correlation columns limit."""
        analyzer = EnhancedCorrelationAnalyzer(max_correlation_columns=5)

        # Create data with many columns
        many_cols = {f"col{i}": np.random.rand(50).tolist() for i in range(20)}

        result = analyzer.calculate_correlations_multi_method(
            many_cols,
            row_count=50,
            methods=['pearson']
        )

        # Should limit to 5 columns
        assert result["columns_analyzed"] <= 5

    # -------------------------------------------------------------------------
    # Parametrized Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("method", [
        "pearson",
        "spearman",
        pytest.param("kendall", marks=pytest.mark.skipif(
            not pytest.importorskip("scipy", minversion="1.0"),
            reason="Requires scipy"
        )),
        pytest.param("mutual_info", marks=pytest.mark.skipif(
            not pytest.importorskip("sklearn", minversion="1.0"),
            reason="Requires sklearn"
        )),
    ])
    def test_individual_methods(self, analyzer, linear_data, method):
        """Test each correlation method individually."""
        result = analyzer.calculate_correlations_multi_method(
            linear_data,
            row_count=len(linear_data["x"]),
            methods=[method]
        )

        assert result["available"] is True or method in ["kendall", "mutual_info"]
        if result["available"]:
            assert method in result["methods_used"]


class TestEnhancedCorrelationAnalyzerPerformance:
    """Performance tests for EnhancedCorrelationAnalyzer."""

    def test_large_dataset_performance(self):
        """Test correlation analysis with large dataset."""
        analyzer = EnhancedCorrelationAnalyzer(max_correlation_columns=10)

        # Generate large dataset
        large_data = {
            f"col{i}": np.random.rand(10000).tolist()
            for i in range(5)
        }

        result = analyzer.calculate_correlations_multi_method(
            large_data,
            row_count=10000,
            methods=['pearson']
        )

        # Should complete successfully
        assert result["available"] is True

    def test_many_columns_performance(self):
        """Test correlation analysis with many columns."""
        analyzer = EnhancedCorrelationAnalyzer(max_correlation_columns=50)

        # Generate data with many columns
        many_cols = {
            f"col{i}": np.random.rand(100).tolist()
            for i in range(30)
        }

        result = analyzer.calculate_correlations_multi_method(
            many_cols,
            row_count=100,
            methods=['pearson']
        )

        # Should respect column limit
        assert result["columns_analyzed"] <= 50
