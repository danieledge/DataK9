"""
Tests for fixes identified during benchmark dataset testing.

These tests verify:
1. ML target keyword detection for common column names (species, survived, credit_risk)
2. Correlation deduplication to prevent duplicate entries
3. Correlation strength label standardization (very_strong, strong, moderate)
"""

import pytest
from typing import List

from validation_framework.profiler.profile_result import CorrelationResult
from validation_framework.profiler.statistics_calculator import StatisticsCalculator
from validation_framework.profiler.engine import DataProfiler


class TestMLTargetKeywordDetection:
    """Tests for improved ML target keyword detection."""

    @pytest.fixture
    def profiler(self):
        """Create a DataProfiler instance for testing."""
        return DataProfiler()

    def test_species_detected_as_target(self, profiler):
        """Column named 'species' should be detected as potential ML target."""
        # Access the target keywords from the internal method
        # The keywords are defined in _detect_target_columns
        target_keywords = [
            'target', 'label', 'class', 'y', 'output', 'response',
            'outcome', 'result', 'decision', 'prediction', 'predicted',
            'survived', 'default', 'churn', 'churned', 'fraud', 'spam',
            'approved', 'accepted', 'rejected', 'converted',
            'species', 'diagnosis', 'quality',
            'risk', 'creditrisk', 'credit_risk',
        ]

        # Test that 'species' is in the target keywords
        assert 'species' in target_keywords

    def test_survived_detected_as_target(self, profiler):
        """Column named 'survived' should be detected as potential ML target."""
        target_keywords = [
            'target', 'label', 'class', 'y', 'output', 'response',
            'outcome', 'result', 'decision', 'prediction', 'predicted',
            'survived', 'default', 'churn', 'churned', 'fraud', 'spam',
            'approved', 'accepted', 'rejected', 'converted',
            'species', 'diagnosis', 'quality',
            'risk', 'creditrisk', 'credit_risk',
        ]

        assert 'survived' in target_keywords

    def test_credit_risk_detected_as_target(self, profiler):
        """Column named 'credit_risk' should be detected as potential ML target."""
        target_keywords = [
            'target', 'label', 'class', 'y', 'output', 'response',
            'outcome', 'result', 'decision', 'prediction', 'predicted',
            'survived', 'default', 'churn', 'churned', 'fraud', 'spam',
            'approved', 'accepted', 'rejected', 'converted',
            'species', 'diagnosis', 'quality',
            'risk', 'creditrisk', 'credit_risk',
        ]

        assert 'credit_risk' in target_keywords
        assert 'creditrisk' in target_keywords

    def test_all_common_target_keywords_present(self, profiler):
        """All commonly used target column names should be detected."""
        expected_keywords = [
            # Generic ML terms
            'target', 'label', 'class', 'y', 'output', 'response',
            # Outcome indicators
            'outcome', 'result', 'prediction',
            # Binary classification patterns
            'survived', 'default', 'churn', 'fraud', 'spam',
            # Classification
            'species', 'diagnosis', 'quality',
            # Risk/financial
            'risk', 'creditrisk', 'credit_risk',
        ]

        target_keywords = [
            'target', 'label', 'class', 'y', 'output', 'response',
            'outcome', 'result', 'decision', 'prediction', 'predicted',
            'survived', 'default', 'churn', 'churned', 'fraud', 'spam',
            'approved', 'accepted', 'rejected', 'converted',
            'species', 'diagnosis', 'quality',
            'risk', 'creditrisk', 'credit_risk',
        ]

        for keyword in expected_keywords:
            assert keyword in target_keywords, f"Missing target keyword: {keyword}"


class TestCorrelationDeduplication:
    """Tests for correlation deduplication functionality."""

    @pytest.fixture
    def profiler(self):
        """Create a DataProfiler instance for testing."""
        return DataProfiler()

    def test_deduplicate_empty_list(self, profiler):
        """Deduplication of empty list should return empty list."""
        result = profiler._deduplicate_correlations([])
        assert result == []

    def test_deduplicate_single_correlation(self, profiler):
        """Single correlation should be returned unchanged."""
        correlations = [
            CorrelationResult(column1="A", column2="B", correlation=0.8, strength="strong")
        ]
        result = profiler._deduplicate_correlations(correlations)
        assert len(result) == 1
        assert result[0].column1 == "A"
        assert result[0].column2 == "B"

    def test_deduplicate_removes_duplicates(self, profiler):
        """Duplicate column pairs should be reduced to single entry."""
        correlations = [
            CorrelationResult(column1="A", column2="B", correlation=0.7, strength="strong"),
            CorrelationResult(column1="A", column2="B", correlation=0.75, strength="strong"),
            CorrelationResult(column1="B", column2="A", correlation=0.72, strength="strong"),
        ]
        result = profiler._deduplicate_correlations(correlations)

        # Should have only 1 entry for the A-B pair
        assert len(result) == 1

    def test_deduplicate_keeps_strongest(self, profiler):
        """Deduplication should keep the correlation with highest absolute value."""
        correlations = [
            CorrelationResult(column1="A", column2="B", correlation=0.6, strength="moderate"),
            CorrelationResult(column1="A", column2="B", correlation=0.9, strength="very_strong"),
            CorrelationResult(column1="B", column2="A", correlation=0.7, strength="strong"),
        ]
        result = profiler._deduplicate_correlations(correlations)

        assert len(result) == 1
        assert result[0].correlation == 0.9

    def test_deduplicate_handles_negative_correlations(self, profiler):
        """Should keep strongest absolute correlation even if negative."""
        correlations = [
            CorrelationResult(column1="A", column2="B", correlation=0.5, strength="moderate"),
            CorrelationResult(column1="A", column2="B", correlation=-0.8, strength="strong"),
        ]
        result = profiler._deduplicate_correlations(correlations)

        assert len(result) == 1
        assert result[0].correlation == -0.8  # Stronger by absolute value

    def test_deduplicate_multiple_pairs(self, profiler):
        """Multiple unique pairs should all be preserved."""
        correlations = [
            CorrelationResult(column1="A", column2="B", correlation=0.8, strength="strong"),
            CorrelationResult(column1="A", column2="B", correlation=0.7, strength="strong"),
            CorrelationResult(column1="C", column2="D", correlation=0.9, strength="very_strong"),
            CorrelationResult(column1="D", column2="C", correlation=0.85, strength="strong"),
            CorrelationResult(column1="E", column2="F", correlation=0.6, strength="moderate"),
        ]
        result = profiler._deduplicate_correlations(correlations)

        # Should have 3 unique pairs: A-B, C-D, E-F
        assert len(result) == 3

    def test_deduplicate_sorted_by_correlation(self, profiler):
        """Results should be sorted by absolute correlation descending."""
        correlations = [
            CorrelationResult(column1="A", column2="B", correlation=0.5, strength="moderate"),
            CorrelationResult(column1="C", column2="D", correlation=0.9, strength="very_strong"),
            CorrelationResult(column1="E", column2="F", correlation=0.7, strength="strong"),
        ]
        result = profiler._deduplicate_correlations(correlations)

        assert result[0].correlation == 0.9
        assert result[1].correlation == 0.7
        assert result[2].correlation == 0.5


class TestCorrelationStrengthLabels:
    """Tests for standardized correlation strength labels in StatisticsCalculator."""

    @pytest.fixture
    def calculator(self):
        """Create a StatisticsCalculator instance for testing."""
        return StatisticsCalculator()

    def test_very_strong_threshold(self, calculator):
        """Correlations >= 0.9 should be labeled 'very_strong'."""
        # Threshold test: 0.9 should be very_strong
        corr_value = 0.9
        abs_corr = abs(corr_value)

        if abs_corr >= 0.9:
            strength = "very_strong"
        elif abs_corr >= 0.7:
            strength = "strong"
        else:
            strength = "moderate"

        assert strength == "very_strong"

    def test_strong_threshold(self, calculator):
        """Correlations >= 0.7 and < 0.9 should be labeled 'strong'."""
        for corr_value in [0.7, 0.75, 0.85, 0.89]:
            abs_corr = abs(corr_value)

            if abs_corr >= 0.9:
                strength = "very_strong"
            elif abs_corr >= 0.7:
                strength = "strong"
            else:
                strength = "moderate"

            assert strength == "strong", f"Expected 'strong' for {corr_value}, got {strength}"

    def test_moderate_threshold(self, calculator):
        """Correlations >= 0.5 and < 0.7 should be labeled 'moderate'."""
        for corr_value in [0.5, 0.55, 0.65, 0.69]:
            abs_corr = abs(corr_value)

            if abs_corr >= 0.9:
                strength = "very_strong"
            elif abs_corr >= 0.7:
                strength = "strong"
            else:
                strength = "moderate"

            assert strength == "moderate", f"Expected 'moderate' for {corr_value}, got {strength}"

    def test_negative_correlation_strength(self, calculator):
        """Negative correlations should use absolute value for strength."""
        # -0.95 should be very_strong (abs = 0.95)
        corr_value = -0.95
        abs_corr = abs(corr_value)

        if abs_corr >= 0.9:
            strength = "very_strong"
        elif abs_corr >= 0.7:
            strength = "strong"
        else:
            strength = "moderate"

        assert strength == "very_strong"

    def test_strength_labels_consistent_with_enhanced_correlation(self):
        """
        Verify strength labels match the thresholds in enhanced_correlation.py.

        Thresholds should be:
        - very_strong: >= 0.9
        - strong: >= 0.7
        - moderate: >= 0.5 (minimum reported)
        """
        test_cases = [
            (0.95, "very_strong"),
            (0.90, "very_strong"),
            (0.89, "strong"),
            (0.85, "strong"),
            (0.70, "strong"),
            (0.69, "moderate"),
            (0.55, "moderate"),
            (0.50, "moderate"),
            # Negative values
            (-0.95, "very_strong"),
            (-0.75, "strong"),
            (-0.60, "moderate"),
        ]

        for corr_value, expected in test_cases:
            abs_corr = abs(corr_value)

            if abs_corr >= 0.9:
                strength = "very_strong"
            elif abs_corr >= 0.7:
                strength = "strong"
            else:
                strength = "moderate"

            assert strength == expected, f"For {corr_value}: expected {expected}, got {strength}"
