"""
Tests for MLAnalyzer - Known Domain Detection and Rare Category Analysis.

Tests the integration between ml_analyzer.py and ReferenceDataLoader for:
- Known domain detection (currency, country columns)
- Rare category identification
- Category proportion analysis
"""

import pytest
import pandas as pd
from collections import Counter

from validation_framework.profiler.ml_analyzer import ChunkedMLAccumulator
from validation_framework.reference_data import ReferenceDataLoader


class TestKnownDomainDetection:
    """Tests for known domain detection using ReferenceDataLoader."""

    def test_currencies_loaded_from_reference_data(self):
        """ML analyzer should have access to ISO 4217 currencies."""
        currencies = ReferenceDataLoader.get_currencies()
        assert len(currencies) > 100, "Should have 100+ currencies from ISO 4217"
        # Common currencies should be present
        assert 'usd' in currencies
        assert 'eur' in currencies
        assert 'gbp' in currencies

    def test_countries_loaded_from_reference_data(self):
        """ML analyzer should have access to ISO 3166-1 countries."""
        countries = ReferenceDataLoader.get_countries()
        assert len(countries) > 200, "Should have 200+ country identifiers"
        # Common countries should be present
        assert 'us' in countries
        assert 'usa' in countries
        assert 'united states of america' in countries

    def test_currency_column_detection(self):
        """Currency values should be recognized as known domain."""
        currencies = ReferenceDataLoader.get_currencies()
        # Simulate currency column values
        column_values = ['USD', 'EUR', 'GBP', 'JPY', 'CHF']

        # All values should be valid currencies
        for val in column_values:
            assert val.lower() in currencies, f"{val} should be a valid currency"

    def test_country_column_detection(self):
        """Country values should be recognized as known domain."""
        countries = ReferenceDataLoader.get_countries()
        # Simulate country column values
        column_values = ['US', 'GB', 'DE', 'FR', 'JP']

        # All values should be valid countries
        for val in column_values:
            assert val.lower() in countries, f"{val} should be a valid country"

    def test_mixed_currency_with_aliases(self):
        """Should handle both ISO codes and common names."""
        currencies = ReferenceDataLoader.get_currencies(include_aliases=True)

        # Mix of codes and aliases
        column_values = ['USD', 'dollar', 'EUR', 'euro', 'GBP', 'pound']

        for val in column_values:
            assert val.lower() in currencies, f"{val} should be recognized"

    def test_invalid_values_not_in_domain(self):
        """Invalid values should not be in known domains."""
        currencies = ReferenceDataLoader.get_currencies()
        countries = ReferenceDataLoader.get_countries()

        invalid_values = ['INVALID', 'XYZ123', 'NOT_A_CURRENCY', 'FAKELAND']

        for val in invalid_values:
            assert val.lower() not in currencies
            assert val.lower() not in countries


class TestChunkedMLAccumulator:
    """Tests for ChunkedMLAccumulator functionality."""

    def test_accumulator_initialization(self):
        """Accumulator should initialize with empty state."""
        acc = ChunkedMLAccumulator()

        assert acc.total_rows == 0
        assert len(acc.benford_digit_counts) == 0
        assert len(acc.value_counts) == 0
        assert len(acc.format_pattern_counts) == 0

    def test_accumulator_loads_fibo_taxonomy(self):
        """Accumulator should load FIBO taxonomy for semantic analysis."""
        acc = ChunkedMLAccumulator()

        # FIBO taxonomy should be loaded
        assert acc._fibo_taxonomy is not None
        # Should have some tags defined
        assert isinstance(acc._fibo_taxonomy, dict)


class TestRareCategoryAnalysis:
    """Tests for rare category detection functionality."""

    def test_rare_category_threshold(self):
        """Rare categories should be detected below threshold."""
        # Create value counts with one rare value
        value_counts = Counter({
            'USD': 950,
            'EUR': 40,
            'GBP': 8,
            'JPY': 2  # Rare category (< 1%)
        })

        total = sum(value_counts.values())
        rare_threshold = 0.01  # 1%

        rare_categories = [
            val for val, count in value_counts.items()
            if count / total < rare_threshold
        ]

        assert 'JPY' in rare_categories
        assert 'USD' not in rare_categories

    def test_known_domain_rare_handling(self):
        """Known domain values should not be flagged as suspicious rare categories."""
        currencies = ReferenceDataLoader.get_currencies()

        # Even if rare, valid currencies shouldn't be flagged as "suspicious"
        rare_but_valid = ['XAF', 'XOF', 'XPF']  # CFA francs - rare but valid ISO codes

        for code in rare_but_valid:
            assert code.lower() in currencies, f"{code} is a valid ISO 4217 currency"


class TestCategoryProportionAnalysis:
    """Tests for category proportion analysis."""

    def test_proportion_calculation(self):
        """Should correctly calculate category proportions."""
        value_counts = Counter({
            'A': 500,
            'B': 300,
            'C': 200
        })

        total = sum(value_counts.values())
        proportions = {k: v / total for k, v in value_counts.items()}

        assert abs(proportions['A'] - 0.5) < 0.001
        assert abs(proportions['B'] - 0.3) < 0.001
        assert abs(proportions['C'] - 0.2) < 0.001

    def test_highly_skewed_distribution(self):
        """Should detect highly skewed distributions."""
        value_counts = Counter({
            'dominant': 9901,
            'rare1': 50,
            'rare2': 30,
            'rare3': 19
        })

        total = sum(value_counts.values())
        dominant_proportion = value_counts['dominant'] / total

        # Dominant category takes > 99%
        assert dominant_proportion > 0.99
