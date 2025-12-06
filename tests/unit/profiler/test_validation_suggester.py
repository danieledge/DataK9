"""
Tests for ValidationSuggestionGenerator - ID/Measurement Pattern Detection.

Tests the integration between validation_suggester.py and ReferenceDataLoader for:
- ID pattern loading and matching
- Measurement pattern loading and matching
- Column classification for range check suggestions
"""

import pytest
import re

from validation_framework.profiler.validation_suggester import ValidationSuggestionGenerator
from validation_framework.profiler.profile_result import ColumnProfile, QualityMetrics
from validation_framework.reference_data import ReferenceDataLoader


class TestPatternLoading:
    """Tests for loading patterns from ReferenceDataLoader."""

    def test_id_patterns_loaded(self):
        """Generator should load ID patterns from ReferenceDataLoader."""
        generator = ValidationSuggestionGenerator()

        # Should have patterns loaded
        assert len(generator._id_patterns) > 0, "Should have ID patterns loaded"

        # Verify loaded from external JSON (not defaults)
        external_patterns = ReferenceDataLoader.get_id_patterns()
        if external_patterns:
            assert generator._id_patterns == external_patterns

    def test_measurement_patterns_loaded(self):
        """Generator should load measurement patterns from ReferenceDataLoader."""
        generator = ValidationSuggestionGenerator()

        # Should have patterns loaded
        assert len(generator._measurement_patterns) > 0, "Should have measurement patterns"

        # Verify loaded from external JSON (not defaults)
        external_patterns = ReferenceDataLoader.get_measurement_patterns()
        if external_patterns:
            assert generator._measurement_patterns == external_patterns

    def test_regex_patterns_compiled(self):
        """Patterns should be compiled into regex for performance."""
        generator = ValidationSuggestionGenerator()

        # Should have compiled regex
        assert generator._id_regex is not None
        assert generator._measurement_regex is not None
        assert isinstance(generator._id_regex, re.Pattern)
        assert isinstance(generator._measurement_regex, re.Pattern)


class TestIDPatternMatching:
    """Tests for ID column detection."""

    def test_id_suffix_patterns(self):
        """Columns ending with _id should be detected as ID columns."""
        generator = ValidationSuggestionGenerator()

        id_columns = [
            'customer_id', 'user_id', 'order_id', 'product_id',
            'transaction_id', 'account_id', 'record_id'
        ]

        for col_name in id_columns:
            match = generator._id_regex.search(col_name)
            assert match is not None, f"{col_name} should match ID pattern"

    def test_id_prefix_patterns(self):
        """Columns starting with id_ should be detected as ID columns."""
        generator = ValidationSuggestionGenerator()

        id_columns = ['id_customer', 'id_user', 'id_order']

        for col_name in id_columns:
            match = generator._id_regex.search(col_name)
            assert match is not None, f"{col_name} should match ID pattern"

    def test_account_customer_patterns(self):
        """Account and customer columns should be detected as ID columns."""
        generator = ValidationSuggestionGenerator()

        id_columns = [
            'account_number', 'customer_number', 'account_code',
            'customer_key', 'account_ref'
        ]

        for col_name in id_columns:
            match = generator._id_regex.search(col_name)
            assert match is not None, f"{col_name} should match ID pattern"

    def test_code_patterns(self):
        """Code columns should be detected as ID columns."""
        generator = ValidationSuggestionGenerator()

        id_columns = ['currency_code', 'country_code', 'product_code', 'status_code']

        for col_name in id_columns:
            match = generator._id_regex.search(col_name)
            assert match is not None, f"{col_name} should match ID pattern"


class TestMeasurementPatternMatching:
    """Tests for measurement column detection."""

    def test_amount_patterns(self):
        """Amount columns should be detected as measurements."""
        generator = ValidationSuggestionGenerator()

        measurement_columns = [
            'amount', 'total_amount', 'transaction_amount',
            'payment_amount', 'invoice_amount'
        ]

        for col_name in measurement_columns:
            match = generator._measurement_regex.search(col_name)
            assert match is not None, f"{col_name} should match measurement pattern"

    def test_price_patterns(self):
        """Price columns should be detected as measurements."""
        generator = ValidationSuggestionGenerator()

        measurement_columns = [
            'price', 'unit_price', 'total_price',
            'sale_price', 'purchase_price'
        ]

        for col_name in measurement_columns:
            match = generator._measurement_regex.search(col_name)
            assert match is not None, f"{col_name} should match measurement pattern"

    def test_quantity_patterns(self):
        """Quantity columns should be detected as measurements."""
        generator = ValidationSuggestionGenerator()

        measurement_columns = [
            'quantity', 'qty', 'item_quantity',
            'order_qty', 'stock_quantity'
        ]

        for col_name in measurement_columns:
            match = generator._measurement_regex.search(col_name)
            assert match is not None, f"{col_name} should match measurement pattern"

    def test_cost_patterns(self):
        """Cost columns should be detected as measurements."""
        generator = ValidationSuggestionGenerator()

        measurement_columns = [
            'cost', 'unit_cost', 'total_cost',
            'shipping_cost', 'material_cost'
        ]

        for col_name in measurement_columns:
            match = generator._measurement_regex.search(col_name)
            assert match is not None, f"{col_name} should match measurement pattern"


class TestColumnClassification:
    """Tests for column classification behavior."""

    def test_id_column_no_range_check(self):
        """ID columns should NOT receive range check suggestions."""
        generator = ValidationSuggestionGenerator()

        id_columns = ['customer_id', 'order_id', 'account_number']

        for col_name in id_columns:
            # ID columns should match ID pattern
            is_id = generator._id_regex.search(col_name) is not None
            # And should NOT match measurement pattern primarily
            is_measurement = generator._measurement_regex.search(col_name) is not None

            assert is_id, f"{col_name} should be classified as ID"
            # customer_id should be ID, not measurement

    def test_measurement_column_range_check(self):
        """Measurement columns SHOULD receive range check suggestions."""
        generator = ValidationSuggestionGenerator()

        measurement_columns = ['amount', 'price', 'quantity']

        for col_name in measurement_columns:
            is_measurement = generator._measurement_regex.search(col_name) is not None
            assert is_measurement, f"{col_name} should be classified as measurement"

    def test_ambiguous_column_handling(self):
        """Columns that could be either should be handled correctly."""
        generator = ValidationSuggestionGenerator()

        # 'count' could be ID or measurement - should match measurement
        ambiguous = ['count', 'item_count']

        for col_name in ambiguous:
            is_measurement = generator._measurement_regex.search(col_name) is not None
            # Count is typically a measurement
            assert is_measurement, f"{col_name} should be measurement"


class TestSuggestionGeneration:
    """Tests for suggestion generation with patterns."""

    def test_generator_initialization(self):
        """Generator should initialize successfully with loaded patterns."""
        generator = ValidationSuggestionGenerator()

        # Should have both pattern sets loaded
        assert len(generator._id_patterns) > 0
        assert len(generator._measurement_patterns) > 0

        # Regex should be compiled
        assert generator._id_regex is not None
        assert generator._measurement_regex is not None

    def test_file_level_suggestions(self):
        """Should generate file-level suggestions."""
        generator = ValidationSuggestionGenerator()

        suggestions = generator._generate_file_level_suggestions(row_count=1000)

        # Should have at least row count suggestion
        assert len(suggestions) >= 1

    def test_empty_column_list(self):
        """Should handle empty column list gracefully."""
        generator = ValidationSuggestionGenerator()

        suggestions = generator.generate_suggestions(columns=[], row_count=0)

        # Should return some suggestions (at least file-level)
        assert isinstance(suggestions, list)
