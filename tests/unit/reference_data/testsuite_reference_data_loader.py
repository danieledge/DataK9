"""
Tests for ReferenceDataLoader.

Validates that reference data loading works correctly for:
- ISO 4217 currencies via pycountry
- ISO 3166-1 countries via pycountry
- PII patterns from JSON
- Column indicators from JSON
- ID/measurement patterns from JSON
"""

import pytest
from validation_framework.reference_data import ReferenceDataLoader


class TestCurrencyLoading:
    """Tests for currency reference data loading."""

    def test_get_currencies_returns_nonempty_set(self):
        """Currency loader should return non-empty set."""
        currencies = ReferenceDataLoader.get_currencies()
        assert len(currencies) > 100, "Should have 100+ currencies from ISO 4217"

    def test_get_currencies_includes_major_codes(self):
        """Major currency codes should be present."""
        currencies = ReferenceDataLoader.get_currencies()
        major_codes = ['usd', 'eur', 'gbp', 'jpy', 'cny', 'chf']
        for code in major_codes:
            assert code in currencies, f"Major currency {code} should be present"

    def test_get_currencies_includes_aliases(self):
        """Currency aliases should be included by default."""
        currencies = ReferenceDataLoader.get_currencies(include_aliases=True)
        aliases = ['dollar', 'euro', 'pound', 'yen']
        for alias in aliases:
            assert alias in currencies, f"Alias {alias} should be present"

    def test_get_currencies_without_aliases(self):
        """Currency aliases should be excluded when requested."""
        currencies = ReferenceDataLoader.get_currencies(include_aliases=False)
        assert 'dollar' not in currencies, "Alias 'dollar' should not be present"

    def test_get_currency_codes_only(self):
        """Should return only 3-letter ISO codes."""
        codes = ReferenceDataLoader.get_currency_codes_only()
        assert len(codes) > 150, "Should have 150+ ISO currency codes"
        for code in codes:
            assert len(code) == 3, f"Currency code should be 3 chars: {code}"
            assert code.isupper(), f"Currency code should be uppercase: {code}"

    def test_is_valid_currency(self):
        """Currency validation should work."""
        assert ReferenceDataLoader.is_valid_currency('USD')
        assert ReferenceDataLoader.is_valid_currency('usd')
        assert ReferenceDataLoader.is_valid_currency('dollar')
        assert not ReferenceDataLoader.is_valid_currency('INVALID')

    def test_normalize_currency(self):
        """Currency normalization should return ISO codes."""
        assert ReferenceDataLoader.normalize_currency('dollar') == 'USD'
        assert ReferenceDataLoader.normalize_currency('USD') == 'USD'
        assert ReferenceDataLoader.normalize_currency('euro') == 'EUR'
        assert ReferenceDataLoader.normalize_currency('invalid') is None


class TestCountryLoading:
    """Tests for country reference data loading."""

    def test_get_countries_returns_nonempty_set(self):
        """Country loader should return non-empty set."""
        countries = ReferenceDataLoader.get_countries()
        assert len(countries) > 200, "Should have 200+ country identifiers"

    def test_get_countries_includes_major_codes(self):
        """Major country codes should be present."""
        countries = ReferenceDataLoader.get_countries()
        major_codes = ['us', 'gb', 'de', 'fr', 'cn', 'jp']
        for code in major_codes:
            assert code in countries, f"Major country {code} should be present"

    def test_get_countries_includes_full_names(self):
        """Full country names should be present."""
        countries = ReferenceDataLoader.get_countries()
        assert 'united states of america' in countries
        assert 'germany' in countries

    def test_get_countries_includes_aliases(self):
        """Country aliases should be included by default."""
        countries = ReferenceDataLoader.get_countries(include_aliases=True)
        aliases = ['usa', 'uk', 'britain']
        for alias in aliases:
            assert alias in countries, f"Alias {alias} should be present"

    def test_get_country_codes_only(self):
        """Should return only 2-letter ISO codes."""
        codes = ReferenceDataLoader.get_country_codes_only()
        assert len(codes) >= 249, "Should have 249 ISO country codes"
        for code in codes:
            assert len(code) == 2, f"Country code should be 2 chars: {code}"
            assert code.isupper(), f"Country code should be uppercase: {code}"

    def test_is_valid_country(self):
        """Country validation should work."""
        assert ReferenceDataLoader.is_valid_country('US')
        assert ReferenceDataLoader.is_valid_country('us')
        assert ReferenceDataLoader.is_valid_country('usa')
        assert not ReferenceDataLoader.is_valid_country('INVALID')

    def test_normalize_country(self):
        """Country normalization should return ISO alpha-2 codes."""
        assert ReferenceDataLoader.normalize_country('usa') == 'US'
        assert ReferenceDataLoader.normalize_country('US') == 'US'
        assert ReferenceDataLoader.normalize_country('uk') == 'GB'
        assert ReferenceDataLoader.normalize_country('invalid') is None


class TestPIIPatterns:
    """Tests for PII pattern loading from JSON."""

    def test_get_pii_patterns_returns_nonempty(self):
        """PII patterns should be loaded from JSON."""
        patterns = ReferenceDataLoader.get_pii_patterns()
        assert len(patterns) > 0, "Should have PII patterns loaded"

    def test_pii_patterns_have_required_fields(self):
        """Each PII pattern should have required fields."""
        patterns = ReferenceDataLoader.get_pii_patterns()
        required_fields = ['regex', 'name', 'risk_score']
        for pii_type, config in patterns.items():
            for field in required_fields:
                assert field in config, f"Pattern {pii_type} missing {field}"

    def test_pii_patterns_include_common_types(self):
        """Common PII types should be present."""
        patterns = ReferenceDataLoader.get_pii_patterns()
        common_types = ['email', 'ssn', 'credit_card', 'phone_us']
        for pii_type in common_types:
            assert pii_type in patterns, f"Common PII type {pii_type} should be present"


class TestColumnIndicators:
    """Tests for column indicator loading from JSON."""

    def test_get_column_indicators_returns_nonempty(self):
        """Column indicators should be loaded from JSON."""
        indicators = ReferenceDataLoader.get_column_indicators()
        assert len(indicators) > 0, "Should have column indicators loaded"

    def test_column_indicators_have_required_fields(self):
        """Each column indicator should have required fields."""
        indicators = ReferenceDataLoader.get_column_indicators()
        required_fields = ['keywords', 'risk_score', 'pii_type']
        for category, config in indicators.items():
            for field in required_fields:
                assert field in config, f"Indicator {category} missing {field}"


class TestValidationPatterns:
    """Tests for validation pattern loading from JSON."""

    def test_get_id_patterns_returns_list(self):
        """ID patterns should return a list."""
        patterns = ReferenceDataLoader.get_id_patterns()
        assert isinstance(patterns, list)
        assert len(patterns) > 0, "Should have ID patterns loaded"

    def test_id_patterns_include_common_patterns(self):
        """Common ID patterns should be present."""
        patterns = ReferenceDataLoader.get_id_patterns()
        # Check for presence of common patterns
        patterns_str = ' '.join(patterns)
        assert '_id' in patterns_str or 'id' in patterns_str

    def test_get_measurement_patterns_returns_list(self):
        """Measurement patterns should return a list."""
        patterns = ReferenceDataLoader.get_measurement_patterns()
        assert isinstance(patterns, list)
        assert len(patterns) > 0, "Should have measurement patterns loaded"

    def test_measurement_patterns_include_common_patterns(self):
        """Common measurement patterns should be present."""
        patterns = ReferenceDataLoader.get_measurement_patterns()
        # Check for presence of common measurement patterns
        patterns_str = ' '.join(patterns)
        assert 'amount' in patterns_str or 'price' in patterns_str


class TestCaching:
    """Tests for caching behavior."""

    def test_cache_is_populated(self):
        """Cache should be populated after first load."""
        ReferenceDataLoader.clear_cache()
        assert len(ReferenceDataLoader._cache) == 0

        # Load currencies
        ReferenceDataLoader.get_currencies()
        assert len(ReferenceDataLoader._cache) > 0

    def test_cache_reuse(self):
        """Subsequent calls should reuse cached data."""
        ReferenceDataLoader.clear_cache()

        # First call
        currencies1 = ReferenceDataLoader.get_currencies()
        cache_size_after_first = len(ReferenceDataLoader._cache)

        # Second call
        currencies2 = ReferenceDataLoader.get_currencies()

        # Cache size should be same (data reused)
        assert len(ReferenceDataLoader._cache) == cache_size_after_first
        # Should return same set
        assert currencies1 == currencies2


class TestDataSourceInfo:
    """Tests for data source provenance."""

    def test_get_data_source_info(self):
        """Should return source information."""
        info = ReferenceDataLoader.get_data_source_info()
        assert 'sources' in info
        assert 'countries' in info['sources']
        assert 'currencies' in info['sources']
        assert info['sources']['countries']['standard'] == 'ISO 3166-1'
        assert info['sources']['currencies']['standard'] == 'ISO 4217'
