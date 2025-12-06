# Reference Data System

DataK9 uses a centralized reference data system to validate values against authoritative ISO standards and maintain consistent pattern definitions across the profiler and validation components.

---

## Overview

The `reference_data` module provides:

1. **ISO Standard Data** - Countries (ISO 3166-1) and currencies (ISO 4217) via pycountry
2. **PII Detection Patterns** - Regex patterns and risk scores for sensitive data
3. **Column Indicators** - Semantic column name patterns
4. **Validation Patterns** - ID vs measurement column detection

All reference data is loaded lazily and cached for performance.

---

## Architecture

```
validation_framework/
└── reference_data/
    ├── __init__.py          # Exports ReferenceDataLoader
    ├── loader.py            # Main loader class
    ├── pii/
    │   ├── patterns.json    # PII regex patterns with metadata
    │   └── column_indicators.json  # Semantic column name indicators
    └── patterns/
        └── validation_patterns.json  # ID and measurement patterns
```

---

## ReferenceDataLoader API

### Currency Methods

```python
from validation_framework.reference_data import ReferenceDataLoader

# Get all valid currencies (ISO 4217 + aliases)
currencies = ReferenceDataLoader.get_currencies(include_aliases=True)
# Returns: {'usd', 'dollar', 'eur', 'euro', 'gbp', ...}

# Get only ISO 4217 codes (uppercase)
codes = ReferenceDataLoader.get_currency_codes_only()
# Returns: {'USD', 'EUR', 'GBP', 'JPY', ...}

# Validate a currency
ReferenceDataLoader.is_valid_currency('USD')  # True
ReferenceDataLoader.is_valid_currency('dollar')  # True
ReferenceDataLoader.is_valid_currency('invalid')  # False

# Normalize to ISO code
ReferenceDataLoader.normalize_currency('dollar')  # Returns 'USD'
ReferenceDataLoader.normalize_currency('euro')    # Returns 'EUR'
```

### Country Methods

```python
# Get all valid countries (ISO 3166-1 + aliases)
countries = ReferenceDataLoader.get_countries(include_aliases=True)
# Returns: {'us', 'usa', 'united states', 'gb', 'uk', ...}

# Get only ISO 3166-1 alpha-2 codes
codes = ReferenceDataLoader.get_country_codes_only()
# Returns: {'US', 'GB', 'DE', 'FR', ...}

# Validate a country
ReferenceDataLoader.is_valid_country('US')   # True
ReferenceDataLoader.is_valid_country('usa')  # True (alias)

# Normalize to ISO code
ReferenceDataLoader.normalize_country('usa')  # Returns 'US'
ReferenceDataLoader.normalize_country('uk')   # Returns 'GB'
```

### Pattern Methods

```python
# PII detection patterns
patterns = ReferenceDataLoader.get_pii_patterns()
# Returns: {'email': {'regex': '...', 'name': 'Email', 'risk_score': 70, ...}, ...}

# Column name indicators
indicators = ReferenceDataLoader.get_column_indicators()
# Returns: {'email': {'keywords': ['email', 'mail'], 'risk_score': 70, ...}, ...}

# ID patterns (columns that should NOT receive range checks)
id_patterns = ReferenceDataLoader.get_id_patterns()
# Returns: ['_id$', '^id_', 'account', 'customer', ...]

# Measurement patterns (columns that SHOULD receive range checks)
measurement_patterns = ReferenceDataLoader.get_measurement_patterns()
# Returns: ['amount', 'price', 'quantity', ...]
```

### Utility Methods

```python
# Get data source provenance information
info = ReferenceDataLoader.get_data_source_info()
# Returns: {'sources': {'countries': {'standard': 'ISO 3166-1', ...}, ...}}

# Clear cache (useful for testing)
ReferenceDataLoader.clear_cache()
```

---

## Data Sources

### ISO Standards (via pycountry)

- **ISO 3166-1**: 249 countries with alpha-2, alpha-3 codes and names
- **ISO 4217**: 181 currencies with 3-letter codes and names
- **ISO 639**: Languages (available but not currently used)

pycountry is a hard dependency that works fully offline with bundled data.

### Currency Aliases

Common informal names mapped to ISO codes:

| Alias | ISO Code |
|-------|----------|
| dollar | USD |
| euro | EUR |
| pound, sterling | GBP |
| yen | JPY |
| yuan, renminbi | CNY |

### Country Aliases

Common informal names mapped to ISO alpha-2 codes:

| Alias | ISO Code |
|-------|----------|
| usa, america | US |
| uk, britain | GB |
| uae | AE |
| korea | KR |

---

## JSON Configuration Files

### PII Patterns (`pii/patterns.json`)

```json
{
  "patterns": {
    "email": {
      "regex": "\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b",
      "name": "Email Address",
      "risk_score": 70,
      "regulatory": ["GDPR Article 6", "CCPA"]
    }
  }
}
```

Fields:
- `regex` - Pattern for value matching
- `name` - Human-readable name
- `risk_score` - 0-100 privacy risk score
- `regulatory` - Applicable compliance frameworks

### Column Indicators (`pii/column_indicators.json`)

```json
{
  "indicators": {
    "email": {
      "keywords": ["email", "e_mail", "mail"],
      "risk_score": 70,
      "pii_type": "email"
    }
  }
}
```

Fields:
- `keywords` - Column name patterns to match
- `risk_score` - Privacy risk when column name matches
- `pii_type` - Associated PII pattern type

### Validation Patterns (`patterns/validation_patterns.json`)

```json
{
  "id_patterns": ["_id$", "account", "customer"],
  "measurement_patterns": ["amount", "price", "quantity"]
}
```

---

## Integration Points

### ml_analyzer.py

Uses `ReferenceDataLoader.get_currencies()` and `get_countries()` for known domain detection to prevent false positives in rare category analysis.

### pii_detector.py

Loads PII patterns and column indicators from JSON for flexible, maintainable PII detection.

### validation_suggester.py

Loads ID and measurement patterns from JSON to determine which numeric columns should receive range check suggestions.

### semantic_tagger.py

Uses `get_expected_values()` to dynamically load ISO-standard values for semantic types like `money.currency`.

---

## Extending Reference Data

### Adding New Currency Aliases

Edit `loader.py` CURRENCY_ALIASES dict:

```python
CURRENCY_ALIASES = {
    'dollar': 'USD',
    'my_custom_alias': 'XYZ',
    ...
}
```

### Adding New PII Patterns

Edit `pii/patterns.json`:

```json
{
  "patterns": {
    "my_new_pattern": {
      "regex": "...",
      "name": "My Pattern",
      "risk_score": 80,
      "regulatory": ["GDPR"]
    }
  }
}
```

### Adding ID/Measurement Patterns

Edit `patterns/validation_patterns.json`:

```json
{
  "id_patterns": [..., "my_new_id_pattern"],
  "measurement_patterns": [..., "my_new_measurement_pattern"]
}
```

---

## Testing

Run reference data tests:

```bash
pytest tests/unit/reference_data/ -v
```

Key test coverage:
- Currency/country loading and caching
- Alias handling
- Normalization methods
- PII pattern loading
- Validation pattern loading
