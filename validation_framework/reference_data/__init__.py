"""
Centralized reference data management for DataK9.

Provides authoritative reference data from trusted sources:
- ISO 3166-1: Countries (via pycountry)
- ISO 4217: Currencies (via pycountry)
- ISO 639: Languages (via pycountry)
- PII patterns: Regex patterns for sensitive data detection
- Column indicators: Semantic column name patterns

All reference data is loaded lazily and cached for performance.
Data sources are documented with provenance for audit purposes.
"""

from validation_framework.reference_data.loader import ReferenceDataLoader

__all__ = ['ReferenceDataLoader']
