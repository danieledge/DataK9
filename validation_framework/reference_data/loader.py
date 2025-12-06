"""
Reference data loader with caching.

Loads authoritative reference data from pycountry (ISO standards).
pycountry is a required dependency providing:
- ISO 3166-1: Countries (249 countries)
- ISO 4217: Currencies (180+ currencies)
- ISO 639: Languages

All data is cached after first load for performance.
"""

import json
import logging
from pathlib import Path
from typing import Set, Dict, Any, Optional, List

import pycountry  # Required dependency - bundled ISO data, works offline

logger = logging.getLogger(__name__)


class ReferenceDataLoader:
    """
    Centralized loader for reference data with caching.

    Provides access to:
    - ISO 3166-1 countries (249 countries) via pycountry
    - ISO 4217 currencies (180+ currencies) via pycountry
    - ISO 639 languages via pycountry
    - PII detection patterns (from bundled JSON)
    - Column name indicators (from bundled JSON)
    - Validation patterns (ID, measurement) (from bundled JSON)

    Data is loaded lazily and cached for performance.
    pycountry provides authoritative ISO data that works fully offline.
    """

    _cache: Dict[str, Any] = {}
    _data_dir = Path(__file__).parent

    # Common currency aliases (informal names -> ISO code)
    CURRENCY_ALIASES = {
        'dollar': 'USD', 'dollars': 'USD', 'us dollar': 'USD', 'usd': 'USD',
        'euro': 'EUR', 'euros': 'EUR', 'eur': 'EUR',
        'pound': 'GBP', 'pounds': 'GBP', 'sterling': 'GBP', 'uk pound': 'GBP', 'gbp': 'GBP',
        'yen': 'JPY', 'jpy': 'JPY',
        'yuan': 'CNY', 'renminbi': 'CNY', 'rmb': 'CNY', 'cny': 'CNY',
        'franc': 'CHF', 'swiss franc': 'CHF', 'chf': 'CHF',
        'rupee': 'INR', 'rupees': 'INR', 'inr': 'INR',
        'peso': 'MXN', 'mexican peso': 'MXN', 'mxn': 'MXN',
        'real': 'BRL', 'reais': 'BRL', 'brazil real': 'BRL', 'brl': 'BRL',
        'won': 'KRW', 'krw': 'KRW',
        'baht': 'THB', 'thb': 'THB',
        'ringgit': 'MYR', 'myr': 'MYR',
        'rupiah': 'IDR', 'idr': 'IDR',
        'zloty': 'PLN', 'pln': 'PLN',
        'shekel': 'ILS', 'ils': 'ILS',
        'rand': 'ZAR', 'zar': 'ZAR',
        'lira': 'TRY', 'turkish lira': 'TRY', 'try': 'TRY',
        'dirham': 'AED', 'aed': 'AED',
        'riyal': 'SAR', 'sar': 'SAR',
        'ruble': 'RUB', 'rouble': 'RUB', 'rub': 'RUB',
        'krone': 'NOK', 'norwegian krone': 'NOK', 'nok': 'NOK',
        'krona': 'SEK', 'swedish krona': 'SEK', 'sek': 'SEK',
        # Crypto (not in ISO but commonly seen)
        'bitcoin': 'BTC', 'btc': 'BTC',
        'ethereum': 'ETH', 'eth': 'ETH',
        'crypto': 'CRYPTO',
    }

    # Common country aliases (informal names -> ISO alpha-2)
    COUNTRY_ALIASES = {
        'usa': 'US', 'united states': 'US', 'united states of america': 'US', 'america': 'US',
        'uk': 'GB', 'united kingdom': 'GB', 'great britain': 'GB', 'britain': 'GB', 'england': 'GB',
        'uae': 'AE', 'emirates': 'AE',
        'korea': 'KR', 'south korea': 'KR',
        'russia': 'RU',
        'china': 'CN',
        'japan': 'JP',
        'germany': 'DE',
        'france': 'FR',
        'italy': 'IT',
        'spain': 'ES',
        'canada': 'CA',
        'australia': 'AU',
        'brazil': 'BR',
        'mexico': 'MX',
        'india': 'IN',
        'netherlands': 'NL', 'holland': 'NL',
        'switzerland': 'CH',
        'sweden': 'SE',
        'norway': 'NO',
    }

    @classmethod
    def get_currencies(cls, include_aliases: bool = True) -> Set[str]:
        """
        Get all valid currency codes (ISO 4217 + common aliases).

        Args:
            include_aliases: Include informal names like 'dollar', 'euro'

        Returns:
            Set of valid currency identifiers (lowercase for matching)
        """
        cache_key = f'currencies_{include_aliases}'
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        currencies = set()

        # Get all ISO 4217 currency codes from pycountry
        for currency in pycountry.currencies:
            currencies.add(currency.alpha_3.lower())
            if hasattr(currency, 'name'):
                currencies.add(currency.name.lower())
        logger.debug(f"Loaded {len(currencies)} currencies from pycountry (ISO 4217)")

        # Add common aliases
        if include_aliases:
            currencies.update(cls.CURRENCY_ALIASES.keys())

        cls._cache[cache_key] = currencies
        return currencies

    @classmethod
    def get_currency_codes_only(cls) -> Set[str]:
        """Get only ISO 4217 3-letter currency codes (no aliases)."""
        cache_key = 'currency_codes_only'
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        codes = set()
        for currency in pycountry.currencies:
            codes.add(currency.alpha_3.upper())

        cls._cache[cache_key] = codes
        return codes

    @classmethod
    def get_countries(cls, include_aliases: bool = True) -> Set[str]:
        """
        Get all valid country identifiers (ISO 3166-1 + common aliases).

        Args:
            include_aliases: Include informal names like 'usa', 'uk'

        Returns:
            Set of valid country identifiers (lowercase for matching)
        """
        cache_key = f'countries_{include_aliases}'
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        countries = set()

        # Get all ISO 3166-1 country codes and names from pycountry
        for country in pycountry.countries:
            countries.add(country.alpha_2.lower())
            countries.add(country.alpha_3.lower())
            countries.add(country.name.lower())
            if hasattr(country, 'official_name'):
                countries.add(country.official_name.lower())
        logger.debug(f"Loaded {len(countries)} country identifiers from pycountry (ISO 3166-1)")

        # Add common aliases
        if include_aliases:
            countries.update(cls.COUNTRY_ALIASES.keys())

        cls._cache[cache_key] = countries
        return countries

    @classmethod
    def get_country_codes_only(cls) -> Set[str]:
        """Get only ISO 3166-1 alpha-2 country codes (no aliases)."""
        cache_key = 'country_codes_only'
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        codes = set()
        for country in pycountry.countries:
            codes.add(country.alpha_2.upper())

        cls._cache[cache_key] = codes
        return codes

    @classmethod
    def get_pii_patterns(cls) -> Dict[str, Dict[str, Any]]:
        """
        Get PII detection patterns with metadata.

        Returns:
            Dict mapping PII type to pattern config:
            {
                'email': {
                    'regex': r'...',
                    'name': 'Email Address',
                    'risk_score': 70,
                    'regulatory': ['GDPR Article 6', 'CCPA']
                },
                ...
            }
        """
        cache_key = 'pii_patterns'
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        json_path = cls._data_dir / 'pii' / 'patterns.json'
        if json_path.exists():
            with open(json_path, 'r') as f:
                data = json.load(f)
                cls._cache[cache_key] = data.get('patterns', {})
        else:
            # Return empty - will use component defaults
            cls._cache[cache_key] = {}

        return cls._cache[cache_key]

    @classmethod
    def get_column_indicators(cls) -> Dict[str, Dict[str, Any]]:
        """
        Get semantic column name indicators for PII detection.

        Returns:
            Dict mapping indicator type to config:
            {
                'email': {
                    'keywords': ['email', 'e_mail', 'mail'],
                    'risk_score': 70,
                    'pii_type': 'email'
                },
                ...
            }
        """
        cache_key = 'column_indicators'
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        json_path = cls._data_dir / 'pii' / 'column_indicators.json'
        if json_path.exists():
            with open(json_path, 'r') as f:
                data = json.load(f)
                cls._cache[cache_key] = data.get('indicators', {})
        else:
            cls._cache[cache_key] = {}

        return cls._cache[cache_key]

    @classmethod
    def get_id_patterns(cls) -> List[str]:
        """
        Get regex patterns that identify ID/code columns.

        These columns should NOT receive range checks.

        Returns:
            List of regex pattern strings
        """
        cache_key = 'id_patterns'
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        json_path = cls._data_dir / 'patterns' / 'validation_patterns.json'
        if json_path.exists():
            with open(json_path, 'r') as f:
                data = json.load(f)
                cls._cache[cache_key] = data.get('id_patterns', [])
        else:
            cls._cache[cache_key] = []

        return cls._cache[cache_key]

    @classmethod
    def get_measurement_patterns(cls) -> List[str]:
        """
        Get regex patterns that identify measurement columns.

        These columns SHOULD receive range checks.

        Returns:
            List of regex pattern strings
        """
        cache_key = 'measurement_patterns'
        if cache_key in cls._cache:
            return cls._cache[cache_key]

        json_path = cls._data_dir / 'patterns' / 'validation_patterns.json'
        if json_path.exists():
            with open(json_path, 'r') as f:
                data = json.load(f)
                cls._cache[cache_key] = data.get('measurement_patterns', [])
        else:
            cls._cache[cache_key] = []

        return cls._cache[cache_key]

    @classmethod
    def is_valid_currency(cls, value: str) -> bool:
        """Check if a value is a valid currency code or name."""
        return value.lower() in cls.get_currencies(include_aliases=True)

    @classmethod
    def is_valid_country(cls, value: str) -> bool:
        """Check if a value is a valid country code or name."""
        return value.lower() in cls.get_countries(include_aliases=True)

    @classmethod
    def normalize_currency(cls, value: str) -> Optional[str]:
        """
        Normalize a currency value to ISO 4217 code.

        Args:
            value: Currency string (e.g., 'dollar', 'USD', 'us dollar')

        Returns:
            ISO 4217 code or None if not recognized
        """
        value_lower = value.lower().strip()

        # Check aliases first
        if value_lower in cls.CURRENCY_ALIASES:
            return cls.CURRENCY_ALIASES[value_lower]

        # Check if already an ISO code
        value_upper = value.upper().strip()
        if len(value_upper) == 3 and value_upper in cls.get_currency_codes_only():
            return value_upper

        # Try pycountry lookup
        try:
            currency = pycountry.currencies.lookup(value)
            return currency.alpha_3
        except LookupError:
            pass

        return None

    @classmethod
    def normalize_country(cls, value: str) -> Optional[str]:
        """
        Normalize a country value to ISO 3166-1 alpha-2 code.

        Args:
            value: Country string (e.g., 'usa', 'US', 'United States')

        Returns:
            ISO 3166-1 alpha-2 code or None if not recognized
        """
        value_lower = value.lower().strip()

        # Check aliases first
        if value_lower in cls.COUNTRY_ALIASES:
            return cls.COUNTRY_ALIASES[value_lower]

        # Check if already an ISO code
        value_upper = value.upper().strip()
        if len(value_upper) == 2 and value_upper in cls.get_country_codes_only():
            return value_upper

        # Try pycountry lookup
        try:
            country = pycountry.countries.lookup(value)
            return country.alpha_2
        except LookupError:
            pass

        return None

    @classmethod
    def get_data_source_info(cls) -> Dict[str, Any]:
        """
        Get provenance information about data sources.

        Returns:
            Dict with source information for audit purposes
        """
        return {
            'sources': {
                'countries': {
                    'standard': 'ISO 3166-1',
                    'source': 'pycountry',
                    'record_count': len(list(pycountry.countries))
                },
                'currencies': {
                    'standard': 'ISO 4217',
                    'source': 'pycountry',
                    'record_count': len(list(pycountry.currencies))
                },
                'languages': {
                    'standard': 'ISO 639-3',
                    'source': 'pycountry',
                    'record_count': len(list(pycountry.languages))
                }
            },
            'pii_patterns': {
                'source': 'bundled JSON',
                'path': str(cls._data_dir / 'pii' / 'patterns.json')
            },
            'validation_patterns': {
                'source': 'bundled JSON',
                'path': str(cls._data_dir / 'patterns' / 'validation_patterns.json')
            }
        }

    @classmethod
    def clear_cache(cls):
        """Clear all cached data (useful for testing)."""
        cls._cache.clear()
