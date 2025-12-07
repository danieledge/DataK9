"""
Wikidata Semantic Tagger for DataK9 Profiler.

Assigns general-knowledge semantic types based on Wikidata concepts.
This provides semantic coverage for domains not covered by FIBO (finance)
or Schema.org (web structures), including:
- Geographic locations and codes
- Reference data (languages, nationalities)
- Named entities (companies, organizations)
- Standard classification codes (NAICS, ICD, ISBN)
- Demographic attributes

DATA SOURCE:
Wikidata (wikidata.org) - CC0 Public Domain
No attribution required, but Wikidata is acknowledged as the data source.
https://www.wikidata.org/wiki/Wikidata:Licensing
"""

import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set

logger = logging.getLogger(__name__)


class WikidataReferenceLoader:
    """
    Loads reference data for Wikidata semantic types.

    Provides curated lists of valid values for validation purposes.
    Data is stored locally to avoid runtime API calls.
    """

    _country_names: Optional[Set[str]] = None
    _country_codes: Optional[Set[str]] = None
    _language_names: Optional[Set[str]] = None
    _language_codes: Optional[Set[str]] = None
    _timezones: Optional[Set[str]] = None
    _airport_codes: Optional[Set[str]] = None
    _nationalities: Optional[Set[str]] = None

    @classmethod
    def get_country_names(cls) -> Set[str]:
        """Get set of valid country names."""
        if cls._country_names is None:
            # Curated list of country names (ISO 3166-1 + common variants)
            cls._country_names = {
                # A
                "Afghanistan", "Albania", "Algeria", "Andorra", "Angola",
                "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria",
                "Azerbaijan",
                # B
                "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium",
                "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia and Herzegovina",
                "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi",
                # C
                "Cambodia", "Cameroon", "Canada", "Cape Verde", "Central African Republic",
                "Chad", "Chile", "China", "Colombia", "Comoros", "Congo", "Costa Rica",
                "Croatia", "Cuba", "Cyprus", "Czech Republic", "Czechia",
                # D-E
                "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador",
                "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia",
                "Eswatini", "Ethiopia",
                # F-G
                "Fiji", "Finland", "France", "Gabon", "Gambia", "Georgia", "Germany",
                "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau",
                "Guyana",
                # H-I
                "Haiti", "Honduras", "Hungary", "Iceland", "India", "Indonesia", "Iran",
                "Iraq", "Ireland", "Israel", "Italy",
                # J-K
                "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati",
                "North Korea", "South Korea", "Korea", "Kuwait", "Kyrgyzstan",
                # L
                "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya",
                "Liechtenstein", "Lithuania", "Luxembourg",
                # M
                "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta",
                "Marshall Islands", "Mauritania", "Mauritius", "Mexico", "Micronesia",
                "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique",
                "Myanmar", "Burma",
                # N
                "Namibia", "Nauru", "Nepal", "Netherlands", "New Zealand", "Nicaragua",
                "Niger", "Nigeria", "North Macedonia", "Norway",
                # O-P
                "Oman", "Pakistan", "Palau", "Palestine", "Panama", "Papua New Guinea",
                "Paraguay", "Peru", "Philippines", "Poland", "Portugal",
                # Q-R
                "Qatar", "Romania", "Russia", "Russian Federation", "Rwanda",
                # S
                "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent and the Grenadines",
                "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal",
                "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia",
                "Solomon Islands", "Somalia", "South Africa", "South Sudan", "Spain",
                "Sri Lanka", "Sudan", "Suriname", "Sweden", "Switzerland", "Syria",
                # T
                "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo",
                "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan",
                "Tuvalu",
                # U-V
                "Uganda", "Ukraine", "United Arab Emirates", "UAE", "United Kingdom", "UK",
                "United States", "USA", "US", "Uruguay", "Uzbekistan", "Vanuatu",
                "Vatican City", "Venezuela", "Vietnam",
                # Y-Z
                "Yemen", "Zambia", "Zimbabwe"
            }
        return cls._country_names

    @classmethod
    def get_country_codes(cls) -> Set[str]:
        """Get set of valid ISO 3166-1 alpha-2 and alpha-3 country codes."""
        if cls._country_codes is None:
            cls._country_codes = {
                # Alpha-2 codes
                "AF", "AL", "DZ", "AD", "AO", "AG", "AR", "AM", "AU", "AT", "AZ",
                "BS", "BH", "BD", "BB", "BY", "BE", "BZ", "BJ", "BT", "BO", "BA",
                "BW", "BR", "BN", "BG", "BF", "BI", "KH", "CM", "CA", "CV", "CF",
                "TD", "CL", "CN", "CO", "KM", "CG", "CD", "CR", "HR", "CU", "CY",
                "CZ", "DK", "DJ", "DM", "DO", "EC", "EG", "SV", "GQ", "ER", "EE",
                "SZ", "ET", "FJ", "FI", "FR", "GA", "GM", "GE", "DE", "GH", "GR",
                "GD", "GT", "GN", "GW", "GY", "HT", "HN", "HU", "IS", "IN", "ID",
                "IR", "IQ", "IE", "IL", "IT", "JM", "JP", "JO", "KZ", "KE", "KI",
                "KP", "KR", "KW", "KG", "LA", "LV", "LB", "LS", "LR", "LY", "LI",
                "LT", "LU", "MG", "MW", "MY", "MV", "ML", "MT", "MH", "MR", "MU",
                "MX", "FM", "MD", "MC", "MN", "ME", "MA", "MZ", "MM", "NA", "NR",
                "NP", "NL", "NZ", "NI", "NE", "NG", "MK", "NO", "OM", "PK", "PW",
                "PS", "PA", "PG", "PY", "PE", "PH", "PL", "PT", "QA", "RO", "RU",
                "RW", "KN", "LC", "VC", "WS", "SM", "ST", "SA", "SN", "RS", "SC",
                "SL", "SG", "SK", "SI", "SB", "SO", "ZA", "SS", "ES", "LK", "SD",
                "SR", "SE", "CH", "SY", "TW", "TJ", "TZ", "TH", "TL", "TG", "TO",
                "TT", "TN", "TR", "TM", "TV", "UG", "UA", "AE", "GB", "US", "UY",
                "UZ", "VU", "VA", "VE", "VN", "YE", "ZM", "ZW",
                # Alpha-3 codes (selected)
                "USA", "GBR", "CAN", "AUS", "DEU", "FRA", "JPN", "CHN", "IND", "BRA"
            }
        return cls._country_codes

    @classmethod
    def get_language_names(cls) -> Set[str]:
        """Get set of valid language names."""
        if cls._language_names is None:
            cls._language_names = {
                "Afrikaans", "Albanian", "Arabic", "Armenian", "Basque", "Bengali",
                "Bulgarian", "Catalan", "Chinese", "Croatian", "Czech", "Danish",
                "Dutch", "English", "Estonian", "Finnish", "French", "Galician",
                "Georgian", "German", "Greek", "Gujarati", "Hebrew", "Hindi",
                "Hungarian", "Icelandic", "Indonesian", "Irish", "Italian", "Japanese",
                "Kannada", "Korean", "Latvian", "Lithuanian", "Macedonian", "Malay",
                "Malayalam", "Maltese", "Marathi", "Norwegian", "Persian", "Polish",
                "Portuguese", "Punjabi", "Romanian", "Russian", "Serbian", "Slovak",
                "Slovenian", "Spanish", "Swahili", "Swedish", "Tamil", "Telugu",
                "Thai", "Turkish", "Ukrainian", "Urdu", "Vietnamese", "Welsh",
                "Mandarin", "Cantonese", "Simplified Chinese", "Traditional Chinese"
            }
        return cls._language_names

    @classmethod
    def get_language_codes(cls) -> Set[str]:
        """Get set of valid ISO 639-1 language codes."""
        if cls._language_codes is None:
            cls._language_codes = {
                "aa", "ab", "af", "am", "ar", "as", "ay", "az", "ba", "be", "bg",
                "bh", "bi", "bn", "bo", "br", "ca", "co", "cs", "cy", "da", "de",
                "dz", "el", "en", "eo", "es", "et", "eu", "fa", "fi", "fj", "fo",
                "fr", "fy", "ga", "gd", "gl", "gn", "gu", "ha", "he", "hi", "hr",
                "hu", "hy", "ia", "id", "ie", "ik", "is", "it", "ja", "jv", "ka",
                "kk", "kl", "km", "kn", "ko", "ks", "ku", "ky", "la", "ln", "lo",
                "lt", "lv", "mg", "mi", "mk", "ml", "mn", "mr", "ms", "mt", "my",
                "na", "ne", "nl", "no", "oc", "om", "or", "pa", "pl", "ps", "pt",
                "qu", "rm", "rn", "ro", "ru", "rw", "sa", "sd", "sg", "si", "sk",
                "sl", "sm", "sn", "so", "sq", "sr", "ss", "st", "su", "sv", "sw",
                "ta", "te", "tg", "th", "ti", "tk", "tl", "tn", "to", "tr", "ts",
                "tt", "tw", "ug", "uk", "ur", "uz", "vi", "vo", "wo", "xh", "yi",
                "yo", "za", "zh", "zu",
                # Common 3-letter codes
                "eng", "spa", "fra", "deu", "zho", "jpn", "ara", "hin", "por", "rus"
            }
        return cls._language_codes

    @classmethod
    def get_timezones(cls) -> Set[str]:
        """Get set of valid timezone identifiers."""
        if cls._timezones is None:
            cls._timezones = {
                # UTC variants
                "UTC", "GMT", "UTC+0", "UTC-0",
                # US timezones
                "America/New_York", "America/Chicago", "America/Denver",
                "America/Los_Angeles", "America/Anchorage", "Pacific/Honolulu",
                "EST", "CST", "MST", "PST", "EDT", "CDT", "MDT", "PDT",
                # Europe
                "Europe/London", "Europe/Paris", "Europe/Berlin", "Europe/Rome",
                "Europe/Madrid", "Europe/Amsterdam", "Europe/Brussels",
                "CET", "CEST", "WET", "WEST",
                # Asia
                "Asia/Tokyo", "Asia/Shanghai", "Asia/Hong_Kong", "Asia/Singapore",
                "Asia/Dubai", "Asia/Kolkata", "Asia/Seoul",
                "JST", "CST", "IST", "KST",
                # Australia
                "Australia/Sydney", "Australia/Melbourne", "Australia/Perth",
                "AEST", "AEDT", "AWST",
                # Generic offsets
                "UTC+1", "UTC+2", "UTC+3", "UTC+4", "UTC+5", "UTC+6", "UTC+7",
                "UTC+8", "UTC+9", "UTC+10", "UTC+11", "UTC+12",
                "UTC-1", "UTC-2", "UTC-3", "UTC-4", "UTC-5", "UTC-6", "UTC-7",
                "UTC-8", "UTC-9", "UTC-10", "UTC-11", "UTC-12"
            }
        return cls._timezones

    @classmethod
    def get_airport_codes(cls) -> Set[str]:
        """Get set of common IATA airport codes."""
        if cls._airport_codes is None:
            # Top 200+ busiest airports by IATA code
            cls._airport_codes = {
                # Americas
                "ATL", "LAX", "ORD", "DFW", "DEN", "JFK", "SFO", "SEA", "LAS", "MCO",
                "EWR", "CLT", "PHX", "IAH", "MIA", "BOS", "MSP", "FLL", "DTW", "PHL",
                "LGA", "BWI", "SLC", "SAN", "IAD", "DCA", "TPA", "PDX", "HNL", "STL",
                "YYZ", "YVR", "YUL", "MEX", "GRU", "GIG", "BOG", "LIM", "SCL", "EZE",
                # Europe
                "LHR", "CDG", "FRA", "AMS", "MAD", "BCN", "FCO", "MUC", "LGW", "ORY",
                "ZRH", "VIE", "BRU", "CPH", "OSL", "ARN", "HEL", "DUB", "MAN", "STN",
                "LIS", "ATH", "PRG", "WAW", "BUD", "LED",
                # Asia-Pacific
                "PEK", "PVG", "HKG", "NRT", "HND", "ICN", "SIN", "BKK", "KUL", "CGK",
                "DEL", "BOM", "MAA", "BLR", "SYD", "MEL", "BNE", "AKL",
                # Middle East/Africa
                "DXB", "DOH", "AUH", "JED", "RUH", "TLV", "CAI", "JNB", "CPT"
            }
        return cls._airport_codes

    @classmethod
    def get_nationalities(cls) -> Set[str]:
        """Get set of nationality/demonym names."""
        if cls._nationalities is None:
            cls._nationalities = {
                "American", "British", "Canadian", "Australian", "German", "French",
                "Italian", "Spanish", "Mexican", "Brazilian", "Japanese", "Chinese",
                "Indian", "Korean", "Russian", "Dutch", "Belgian", "Swiss", "Austrian",
                "Swedish", "Norwegian", "Danish", "Finnish", "Polish", "Czech",
                "Greek", "Portuguese", "Irish", "Scottish", "Welsh", "Turkish",
                "Egyptian", "South African", "Nigerian", "Kenyan", "Moroccan",
                "Israeli", "Saudi", "Emirati", "Iranian", "Iraqi", "Pakistani",
                "Thai", "Vietnamese", "Filipino", "Indonesian", "Malaysian",
                "Singaporean", "New Zealander", "Argentine", "Colombian", "Chilean",
                "Venezuelan", "Cuban", "Jamaican"
            }
        return cls._nationalities

    @classmethod
    def get_industry_sectors(cls) -> Set[str]:
        """Get set of industry sector names."""
        return {
            "Agriculture", "Mining", "Construction", "Manufacturing", "Utilities",
            "Wholesale Trade", "Retail Trade", "Transportation", "Information",
            "Finance", "Real Estate", "Professional Services", "Education",
            "Healthcare", "Hospitality", "Entertainment", "Government",
            "Technology", "Telecommunications", "Energy", "Aerospace", "Defense",
            "Automotive", "Pharmaceuticals", "Biotechnology", "Consumer Goods",
            "Food and Beverage", "Media", "Insurance", "Banking"
        }


class WikidataTagger:
    """
    Assigns semantic types to columns using Wikidata-derived taxonomy.

    Covers general-knowledge domains not handled by FIBO or Schema.org:
    - Geographic: countries, cities, coordinates, airport codes
    - Reference: languages, nationalities, timezones
    - Entities: companies, organizations, species
    - Codes: NAICS, ICD, ISBN, DOI
    - Demographics: gender, marital status, education

    DATA SOURCE:
    Wikidata (wikidata.org) - CC0 Public Domain
    """

    MIN_CONFIDENCE_THRESHOLD = 0.55

    def __init__(self, taxonomy_path: Optional[str] = None, min_confidence: float = None):
        """
        Initialize Wikidata tagger.

        Args:
            taxonomy_path: Path to wikidata_taxonomy.json (default: auto-detect)
            min_confidence: Minimum confidence threshold (default: 0.55)
        """
        self.taxonomy = self._load_taxonomy(taxonomy_path)
        self.tag_definitions = self._flatten_taxonomy()
        self.min_confidence = min_confidence if min_confidence is not None else self.MIN_CONFIDENCE_THRESHOLD
        logger.info(f"Loaded {len(self.tag_definitions)} Wikidata semantic definitions")

    def _load_taxonomy(self, taxonomy_path: Optional[str] = None) -> Dict[str, Any]:
        """Load Wikidata taxonomy from JSON file."""
        if taxonomy_path is None:
            current_dir = Path(__file__).parent
            taxonomy_path = current_dir / "taxonomies" / "wikidata_taxonomy.json"

        try:
            with open(taxonomy_path, 'r') as f:
                taxonomy = json.load(f)
                logger.debug(f"Loaded Wikidata taxonomy from {taxonomy_path}")
                return taxonomy
        except Exception as e:
            logger.warning(f"Failed to load Wikidata taxonomy from {taxonomy_path}: {e}")
            return {"metadata": {}, "taxonomy": {}}

    def _flatten_taxonomy(self) -> Dict[str, Dict[str, Any]]:
        """Flatten hierarchical taxonomy into a lookup dict."""
        flattened = {}
        taxonomy = self.taxonomy.get("taxonomy", {})

        for category, category_data in taxonomy.items():
            tags = category_data.get("tags", {})
            for tag_name, tag_def in tags.items():
                flattened[tag_name] = {
                    **tag_def,
                    "category": category,
                    "wikidata_class": tag_def.get("wikidata_class")
                }

        return flattened

    def tag_column(
        self,
        column_name: str,
        inferred_type: str,
        statistics: Any,
        quality: Any = None,
        sample_values: Optional[List[Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Assign Wikidata semantic type to a column.

        Args:
            column_name: Name of the column
            inferred_type: DataK9 inferred type (integer, float, string, etc.)
            statistics: ColumnStatistics object
            quality: QualityMetrics object
            sample_values: Optional sample values for pattern detection

        Returns:
            Dict with wikidata semantic info if match found:
            {
                "type": "geo.country",
                "confidence": 0.85,
                "signals": ["name_pattern:country", "low_cardinality"],
                "display_label": "Country",
                "wikidata_class": "Q6256",
                "validation_rules": ["ValidValuesCheck"],
                "reference_source": {...}
            }
            None if no match found above threshold
        """
        candidates = []

        for tag_name, tag_def in self.tag_definitions.items():
            score, signals = self._score_candidate(
                tag_name,
                tag_def,
                column_name,
                inferred_type,
                statistics,
                quality,
                sample_values
            )

            if score >= self.min_confidence:
                candidates.append({
                    "type": tag_name,
                    "confidence": score,
                    "signals": signals,
                    "display_label": tag_def.get("definition", tag_name).split(".")[0],
                    "wikidata_class": tag_def.get("wikidata_class"),
                    "category": tag_def.get("category"),
                    "validation_rules": tag_def.get("validation_rules", []),
                    "reference_source": tag_def.get("reference_source"),
                    "expected_values": tag_def.get("expected_values"),
                    "ml_rare_behavior": tag_def.get("ml_rare_behavior"),
                    "ml_rare_reason": tag_def.get("ml_rare_reason")
                })

        if not candidates:
            return None

        # Sort by confidence
        candidates.sort(key=lambda c: c["confidence"], reverse=True)
        best = candidates[0]

        return {
            "type": best["type"],
            "confidence": round(best["confidence"], 3),
            "signals": best["signals"],
            "display_label": self._get_display_label(best["type"]),
            "wikidata_class": best["wikidata_class"],
            "category": best["category"],
            "validation_rules": best["validation_rules"],
            "reference_source": best["reference_source"],
            "expected_values": best["expected_values"],
            "ml_rare_behavior": best["ml_rare_behavior"],
            "ml_rare_reason": best["ml_rare_reason"]
        }

    def _score_candidate(
        self,
        tag_name: str,
        tag_def: Dict[str, Any],
        column_name: str,
        inferred_type: str,
        statistics: Any,
        quality: Any,
        sample_values: Optional[List[Any]]
    ) -> Tuple[float, List[str]]:
        """
        Score how well a column matches a Wikidata semantic type.

        Returns:
            (score, signals) tuple
        """
        score = 0.0
        signals = []

        # Pattern matching on column name (strongest signal)
        patterns = tag_def.get("patterns", [])
        pattern_matched = False
        for pattern in patterns:
            try:
                if re.search(pattern, column_name, re.IGNORECASE):
                    score += 0.5
                    signals.append(f"name_pattern:{pattern[:25]}")
                    pattern_matched = True
                    break
            except re.error:
                continue

        # If no pattern match, unlikely to be this type
        if not pattern_matched:
            return 0.0, signals

        # Data type matching
        data_props = tag_def.get("data_properties", {})
        expected_types = data_props.get("type", [])
        if expected_types and inferred_type in expected_types:
            score += 0.2
            signals.append(f"dtype:{inferred_type}")
        elif expected_types and inferred_type not in expected_types:
            score -= 0.3

        # Cardinality checks
        cardinality = getattr(statistics, 'cardinality', None)
        unique_count = getattr(statistics, 'unique_count', None)

        if 'cardinality_min' in data_props and cardinality is not None:
            if cardinality >= data_props['cardinality_min']:
                score += 0.1
                signals.append(f"high_cardinality:{cardinality:.2f}")
            else:
                score -= 0.1

        if 'cardinality_max' in data_props and cardinality is not None:
            if cardinality <= data_props['cardinality_max']:
                score += 0.15
                signals.append(f"low_cardinality:{cardinality:.2f}")
            else:
                score -= 0.15

        # Value range checks (for coordinates)
        min_val = getattr(statistics, 'min_value', None)
        max_val = getattr(statistics, 'max_value', None)

        if 'min_value' in data_props and min_val is not None:
            try:
                if float(min_val) >= data_props['min_value']:
                    score += 0.1
                    signals.append(f"min_ok:{min_val}")
                else:
                    score -= 0.2
            except (ValueError, TypeError):
                pass

        if 'max_value' in data_props and max_val is not None:
            try:
                if float(max_val) <= data_props['max_value']:
                    score += 0.1
                    signals.append(f"max_ok:{max_val}")
                else:
                    score -= 0.2
            except (ValueError, TypeError):
                pass

        # String length checks
        if 'string_length' in data_props:
            min_len, max_len = data_props['string_length']
            actual_min = getattr(statistics, 'min_length', None)
            actual_max = getattr(statistics, 'max_length', None)
            if actual_min is not None and actual_max is not None:
                if actual_min >= min_len and actual_max <= max_len:
                    score += 0.15
                    signals.append(f"length_ok:{actual_min}-{actual_max}")
                else:
                    score -= 0.1

        # Sample value validation against reference data
        if sample_values and 'reference_source' in tag_def:
            ref_source = tag_def['reference_source']
            loader_method = ref_source.get('method')
            if loader_method:
                match_ratio = self._validate_sample_values(sample_values, loader_method)
                if match_ratio > 0.7:
                    score += 0.2
                    signals.append(f"ref_match:{match_ratio:.0%}")
                elif match_ratio < 0.3:
                    score -= 0.15

        # Check against expected_values
        if sample_values and 'expected_values' in tag_def:
            expected = set(v.lower() for v in tag_def['expected_values'])
            sample_lower = set(str(v).lower() for v in sample_values if v is not None)
            match_count = len(sample_lower & expected)
            if match_count > 0:
                score += 0.15
                signals.append(f"expected_match:{match_count}")

        return max(0, min(1, score)), signals

    def _validate_sample_values(self, sample_values: List[Any], loader_method: str) -> float:
        """
        Validate sample values against reference data.

        Returns:
            Ratio of sample values that match reference data (0.0 to 1.0)
        """
        try:
            # Get reference data
            method = getattr(WikidataReferenceLoader, loader_method, None)
            if not method:
                return 0.0

            reference_values = method()
            reference_lower = {v.lower() for v in reference_values}

            # Check sample values
            valid_samples = [str(v).strip() for v in sample_values if v is not None]
            if not valid_samples:
                return 0.0

            match_count = sum(1 for v in valid_samples if v.lower() in reference_lower)
            return match_count / len(valid_samples)
        except Exception as e:
            logger.debug(f"Reference validation failed: {e}")
            return 0.0

    def _get_display_label(self, tag_name: str) -> str:
        """Get human-readable display label for a tag."""
        labels = {
            # Geographic
            "geo.country": "Country",
            "geo.country_code": "Country code",
            "geo.city": "City",
            "geo.state_province": "State/Province",
            "geo.continent": "Continent",
            "geo.latitude": "Latitude",
            "geo.longitude": "Longitude",
            "geo.postal_code": "Postal code",
            "geo.airport_code": "Airport code",
            "geo.port_code": "Port code",
            # Reference
            "ref.language": "Language",
            "ref.language_code": "Language code",
            "ref.timezone": "Timezone",
            "ref.unit": "Unit of measure",
            "ref.nationality": "Nationality",
            "ref.religion": "Religion",
            "ref.ethnicity": "Ethnicity",
            # Entity
            "entity.company": "Company",
            "entity.university": "University",
            "entity.brand": "Brand",
            "entity.species": "Species",
            "entity.occupation": "Occupation",
            "entity.industry": "Industry",
            # Codes
            "code.naics": "NAICS code",
            "code.sic": "SIC code",
            "code.hs": "HS tariff code",
            "code.icd": "ICD code",
            "code.isbn": "ISBN",
            "code.doi": "DOI",
            # Demographics
            "demo.gender": "Gender",
            "demo.age_group": "Age group",
            "demo.marital_status": "Marital status",
            "demo.education_level": "Education level"
        }
        return labels.get(tag_name, tag_name.replace(".", " ").title())

    def get_validation_rules(self, wikidata_tag: str) -> List[str]:
        """
        Get recommended validation rules for a Wikidata tag.

        Args:
            wikidata_tag: Wikidata tag (e.g., 'geo.country')

        Returns:
            List of validation rule names
        """
        if wikidata_tag in self.tag_definitions:
            return self.tag_definitions[wikidata_tag].get('validation_rules', [])
        return []

    def get_expected_values(self, wikidata_tag: str) -> Optional[Set[str]]:
        """
        Get expected valid values for a Wikidata tag.

        Args:
            wikidata_tag: Wikidata tag (e.g., 'geo.continent')

        Returns:
            Set of valid values or None if not applicable
        """
        if wikidata_tag not in self.tag_definitions:
            return None

        tag_def = self.tag_definitions[wikidata_tag]

        # Check for static expected_values
        expected = tag_def.get('expected_values')
        if expected:
            return set(expected)

        # Check for reference source
        ref_source = tag_def.get('reference_source')
        if ref_source:
            method_name = ref_source.get('method')
            if method_name:
                method = getattr(WikidataReferenceLoader, method_name, None)
                if method:
                    try:
                        return method()
                    except Exception as e:
                        logger.warning(f"Failed to load reference values for {wikidata_tag}: {e}")

        return None

    def get_ml_behavior(self, wikidata_tag: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Get ML anomaly detection behavior for a Wikidata tag.

        Args:
            wikidata_tag: Wikidata tag

        Returns:
            Tuple of (behavior, reason)
            behavior: 'skip', 'reference_validate', or None
            reason: Explanation string
        """
        if wikidata_tag not in self.tag_definitions:
            return None, None

        tag_def = self.tag_definitions[wikidata_tag]
        return (
            tag_def.get('ml_rare_behavior'),
            tag_def.get('ml_rare_reason')
        )
