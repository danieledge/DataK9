"""
Semantic Configuration Loader for DataK9 Profiler.

Loads semantic_config.yaml and provides unified access to:
- Scoring weights for semantic detection
- Resolution thresholds for multi-layer classification
- Global negative patterns to prevent false positives
- Value hint defaults for type-specific constraints
- Value patterns for regex-based detection on sample data
- Name tokens for column name matching

This module uses a singleton pattern to ensure config is loaded once
and shared across all semantic taggers.
"""

import re
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Set, Pattern
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Try to import yaml, fall back gracefully
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    logger.warning("PyYAML not installed. Using default semantic config values.")


@dataclass
class ScoringWeights:
    """Configurable scoring weights for semantic detection."""
    name_pattern_match: float = 0.50
    name_pattern_negative: float = -0.40
    dtype_match: float = 0.20
    dtype_mismatch: float = -0.30
    cardinality_match: float = 0.15
    cardinality_mismatch: float = -0.10
    value_range_match: float = 0.10
    value_pattern_match: float = 0.30
    unique_ratio_match: float = 0.15
    string_length_match: float = 0.10


@dataclass
class ResolutionThresholds:
    """Configurable resolution thresholds for semantic layer selection."""
    fibo_min_conf: float = 0.70
    science_min_conf: float = 0.60
    wikidata_min_conf: float = 0.60
    schema_min_conf: float = 0.50
    fallback_conf: float = 0.30


@dataclass
class ValuePatternDef:
    """Definition of a value-based pattern for semantic detection."""
    pattern: str
    compiled_pattern: Optional[Pattern] = None
    min_match_ratio: float = 0.50
    tags: List[str] = field(default_factory=list)
    confidence: float = 0.90
    skip_if_name_matches: List[str] = field(default_factory=list)
    compiled_skip_patterns: List[Pattern] = field(default_factory=list)


@dataclass
class NameTokens:
    """Token-based name matching for quick classification."""
    positive: Set[str] = field(default_factory=set)
    negative: Set[str] = field(default_factory=set)


@dataclass
class SemanticConfig:
    """Complete semantic detection configuration."""
    version: str = "1.0.0"
    scoring: ScoringWeights = field(default_factory=ScoringWeights)
    resolution: ResolutionThresholds = field(default_factory=ResolutionThresholds)
    global_negative_patterns: Dict[str, List[Pattern]] = field(default_factory=dict)
    value_hints_defaults: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    value_patterns: Dict[str, ValuePatternDef] = field(default_factory=dict)
    name_tokens: Dict[str, NameTokens] = field(default_factory=dict)
    validation_rules: Dict[str, List[str]] = field(default_factory=dict)


class SemanticConfigLoader:
    """
    Loads and provides semantic configuration.

    Uses singleton pattern to ensure config is loaded once and shared.
    Falls back to sensible defaults if config file is missing or invalid.
    """

    _instance: Optional['SemanticConfigLoader'] = None
    _config: Optional[SemanticConfig] = None
    _config_path: Optional[Path] = None

    def __new__(cls, config_path: Optional[Path] = None):
        """Singleton pattern for config loader."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the config loader.

        Args:
            config_path: Optional path to semantic_config.yaml.
                        If None, uses default location in taxonomies/.
        """
        # Skip re-initialization if already loaded
        if self._initialized and self._config is not None:
            return

        if config_path is None:
            config_path = Path(__file__).parent / "taxonomies" / "semantic_config.yaml"

        self._config_path = config_path
        self._config = self._load_config(config_path)
        self._initialized = True
        logger.info(f"Loaded semantic config v{self._config.version} from {config_path}")

    def _load_config(self, path: Path) -> SemanticConfig:
        """Load config from YAML file."""
        if not YAML_AVAILABLE:
            logger.warning("YAML not available, using default config")
            return self._get_default_config()

        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
            return self._parse_config(data)
        except FileNotFoundError:
            logger.warning(f"Semantic config not found at {path}, using defaults")
            return self._get_default_config()
        except Exception as e:
            logger.warning(f"Failed to load semantic config from {path}: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> SemanticConfig:
        """Return default configuration when YAML is unavailable or invalid."""
        return SemanticConfig(
            version="1.0.0-default",
            scoring=ScoringWeights(),
            resolution=ResolutionThresholds(),
            global_negative_patterns={},
            value_hints_defaults={},
            value_patterns={},
            name_tokens={},
            validation_rules={}
        )

    def _parse_config(self, data: Dict[str, Any]) -> SemanticConfig:
        """Parse YAML data into SemanticConfig."""
        # Parse scoring weights
        scoring_data = data.get('scoring', {})
        scoring = ScoringWeights(
            name_pattern_match=scoring_data.get('name_pattern_match', 0.50),
            name_pattern_negative=scoring_data.get('name_pattern_negative', -0.40),
            dtype_match=scoring_data.get('dtype_match', 0.20),
            dtype_mismatch=scoring_data.get('dtype_mismatch', -0.30),
            cardinality_match=scoring_data.get('cardinality_match', 0.15),
            cardinality_mismatch=scoring_data.get('cardinality_mismatch', -0.10),
            value_range_match=scoring_data.get('value_range_match', 0.10),
            value_pattern_match=scoring_data.get('value_pattern_match', 0.30),
            unique_ratio_match=scoring_data.get('unique_ratio_match', 0.15),
            string_length_match=scoring_data.get('string_length_match', 0.10)
        )

        # Parse resolution thresholds
        resolution_data = data.get('resolution', {})
        resolution = ResolutionThresholds(
            fibo_min_conf=resolution_data.get('fibo_min_conf', 0.70),
            science_min_conf=resolution_data.get('science_min_conf', 0.60),
            wikidata_min_conf=resolution_data.get('wikidata_min_conf', 0.60),
            schema_min_conf=resolution_data.get('schema_min_conf', 0.50),
            fallback_conf=resolution_data.get('fallback_conf', 0.30)
        )

        # Parse and compile global negative patterns
        global_negative_patterns = {}
        for category, patterns in data.get('global_negative_patterns', {}).items():
            compiled = []
            for pattern in patterns:
                try:
                    compiled.append(re.compile(pattern, re.IGNORECASE))
                except re.error as e:
                    logger.warning(f"Invalid negative pattern for {category}: {pattern} - {e}")
            global_negative_patterns[category] = compiled

        # Parse value hints defaults (no compilation needed)
        value_hints_defaults = data.get('value_hints_defaults', {})

        # Parse and compile value patterns
        value_patterns = {}
        for name, pattern_data in data.get('value_patterns', {}).items():
            try:
                compiled_pattern = re.compile(pattern_data.get('pattern', ''), re.IGNORECASE)
                skip_patterns = []
                for skip in pattern_data.get('skip_if_name_matches', []):
                    try:
                        skip_patterns.append(re.compile(skip, re.IGNORECASE))
                    except re.error:
                        pass

                value_patterns[name] = ValuePatternDef(
                    pattern=pattern_data.get('pattern', ''),
                    compiled_pattern=compiled_pattern,
                    min_match_ratio=pattern_data.get('min_match_ratio', 0.50),
                    tags=pattern_data.get('tags', []),
                    confidence=pattern_data.get('confidence', 0.90),
                    skip_if_name_matches=pattern_data.get('skip_if_name_matches', []),
                    compiled_skip_patterns=skip_patterns
                )
            except re.error as e:
                logger.warning(f"Invalid value pattern {name}: {e}")

        # Parse name tokens
        name_tokens = {}
        for semantic_type, tokens in data.get('name_tokens', {}).items():
            name_tokens[semantic_type] = NameTokens(
                positive=set(t.lower() for t in tokens.get('positive', [])),
                negative=set(t.lower() for t in tokens.get('negative', []))
            )

        # Parse validation rules
        validation_rules = data.get('validation_rules', {})

        return SemanticConfig(
            version=data.get('version', '1.0.0'),
            scoring=scoring,
            resolution=resolution,
            global_negative_patterns=global_negative_patterns,
            value_hints_defaults=value_hints_defaults,
            value_patterns=value_patterns,
            name_tokens=name_tokens,
            validation_rules=validation_rules
        )

    @property
    def config(self) -> SemanticConfig:
        """Get the loaded configuration."""
        return self._config

    @property
    def scoring(self) -> ScoringWeights:
        """Get scoring weights."""
        return self._config.scoring

    @property
    def resolution(self) -> ResolutionThresholds:
        """Get resolution thresholds."""
        return self._config.resolution

    def get_negative_patterns(self, semantic_category: str) -> List[Pattern]:
        """
        Get compiled negative patterns for a semantic category.

        Args:
            semantic_category: Category name (e.g., 'monetary', 'identifier')

        Returns:
            List of compiled regex patterns that should NOT match this category
        """
        return self._config.global_negative_patterns.get(semantic_category, [])

    def matches_negative_pattern(self, semantic_category: str, column_name: str) -> bool:
        """
        Check if a column name matches any negative pattern for a category.

        Args:
            semantic_category: Category name (e.g., 'monetary', 'identifier')
            column_name: Column name to check

        Returns:
            True if column should be excluded from this category
        """
        patterns = self.get_negative_patterns(semantic_category)
        for pattern in patterns:
            if pattern.search(column_name):
                logger.debug(
                    f"Column '{column_name}' excluded from {semantic_category} "
                    f"by negative pattern: {pattern.pattern}"
                )
                return True
        return False

    def get_value_hints(self, hint_type: str) -> Dict[str, Any]:
        """
        Get default value hints for a semantic type.

        Args:
            hint_type: Type name (e.g., 'identifier', 'categorical', 'boolean')

        Returns:
            Dictionary of value-based constraints
        """
        return self._config.value_hints_defaults.get(hint_type, {})

    def get_value_pattern(self, pattern_name: str) -> Optional[ValuePatternDef]:
        """
        Get a value pattern definition by name.

        Args:
            pattern_name: Pattern name (e.g., 'email', 'phone', 'uuid')

        Returns:
            ValuePatternDef or None if not found
        """
        return self._config.value_patterns.get(pattern_name)

    def get_all_value_patterns(self) -> Dict[str, ValuePatternDef]:
        """Get all value pattern definitions."""
        return self._config.value_patterns

    def get_name_tokens(self, semantic_type: str) -> Optional[NameTokens]:
        """
        Get name tokens for quick column name matching.

        Args:
            semantic_type: Type name (e.g., 'monetary', 'identifier')

        Returns:
            NameTokens with positive and negative token sets
        """
        return self._config.name_tokens.get(semantic_type)

    def check_name_tokens(self, semantic_type: str, column_name: str) -> Optional[bool]:
        """
        Check if column name contains positive or negative tokens.

        Args:
            semantic_type: Type to check against
            column_name: Column name to analyze

        Returns:
            True if positive token match (no negative),
            False if negative token match,
            None if no token match
        """
        tokens = self.get_name_tokens(semantic_type)
        if not tokens:
            return None

        name_lower = column_name.lower()
        name_parts = set(re.split(r'[_\s]+', name_lower))

        # Check for negative tokens first (exclusion takes priority)
        if tokens.negative:
            for token in tokens.negative:
                if token in name_lower or token in name_parts:
                    return False

        # Check for positive tokens
        if tokens.positive:
            for token in tokens.positive:
                if token in name_lower or token in name_parts:
                    return True

        return None

    def get_validation_rules(self, semantic_type: str) -> List[str]:
        """
        Get recommended validation rules for a semantic type.

        Args:
            semantic_type: Type name (e.g., 'monetary', 'identifier')

        Returns:
            List of validation rule names
        """
        return self._config.validation_rules.get(semantic_type, [])

    @classmethod
    def reset(cls):
        """
        Reset the singleton instance.

        Primarily used for testing to force reload of config.
        """
        cls._instance = None
        cls._config = None


def get_semantic_config() -> SemanticConfigLoader:
    """
    Get the singleton SemanticConfigLoader instance.

    Convenience function for accessing the config loader.

    Returns:
        SemanticConfigLoader instance
    """
    return SemanticConfigLoader()
