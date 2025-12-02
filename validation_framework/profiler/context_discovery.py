"""
Context Discovery Module for Self-Calibrating Anomaly Detection.

Discovers contextual patterns in data that explain apparent outliers:
- Subgroup patterns: categorical columns that segment numeric distributions
- Correlations: numeric columns that move together
- Implicit rules: logical constraints between fields

This enables the profiler to distinguish genuine anomalies from
values that are simply "different but expected" for their context.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from itertools import combinations

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class FieldDescription:
    """Human-friendly field description from YAML config."""
    name: str
    friendly_name: str
    description: str = ""

    def display_name(self) -> str:
        """Return friendly name with original in parentheses if different."""
        if self.friendly_name and self.friendly_name != self.name:
            return f"{self.friendly_name} ({self.name})"
        return self.name


@dataclass
class SubgroupStats:
    """Statistics for a numeric column within a categorical subgroup."""
    segment_value: Any
    count: int
    mean: float
    std: float
    min_val: float
    max_val: float
    q1: float
    q3: float

    def is_within_bounds(self, value: float, num_std: float = 2.5) -> bool:
        """Check if value is within expected range for this subgroup."""
        if self.std == 0:
            return abs(value - self.mean) < 0.001
        return abs(value - self.mean) <= num_std * self.std

    def z_score_in_group(self, value: float) -> float:
        """Calculate z-score relative to this subgroup's distribution."""
        if self.std == 0:
            return 0.0 if abs(value - self.mean) < 0.001 else float('inf')
        return abs(value - self.mean) / self.std


@dataclass
class SubgroupPattern:
    """A discovered pattern where a categorical column segments a numeric column."""
    segment_col: str
    value_col: str
    variance_explained: float  # R-squared: how much variance is explained by segmentation
    stats_by_segment: Dict[Any, SubgroupStats] = field(default_factory=dict)

    def explain_value(self, segment_value: Any, numeric_value: float, field_descs: Dict[str, FieldDescription]) -> Tuple[bool, str, float]:
        """
        Check if a numeric value is explained by its segment.

        Returns:
            (is_explained, explanation_text, confidence)
        """
        # Try to find the segment value with type coercion
        # (sample_rows from ML analyzer may have string values for numeric columns)
        stats = None
        matched_segment = segment_value

        if segment_value in self.stats_by_segment:
            stats = self.stats_by_segment[segment_value]
        else:
            # Try type coercion for numeric strings
            if isinstance(segment_value, str):
                try:
                    # Try as int first
                    int_val = int(segment_value)
                    if int_val in self.stats_by_segment:
                        stats = self.stats_by_segment[int_val]
                        matched_segment = int_val
                except ValueError:
                    try:
                        # Try as float
                        float_val = float(segment_value)
                        if float_val in self.stats_by_segment:
                            stats = self.stats_by_segment[float_val]
                            matched_segment = float_val
                    except ValueError:
                        pass

        if stats is None:
            return False, "", 0.0

        # Get friendly names
        seg_desc = field_descs.get(self.segment_col, FieldDescription(self.segment_col, self.segment_col))
        val_desc = field_descs.get(self.value_col, FieldDescription(self.value_col, self.value_col))

        z_in_group = stats.z_score_in_group(numeric_value)

        if z_in_group <= 2.5:
            # Value is normal for this segment
            confidence = max(0, 1.0 - (z_in_group / 3.0))
            explanation = (
                f"Normal for {seg_desc.friendly_name}={matched_segment} "
                f"(avg {val_desc.friendly_name}=${stats.mean:,.0f} for this group)"
            )
            return True, explanation, confidence

        return False, "", 0.0


@dataclass
class CorrelationPattern:
    """A discovered correlation between two numeric columns."""
    col1: str
    col2: str
    correlation: float
    slope: float = 0.0
    intercept: float = 0.0
    tolerance: float = 0.0  # Expected prediction error (std of residuals)

    def predict(self, col1_value: float) -> float:
        """Predict col2 value from col1 value using linear relationship."""
        return self.slope * col1_value + self.intercept

    def explain_value(self, col1_value: float, col2_value: float, field_descs: Dict[str, FieldDescription]) -> Tuple[bool, str, float]:
        """
        Check if col2 value is explained by correlation with col1.

        Returns:
            (is_explained, explanation_text, confidence)
        """
        predicted = self.predict(col1_value)
        error = abs(col2_value - predicted)

        col1_desc = field_descs.get(self.col1, FieldDescription(self.col1, self.col1))
        col2_desc = field_descs.get(self.col2, FieldDescription(self.col2, self.col2))

        if self.tolerance > 0 and error <= 2 * self.tolerance:
            confidence = max(0, 1.0 - (error / (3 * self.tolerance)))
            direction = "increases" if self.correlation > 0 else "decreases"
            explanation = (
                f"{col2_desc.friendly_name} {direction} with {col1_desc.friendly_name} "
                f"(correlation: {self.correlation:.2f})"
            )
            return True, explanation, confidence

        return False, "", 0.0


@dataclass
class ContextStore:
    """Container for all discovered contextual patterns."""
    subgroups: List[SubgroupPattern] = field(default_factory=list)
    correlations: List[CorrelationPattern] = field(default_factory=list)
    field_descriptions: Dict[str, FieldDescription] = field(default_factory=dict)
    discovery_stats: Dict[str, Any] = field(default_factory=dict)

    def get_subgroups_for_column(self, col: str) -> List[SubgroupPattern]:
        """Get all subgroup patterns that explain a given column."""
        return [sg for sg in self.subgroups if sg.value_col == col]

    def get_correlations_for_column(self, col: str) -> List[CorrelationPattern]:
        """Get all correlations involving a given column."""
        return [c for c in self.correlations if c.col1 == col or c.col2 == col]

    def get_field_display_name(self, col: str) -> str:
        """Get human-friendly display name for a column."""
        if col in self.field_descriptions:
            return self.field_descriptions[col].display_name()
        return col

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'subgroups': [
                {
                    'segment_col': sg.segment_col,
                    'value_col': sg.value_col,
                    'variance_explained': sg.variance_explained,
                    'num_segments': len(sg.stats_by_segment)
                }
                for sg in self.subgroups
            ],
            'correlations': [
                {
                    'col1': c.col1,
                    'col2': c.col2,
                    'correlation': c.correlation
                }
                for c in self.correlations
            ],
            'field_descriptions': {
                col: {'friendly_name': fd.friendly_name, 'description': fd.description}
                for col, fd in self.field_descriptions.items()
            },
            'discovery_stats': self.discovery_stats
        }


class ContextDiscovery:
    """
    Discovers contextual patterns in data for smarter anomaly detection.

    Usage:
        discovery = ContextDiscovery(field_descriptions={'Parch': {'friendly_name': 'Parents/Children'}})
        context = discovery.discover(df)
    """

    def __init__(
        self,
        field_descriptions: Optional[Dict[str, Dict[str, str]]] = None,
        min_variance_explained: float = 0.20,  # 20% variance explained threshold
        min_correlation: float = 0.5,  # Minimum correlation to consider
        max_categorical_unique: int = 20,  # Max unique values for categorical
        min_segment_size: int = 10  # Minimum rows per segment
    ):
        """
        Initialize context discovery.

        Args:
            field_descriptions: Dict mapping column names to friendly descriptions
                Example: {'Parch': {'friendly_name': 'Parents/Children', 'description': 'Number of parents/children aboard'}}
            min_variance_explained: Minimum R-squared for subgroup pattern (default 0.20)
            min_correlation: Minimum absolute correlation to consider (default 0.5)
            max_categorical_unique: Max unique values for a column to be categorical (default 20)
            min_segment_size: Minimum rows per segment to include (default 10)
        """
        self.min_variance_explained = min_variance_explained
        self.min_correlation = min_correlation
        self.max_categorical_unique = max_categorical_unique
        self.min_segment_size = min_segment_size

        # Parse field descriptions
        self.field_descriptions: Dict[str, FieldDescription] = {}
        if field_descriptions:
            for col, desc in field_descriptions.items():
                if isinstance(desc, dict):
                    self.field_descriptions[col] = FieldDescription(
                        name=col,
                        friendly_name=desc.get('friendly_name', col),
                        description=desc.get('description', '')
                    )
                elif isinstance(desc, str):
                    self.field_descriptions[col] = FieldDescription(
                        name=col,
                        friendly_name=desc,
                        description=''
                    )

    def discover(self, df) -> ContextStore:
        """
        Run all discovery methods and return context store.

        Args:
            df: Polars or pandas DataFrame

        Returns:
            ContextStore with discovered patterns
        """
        logger.info("Starting context discovery...")

        # Convert to Polars if needed
        if not HAS_POLARS:
            logger.warning("Polars not available, context discovery limited")
            return ContextStore(field_descriptions=self.field_descriptions)

        if not isinstance(df, pl.DataFrame):
            try:
                df = pl.from_pandas(df)
            except Exception as e:
                logger.warning(f"Could not convert to Polars: {e}")
                return ContextStore(field_descriptions=self.field_descriptions)

        # Identify column types
        numeric_cols = [c for c in df.columns if df[c].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]]
        categorical_cols = self._identify_categorical_columns(df)

        logger.info(f"Found {len(numeric_cols)} numeric, {len(categorical_cols)} categorical columns")

        # Discover patterns
        subgroups = self._discover_subgroups(df, categorical_cols, numeric_cols)
        correlations = self._discover_correlations(df, numeric_cols)

        context = ContextStore(
            subgroups=subgroups,
            correlations=correlations,
            field_descriptions=self.field_descriptions,
            discovery_stats={
                'numeric_columns': len(numeric_cols),
                'categorical_columns': len(categorical_cols),
                'subgroup_patterns_found': len(subgroups),
                'correlation_patterns_found': len(correlations)
            }
        )

        logger.info(f"Context discovery complete: {len(subgroups)} subgroup patterns, {len(correlations)} correlations")

        return context

    def _identify_categorical_columns(self, df: 'pl.DataFrame') -> List[str]:
        """Identify columns suitable for categorical segmentation."""
        categorical = []

        for col in df.columns:
            dtype = df[col].dtype

            # String columns with limited unique values
            if dtype == pl.Utf8:
                n_unique = df[col].n_unique()
                if n_unique <= self.max_categorical_unique and n_unique >= 2:
                    categorical.append(col)

            # Integer columns with limited unique values (like Pclass)
            elif dtype in [pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
                n_unique = df[col].n_unique()
                if 2 <= n_unique <= self.max_categorical_unique:
                    categorical.append(col)

        return categorical

    def _discover_subgroups(
        self,
        df: 'pl.DataFrame',
        categorical_cols: List[str],
        numeric_cols: List[str]
    ) -> List[SubgroupPattern]:
        """
        Discover categorical columns that segment numeric distributions.

        Uses variance reduction (R-squared) to measure how much a categorical
        column explains variance in a numeric column.
        """
        patterns = []

        for cat_col in categorical_cols:
            for num_col in numeric_cols:
                if cat_col == num_col:
                    continue

                try:
                    # Calculate variance explained
                    variance_explained = self._calculate_variance_explained(df, cat_col, num_col)

                    if variance_explained >= self.min_variance_explained:
                        # Build stats per segment
                        stats_by_segment = self._calculate_segment_stats(df, cat_col, num_col)

                        if stats_by_segment:  # Only if we have valid segments
                            patterns.append(SubgroupPattern(
                                segment_col=cat_col,
                                value_col=num_col,
                                variance_explained=variance_explained,
                                stats_by_segment=stats_by_segment
                            ))
                            logger.debug(f"Found subgroup pattern: {cat_col} → {num_col} (R²={variance_explained:.2f})")

                except Exception as e:
                    logger.debug(f"Error analyzing {cat_col} → {num_col}: {e}")
                    continue

        # Sort by variance explained (most explanatory first)
        patterns.sort(key=lambda p: p.variance_explained, reverse=True)

        return patterns

    def _calculate_variance_explained(self, df: 'pl.DataFrame', cat_col: str, num_col: str) -> float:
        """
        Calculate R-squared: how much variance in num_col is explained by cat_col.

        R² = 1 - (SS_within / SS_total)
        """
        try:
            # Filter to non-null values
            valid = df.filter(pl.col(num_col).is_not_null() & pl.col(cat_col).is_not_null())

            if len(valid) < 20:
                return 0.0

            # Total variance
            total_mean = valid[num_col].mean()
            ss_total = ((valid[num_col] - total_mean) ** 2).sum()

            if ss_total == 0:
                return 0.0

            # Within-group variance
            ss_within = 0.0
            for segment in valid[cat_col].unique().to_list():
                segment_data = valid.filter(pl.col(cat_col) == segment)[num_col]
                if len(segment_data) >= self.min_segment_size:
                    segment_mean = segment_data.mean()
                    ss_within += ((segment_data - segment_mean) ** 2).sum()

            r_squared = 1.0 - (ss_within / ss_total)
            return max(0.0, min(1.0, r_squared))

        except Exception as e:
            logger.debug(f"Variance calculation error: {e}")
            return 0.0

    def _calculate_segment_stats(
        self,
        df: 'pl.DataFrame',
        cat_col: str,
        num_col: str
    ) -> Dict[Any, SubgroupStats]:
        """Calculate statistics for each segment."""
        stats = {}

        valid = df.filter(pl.col(num_col).is_not_null() & pl.col(cat_col).is_not_null())

        for segment in valid[cat_col].unique().to_list():
            segment_data = valid.filter(pl.col(cat_col) == segment)[num_col]

            if len(segment_data) < self.min_segment_size:
                continue

            try:
                stats[segment] = SubgroupStats(
                    segment_value=segment,
                    count=len(segment_data),
                    mean=float(segment_data.mean()),
                    std=float(segment_data.std()) if segment_data.std() else 0.0,
                    min_val=float(segment_data.min()),
                    max_val=float(segment_data.max()),
                    q1=float(segment_data.quantile(0.25)),
                    q3=float(segment_data.quantile(0.75))
                )
            except Exception as e:
                logger.debug(f"Error calculating stats for {cat_col}={segment}: {e}")
                continue

        return stats

    def _discover_correlations(
        self,
        df: 'pl.DataFrame',
        numeric_cols: List[str]
    ) -> List[CorrelationPattern]:
        """Discover strong correlations between numeric columns."""
        patterns = []

        # Limit pairs to check for performance
        max_pairs = 50
        pairs = list(combinations(numeric_cols, 2))[:max_pairs]

        for col1, col2 in pairs:
            try:
                # Get valid pairs
                valid = df.filter(
                    pl.col(col1).is_not_null() & pl.col(col2).is_not_null()
                ).select([col1, col2])

                if len(valid) < 20:
                    continue

                # Calculate correlation
                arr1 = valid[col1].to_numpy()
                arr2 = valid[col2].to_numpy()

                corr = np.corrcoef(arr1, arr2)[0, 1]

                if abs(corr) >= self.min_correlation:
                    # Calculate linear regression for prediction
                    slope, intercept = np.polyfit(arr1, arr2, 1)

                    # Calculate residual std (tolerance)
                    predicted = slope * arr1 + intercept
                    residuals = arr2 - predicted
                    tolerance = np.std(residuals)

                    patterns.append(CorrelationPattern(
                        col1=col1,
                        col2=col2,
                        correlation=float(corr),
                        slope=float(slope),
                        intercept=float(intercept),
                        tolerance=float(tolerance)
                    ))
                    logger.debug(f"Found correlation: {col1} ↔ {col2} (r={corr:.2f})")

            except Exception as e:
                logger.debug(f"Error analyzing correlation {col1} ↔ {col2}: {e}")
                continue

        # Sort by absolute correlation (strongest first)
        patterns.sort(key=lambda p: abs(p.correlation), reverse=True)

        return patterns


def load_field_descriptions(yaml_path: str) -> Dict[str, Dict[str, str]]:
    """
    Load field descriptions from YAML file.

    Expected format:
    ```yaml
    field_descriptions:
      Pclass:
        friendly_name: "Passenger Class"
        description: "Ticket class (1=First, 2=Second, 3=Third)"
      Parch:
        friendly_name: "Parents/Children"
        description: "Number of parents/children aboard"
      SibSp:
        friendly_name: "Siblings/Spouses"
        description: "Number of siblings/spouses aboard"
      Fare:
        friendly_name: "Ticket Fare"
        description: "Price paid for ticket in British pounds"
    ```
    """
    try:
        import yaml
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
        return config.get('field_descriptions', {})
    except Exception as e:
        logger.warning(f"Could not load field descriptions from {yaml_path}: {e}")
        return {}
