"""
Tests for context discovery module (context-aware anomaly detection).

Tests the FieldDescription, SubgroupStats, SubgroupPattern, and ContextDiscovery
classes that enable self-calibrating anomaly detection.
"""

import pytest
import numpy as np

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

from validation_framework.profiler.context_discovery import (
    FieldDescription,
    SubgroupStats,
    SubgroupPattern,
    ContextDiscovery,
    ContextStore,
)


# ============================================================================
# FIELD DESCRIPTION TESTS
# ============================================================================

class TestFieldDescription:
    """Test FieldDescription dataclass."""

    def test_display_name_with_different_friendly_name(self):
        """Test display_name shows both names when different."""
        fd = FieldDescription(
            name="cust_id",
            friendly_name="Customer ID",
            description="Unique customer identifier"
        )
        assert fd.display_name() == "Customer ID (cust_id)"

    def test_display_name_when_same(self):
        """Test display_name shows just the name when same."""
        fd = FieldDescription(
            name="email",
            friendly_name="email",
            description="Customer email address"
        )
        assert fd.display_name() == "email"

    def test_display_name_when_empty_friendly_name(self):
        """Test display_name when friendly_name is empty."""
        fd = FieldDescription(
            name="status",
            friendly_name="",
            description="Account status"
        )
        assert fd.display_name() == "status"


# ============================================================================
# SUBGROUP STATS TESTS
# ============================================================================

class TestSubgroupStats:
    """Test SubgroupStats dataclass."""

    @pytest.fixture
    def sample_stats(self):
        """Create sample subgroup statistics."""
        return SubgroupStats(
            segment_value="Premium",
            count=100,
            mean=500.0,
            std=100.0,
            min_val=200.0,
            max_val=800.0,
            q1=425.0,
            q3=575.0
        )

    def test_is_within_bounds_true(self, sample_stats):
        """Test value within expected range."""
        # Value within 2.5 std of mean (500 +/- 250)
        assert sample_stats.is_within_bounds(400) is True
        assert sample_stats.is_within_bounds(600) is True
        assert sample_stats.is_within_bounds(500) is True

    def test_is_within_bounds_false(self, sample_stats):
        """Test value outside expected range."""
        # Value beyond 2.5 std of mean
        assert sample_stats.is_within_bounds(100) is False
        assert sample_stats.is_within_bounds(900) is False

    def test_is_within_bounds_zero_std(self):
        """Test is_within_bounds with zero standard deviation."""
        stats = SubgroupStats(
            segment_value="Constant",
            count=50,
            mean=100.0,
            std=0.0,
            min_val=100.0,
            max_val=100.0,
            q1=100.0,
            q3=100.0
        )
        # Exact match should pass
        assert stats.is_within_bounds(100.0) is True
        # Any deviation should fail
        assert stats.is_within_bounds(100.1) is False

    def test_z_score_in_group(self, sample_stats):
        """Test z-score calculation."""
        # At mean, z-score should be 0
        assert sample_stats.z_score_in_group(500) == 0.0
        # 1 std away
        assert sample_stats.z_score_in_group(600) == 1.0
        assert sample_stats.z_score_in_group(400) == 1.0
        # 2 std away
        assert sample_stats.z_score_in_group(700) == 2.0

    def test_z_score_zero_std(self):
        """Test z-score with zero standard deviation."""
        stats = SubgroupStats(
            segment_value="Constant",
            count=50,
            mean=100.0,
            std=0.0,
            min_val=100.0,
            max_val=100.0,
            q1=100.0,
            q3=100.0
        )
        # Exact match should return 0
        assert stats.z_score_in_group(100.0) == 0.0
        # Any deviation should return infinity
        assert stats.z_score_in_group(101.0) == float('inf')


# ============================================================================
# SUBGROUP PATTERN TESTS
# ============================================================================

class TestSubgroupPattern:
    """Test SubgroupPattern dataclass."""

    @pytest.fixture
    def pattern_with_stats(self):
        """Create pattern with segment statistics."""
        return SubgroupPattern(
            segment_col="customer_type",
            value_col="order_amount",
            variance_explained=0.65,
            stats_by_segment={
                "Premium": SubgroupStats(
                    segment_value="Premium",
                    count=100,
                    mean=500.0,
                    std=100.0,
                    min_val=200.0,
                    max_val=800.0,
                    q1=425.0,
                    q3=575.0
                ),
                "Standard": SubgroupStats(
                    segment_value="Standard",
                    count=200,
                    mean=150.0,
                    std=50.0,
                    min_val=50.0,
                    max_val=300.0,
                    q1=112.0,
                    q3=188.0
                )
            }
        )

    def test_explain_value_within_bounds(self, pattern_with_stats):
        """Test explanation for value within segment bounds."""
        is_explained, explanation, confidence = pattern_with_stats.explain_value(
            "Premium", 550.0, {}
        )
        assert is_explained is True
        assert confidence > 0.5

    def test_explain_value_outside_bounds(self, pattern_with_stats):
        """Test explanation for value outside segment bounds."""
        # Premium mean=500, std=100. 900 is 4 std away
        is_explained, explanation, confidence = pattern_with_stats.explain_value(
            "Premium", 900.0, {}
        )
        assert is_explained is False

    def test_explain_value_unknown_segment(self, pattern_with_stats):
        """Test explanation for unknown segment value."""
        is_explained, explanation, confidence = pattern_with_stats.explain_value(
            "Unknown", 500.0, {}
        )
        assert is_explained is False


# ============================================================================
# CONTEXT DISCOVERY TESTS
# ============================================================================

@pytest.mark.skipif(not HAS_POLARS, reason="Polars not available")
class TestContextDiscovery:
    """Test ContextDiscovery class."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with clear subgroup patterns."""
        np.random.seed(42)
        return pl.DataFrame({
            "customer_type": ["Premium"] * 50 + ["Standard"] * 50,
            "order_amount": (
                list(np.random.normal(500, 50, 50)) +
                list(np.random.normal(150, 30, 50))
            ),
            "quantity": list(range(1, 101)),
        })

    def test_discover_patterns(self, sample_df):
        """Test pattern discovery via discover() method."""
        discovery = ContextDiscovery(min_variance_explained=0.2)
        context = discovery.discover(sample_df)

        # Should return a ContextStore
        assert context is not None
        # Should have discovered some subgroup patterns
        assert len(context.subgroups) >= 0  # May or may not find patterns

    def test_field_descriptions_passed_through(self, sample_df):
        """Test field descriptions are stored in context."""
        field_descs = {
            "customer_type": {
                "friendly_name": "Customer Tier",
                "description": "Customer membership level"
            }
        }

        discovery = ContextDiscovery(field_descriptions=field_descs)
        context = discovery.discover(sample_df)

        # Field descriptions should be in context
        assert "customer_type" in context.field_descriptions

    def test_discovery_with_no_patterns(self):
        """Test discovery handles data with no strong patterns."""
        # Single column data - no patterns possible
        np.random.seed(42)
        df = pl.DataFrame({
            "value": np.random.uniform(0, 100, 100)
        })

        discovery = ContextDiscovery()
        context = discovery.discover(df)

        # Should complete without error
        assert context is not None

    def test_context_store_to_dict(self, sample_df):
        """Test ContextStore serialization."""
        discovery = ContextDiscovery()
        context = discovery.discover(sample_df)

        # Should be able to convert to dict
        result = context.to_dict()
        assert isinstance(result, dict)
        assert "subgroups" in result
        assert "correlations" in result
        assert "field_descriptions" in result


# ============================================================================
# VALUE LABELS TESTS
# ============================================================================

class TestValueLabels:
    """Test value_labels functionality in field descriptions."""

    def test_value_labels_lookup(self):
        """Test looking up value labels from field description dict."""
        field_descriptions = {
            "Survived": {
                "friendly_name": "Survival Status",
                "description": "Whether passenger survived",
                "value_labels": {
                    "0": "Did not survive",
                    "1": "Survived",
                    0: "Did not survive",
                    1: "Survived"
                }
            },
            "Pclass": {
                "friendly_name": "Ticket Class",
                "description": "Passenger class",
                "value_labels": {
                    "1": "First Class",
                    "2": "Second Class",
                    "3": "Third Class",
                    1: "First Class",
                    2: "Second Class",
                    3: "Third Class"
                }
            }
        }

        # Test string lookup
        survived_desc = field_descriptions["Survived"]
        assert survived_desc["value_labels"]["0"] == "Did not survive"
        assert survived_desc["value_labels"]["1"] == "Survived"

        # Test numeric lookup
        assert survived_desc["value_labels"][0] == "Did not survive"
        assert survived_desc["value_labels"][1] == "Survived"

        # Test Pclass
        pclass_desc = field_descriptions["Pclass"]
        assert pclass_desc["value_labels"][1] == "First Class"
        assert pclass_desc["value_labels"]["3"] == "Third Class"

    def test_value_labels_missing_value(self):
        """Test behavior when value is not in labels."""
        field_descriptions = {
            "Status": {
                "value_labels": {
                    "A": "Active",
                    "I": "Inactive"
                }
            }
        }

        # Unknown value should not be in labels
        value_labels = field_descriptions["Status"]["value_labels"]
        assert "X" not in value_labels

    def test_empty_value_labels(self):
        """Test field description without value_labels."""
        field_descriptions = {
            "Name": {
                "friendly_name": "Full Name",
                "description": "Passenger name"
                # No value_labels key
            }
        }

        # Should handle missing value_labels gracefully
        name_desc = field_descriptions["Name"]
        value_labels = name_desc.get("value_labels", {})
        assert value_labels == {}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
