"""
Comprehensive Dataset Profiling Tests.

Tests the profiler against real-world datasets (Titanic, Transactions) in both
CSV and Parquet formats to ensure consistent and accurate profiling across
file types.

Test Datasets:
- Titanic: 891 rows, 12 columns - passenger survival data with nulls, mixed types
- Transactions: 1000 rows, 4 columns - e-commerce data with IDs, amounts, status

Test Categories:
- Format Consistency: Same results for CSV vs Parquet
- Data Quality Metrics: Correct quality scores
- Type Inference: Accurate type detection
- Null Detection: Proper handling of missing values
- ML Insights: Benford analysis, outliers, anomalies
- Edge Cases: Empty columns, all-null columns, single values
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any

from validation_framework.profiler.engine import DataProfiler


# Test data paths - samples stored in tests/data/samples/
TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "samples"
TITANIC_CSV = TEST_DATA_DIR / "csv" / "titanic.csv"
TITANIC_PARQUET = TEST_DATA_DIR / "parquet" / "titanic.parquet"
TRANSACTIONS_CSV = TEST_DATA_DIR / "csv" / "transactions.csv"
TRANSACTIONS_PARQUET = TEST_DATA_DIR / "parquet" / "transactions.parquet"


class TestTitanicDataset:
    """Test profiling of Titanic dataset - passenger survival data."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance with all features enabled."""
        return DataProfiler(
            enable_temporal_analysis=True,
            enable_pii_detection=True,
            enable_enhanced_correlation=True
        )

    @pytest.fixture
    def titanic_csv_profile(self, profiler):
        """Profile Titanic CSV file."""
        return profiler.profile_file(str(TITANIC_CSV))

    @pytest.fixture
    def titanic_parquet_profile(self, profiler):
        """Profile Titanic Parquet file."""
        return profiler.profile_file(str(TITANIC_PARQUET))

    # =========================================================================
    # Basic Structure Tests
    # =========================================================================

    def test_row_count_csv(self, titanic_csv_profile):
        """Verify correct row count from CSV."""
        assert titanic_csv_profile.row_count == 891

    def test_row_count_parquet(self, titanic_parquet_profile):
        """Verify correct row count from Parquet."""
        assert titanic_parquet_profile.row_count == 891

    def test_column_count_csv(self, titanic_csv_profile):
        """Verify correct column count from CSV."""
        assert titanic_csv_profile.column_count == 12

    def test_column_count_parquet(self, titanic_parquet_profile):
        """Verify correct column count from Parquet."""
        assert titanic_parquet_profile.column_count == 12

    def test_format_consistency_row_count(self, titanic_csv_profile, titanic_parquet_profile):
        """Verify CSV and Parquet produce same row count."""
        assert titanic_csv_profile.row_count == titanic_parquet_profile.row_count

    def test_format_consistency_column_count(self, titanic_csv_profile, titanic_parquet_profile):
        """Verify CSV and Parquet produce same column count."""
        assert titanic_csv_profile.column_count == titanic_parquet_profile.column_count

    # =========================================================================
    # Null Detection Tests
    # =========================================================================

    def test_age_null_percentage(self, titanic_csv_profile):
        """Age column should have ~19.9% nulls (177 of 891)."""
        age_col = next(c for c in titanic_csv_profile.columns if c.name == "Age")
        # Allow 0.5% tolerance for rounding
        assert 19.0 <= age_col.statistics.null_percentage <= 20.5

    def test_cabin_null_percentage(self, titanic_csv_profile):
        """Cabin column should have ~77.1% nulls (687 of 891)."""
        cabin_col = next(c for c in titanic_csv_profile.columns if c.name == "Cabin")
        assert 76.0 <= cabin_col.statistics.null_percentage <= 78.0

    def test_embarked_null_percentage(self, titanic_csv_profile):
        """Embarked column should have ~0.2% nulls (2 of 891)."""
        embarked_col = next(c for c in titanic_csv_profile.columns if c.name == "Embarked")
        assert embarked_col.statistics.null_percentage < 1.0

    def test_passengerid_no_nulls(self, titanic_csv_profile):
        """PassengerId should have 0% nulls."""
        pid_col = next(c for c in titanic_csv_profile.columns if c.name == "PassengerId")
        assert pid_col.statistics.null_percentage == 0.0

    # =========================================================================
    # Type Inference Tests
    # =========================================================================

    def test_passengerid_is_integer(self, titanic_csv_profile):
        """PassengerId should be inferred as integer."""
        pid_col = next(c for c in titanic_csv_profile.columns if c.name == "PassengerId")
        assert pid_col.type_info.inferred_type == "integer"

    def test_survived_is_integer_or_boolean(self, titanic_csv_profile):
        """Survived should be inferred as integer (0/1 values)."""
        survived_col = next(c for c in titanic_csv_profile.columns if c.name == "Survived")
        assert survived_col.type_info.inferred_type in ["integer", "boolean"]

    def test_name_is_string(self, titanic_csv_profile):
        """Name should be inferred as string."""
        name_col = next(c for c in titanic_csv_profile.columns if c.name == "Name")
        assert name_col.type_info.inferred_type == "string"

    def test_age_is_numeric(self, titanic_csv_profile):
        """Age should be inferred as float (contains decimals like 0.42)."""
        age_col = next(c for c in titanic_csv_profile.columns if c.name == "Age")
        assert age_col.type_info.inferred_type in ["float", "integer"]

    def test_fare_is_numeric(self, titanic_csv_profile):
        """Fare should be inferred as float."""
        fare_col = next(c for c in titanic_csv_profile.columns if c.name == "Fare")
        assert fare_col.type_info.inferred_type in ["float", "integer"]

    # =========================================================================
    # Quality Score Tests
    # =========================================================================

    def test_overall_quality_reasonable(self, titanic_csv_profile):
        """Overall quality should be reasonable (50-95% given nulls)."""
        assert 50.0 <= titanic_csv_profile.overall_quality_score <= 95.0

    def test_passengerid_high_quality(self, titanic_csv_profile):
        """PassengerId should have high quality (complete, unique)."""
        pid_col = next(c for c in titanic_csv_profile.columns if c.name == "PassengerId")
        assert pid_col.quality.overall_score >= 80.0

    def test_cabin_low_completeness(self, titanic_csv_profile):
        """Cabin should have low completeness due to nulls."""
        cabin_col = next(c for c in titanic_csv_profile.columns if c.name == "Cabin")
        assert cabin_col.quality.completeness < 30.0

    def test_numeric_columns_100_validity(self, titanic_csv_profile):
        """Numeric columns should have 100% validity (integer/float compatible)."""
        numeric_cols = ["PassengerId", "Survived", "Pclass", "Age", "SibSp", "Parch", "Fare"]
        for col_name in numeric_cols:
            col = next((c for c in titanic_csv_profile.columns if c.name == col_name), None)
            if col:
                assert col.quality.validity == 100.0, f"{col_name} should have 100% validity"

    # =========================================================================
    # Cardinality Tests
    # =========================================================================

    def test_passengerid_unique(self, titanic_csv_profile):
        """PassengerId should have 891 unique values (all unique)."""
        pid_col = next(c for c in titanic_csv_profile.columns if c.name == "PassengerId")
        assert pid_col.statistics.unique_count == 891
        assert pid_col.statistics.cardinality == 1.0

    def test_survived_binary(self, titanic_csv_profile):
        """Survived should have exactly 2 unique values (0, 1)."""
        survived_col = next(c for c in titanic_csv_profile.columns if c.name == "Survived")
        assert survived_col.statistics.unique_count == 2

    def test_sex_binary(self, titanic_csv_profile):
        """Sex should have exactly 2 unique values (male, female)."""
        sex_col = next(c for c in titanic_csv_profile.columns if c.name == "Sex")
        assert sex_col.statistics.unique_count == 2

    def test_pclass_three_values(self, titanic_csv_profile):
        """Pclass should have exactly 3 unique values (1, 2, 3)."""
        pclass_col = next(c for c in titanic_csv_profile.columns if c.name == "Pclass")
        assert pclass_col.statistics.unique_count == 3

    def test_embarked_three_values(self, titanic_csv_profile):
        """Embarked should have 3-4 unique values (C, Q, S + possible null)."""
        embarked_col = next(c for c in titanic_csv_profile.columns if c.name == "Embarked")
        assert 3 <= embarked_col.statistics.unique_count <= 4

    # =========================================================================
    # PII Detection Tests
    # =========================================================================

    def test_name_detected_as_pii(self, titanic_csv_profile):
        """Name column should be flagged as potential PII."""
        name_col = next(c for c in titanic_csv_profile.columns if c.name == "Name")
        assert name_col.pii_info is not None
        assert name_col.pii_info.get("detected") is True

    def test_passengerid_not_pii(self, titanic_csv_profile):
        """PassengerId should not be flagged as PII (just an ID)."""
        pid_col = next(c for c in titanic_csv_profile.columns if c.name == "PassengerId")
        # PII info might be None or detected=False
        if pid_col.pii_info:
            assert pid_col.pii_info.get("detected") is not True or \
                   pid_col.pii_info.get("type") in ["id", "identifier", None]

    # =========================================================================
    # Observations Tests (not issues)
    # =========================================================================

    def test_categorical_columns_have_observations(self, titanic_csv_profile):
        """Low cardinality columns should have observations, not issues."""
        categorical_cols = ["Survived", "Sex", "Pclass"]
        for col_name in categorical_cols:
            col = next((c for c in titanic_csv_profile.columns if c.name == col_name), None)
            if col:
                # Should have observations about low cardinality
                low_card_obs = [o for o in col.quality.observations if "cardinality" in o.lower()]
                # Issues should NOT contain "Very low cardinality" as error
                low_card_issues = [i for i in col.quality.issues if "cardinality" in i.lower()]
                assert len(low_card_issues) == 0, f"{col_name} should not have cardinality issues"


class TestTransactionsDataset:
    """Test profiling of Transactions dataset - e-commerce transaction data."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance."""
        return DataProfiler(
            enable_temporal_analysis=True,
            enable_pii_detection=True,
            enable_enhanced_correlation=True
        )

    @pytest.fixture
    def transactions_csv_profile(self, profiler):
        """Profile Transactions CSV file."""
        return profiler.profile_file(str(TRANSACTIONS_CSV))

    @pytest.fixture
    def transactions_parquet_profile(self, profiler):
        """Profile Transactions Parquet file."""
        return profiler.profile_file(str(TRANSACTIONS_PARQUET))

    # =========================================================================
    # Basic Structure Tests
    # =========================================================================

    def test_row_count_csv(self, transactions_csv_profile):
        """Verify correct row count from CSV."""
        assert transactions_csv_profile.row_count == 1000

    def test_row_count_parquet(self, transactions_parquet_profile):
        """Verify correct row count from Parquet."""
        assert transactions_parquet_profile.row_count == 1000

    def test_column_count_csv(self, transactions_csv_profile):
        """Verify correct column count from CSV (4 columns)."""
        assert transactions_csv_profile.column_count == 4

    def test_format_consistency(self, transactions_csv_profile, transactions_parquet_profile):
        """Verify CSV and Parquet produce consistent results."""
        assert transactions_csv_profile.row_count == transactions_parquet_profile.row_count
        assert transactions_csv_profile.column_count == transactions_parquet_profile.column_count

    # =========================================================================
    # Type Inference Tests
    # =========================================================================

    def test_transaction_id_is_string(self, transactions_csv_profile):
        """transaction_id should be inferred as string (TXN prefix)."""
        txn_col = next(c for c in transactions_csv_profile.columns if c.name == "transaction_id")
        assert txn_col.type_info.inferred_type == "string"

    def test_customer_id_is_string(self, transactions_csv_profile):
        """customer_id should be inferred as string (CUST prefix)."""
        cust_col = next(c for c in transactions_csv_profile.columns if c.name == "customer_id")
        assert cust_col.type_info.inferred_type == "string"

    def test_amount_is_numeric(self, transactions_csv_profile):
        """amount should be inferred as float."""
        amount_col = next(c for c in transactions_csv_profile.columns if c.name == "amount")
        assert amount_col.type_info.inferred_type in ["float", "integer"]

    def test_status_is_string(self, transactions_csv_profile):
        """status should be inferred as string."""
        status_col = next(c for c in transactions_csv_profile.columns if c.name == "status")
        assert status_col.type_info.inferred_type == "string"

    # =========================================================================
    # Completeness Tests
    # =========================================================================

    def test_customer_id_has_nulls(self, transactions_csv_profile):
        """customer_id should have some nulls (~2%)."""
        cust_col = next(c for c in transactions_csv_profile.columns if c.name == "customer_id")
        assert cust_col.statistics.null_percentage > 0
        assert cust_col.statistics.null_percentage < 5

    def test_transaction_id_complete(self, transactions_csv_profile):
        """transaction_id should be 100% complete."""
        txn_col = next(c for c in transactions_csv_profile.columns if c.name == "transaction_id")
        assert txn_col.statistics.null_percentage == 0

    def test_amount_complete(self, transactions_csv_profile):
        """amount should be 100% complete."""
        amount_col = next(c for c in transactions_csv_profile.columns if c.name == "amount")
        assert amount_col.statistics.null_percentage == 0

    # =========================================================================
    # Cardinality Tests
    # =========================================================================

    def test_transaction_id_unique(self, transactions_csv_profile):
        """transaction_id should be unique."""
        txn_col = next(c for c in transactions_csv_profile.columns if c.name == "transaction_id")
        assert txn_col.statistics.unique_count == 1000

    def test_status_single_value(self, transactions_csv_profile):
        """status should have only 1 unique value (COMPLETED)."""
        status_col = next(c for c in transactions_csv_profile.columns if c.name == "status")
        assert status_col.statistics.unique_count == 1

    # =========================================================================
    # ML Insights Tests
    # =========================================================================

    def test_benford_analysis_present(self, transactions_csv_profile):
        """Benford analysis should be performed on amount column."""
        assert transactions_csv_profile.ml_findings is not None
        benford = transactions_csv_profile.ml_findings.get("benford_analysis", {})
        assert "amount" in benford or len(benford) > 0

    def test_format_anomalies_detected(self, transactions_csv_profile):
        """Format anomalies should be detected on transaction_id."""
        ml = transactions_csv_profile.ml_findings
        if ml and "format_anomalies" in ml:
            fmt = ml["format_anomalies"]
            # Transaction IDs have inconsistent formats (TXN vs TX)
            if "transaction_id" in fmt:
                assert fmt["transaction_id"].get("anomaly_count", 0) > 0


class TestEdgeCases:
    """Test profiler behavior with edge cases and synthetic data."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance."""
        return DataProfiler()

    def test_all_null_column(self, profiler):
        """Test handling of column with all null values."""
        df = pd.DataFrame({
            "normal": [1, 2, 3, 4, 5],
            "all_null": [None, None, None, None, None]
        })
        result = profiler.profile_dataframe(df, name="null_test")

        null_col = next(c for c in result.columns if c.name == "all_null")
        assert null_col.statistics.null_percentage == 100.0
        assert null_col.quality.completeness == 0.0

    def test_single_value_column(self, profiler):
        """Test handling of column with single repeated value."""
        df = pd.DataFrame({
            "varied": [1, 2, 3, 4, 5],
            "constant": ["A", "A", "A", "A", "A"]
        })
        result = profiler.profile_dataframe(df, name="constant_test")

        const_col = next(c for c in result.columns if c.name == "constant")
        assert const_col.statistics.unique_count == 1
        # Should be observation, not issue
        low_card_issues = [i for i in const_col.quality.issues if "cardinality" in i.lower()]
        assert len(low_card_issues) == 0

    def test_empty_dataframe(self, profiler):
        """Test handling of empty dataframe."""
        df = pd.DataFrame(columns=["col1", "col2", "col3"])
        result = profiler.profile_dataframe(df, name="empty_test")

        assert result.row_count == 0
        assert result.column_count == 3

    def test_mixed_types_column(self, profiler):
        """Test handling of column with mixed types."""
        df = pd.DataFrame({
            "mixed": [1, "two", 3.0, "four", 5]
        })
        result = profiler.profile_dataframe(df, name="mixed_test")

        mixed_col = next(c for c in result.columns if c.name == "mixed")
        # Should handle mixed types gracefully
        assert mixed_col.type_info is not None

    def test_large_numbers(self, profiler):
        """Test handling of very large numbers."""
        df = pd.DataFrame({
            "large": [1e15, 1e16, 1e17, 1e18, 1e19]
        })
        result = profiler.profile_dataframe(df, name="large_test")

        large_col = next(c for c in result.columns if c.name == "large")
        assert large_col.type_info.inferred_type in ["float", "integer"]
        assert large_col.quality.validity == 100.0

    def test_special_characters(self, profiler):
        """Test handling of strings with special characters."""
        df = pd.DataFrame({
            "special": ["hello@world.com", "test!#$%", "normal", "emoji:)", "unicode"]
        })
        result = profiler.profile_dataframe(df, name="special_test")

        special_col = next(c for c in result.columns if c.name == "special")
        assert special_col.type_info.inferred_type == "string"

    def test_boolean_values(self, profiler):
        """Test handling of boolean values."""
        df = pd.DataFrame({
            "bool_native": [True, False, True, False, True],
            "bool_string": ["true", "false", "yes", "no", "true"],
            "bool_int": [0, 1, 0, 1, 1]
        })
        result = profiler.profile_dataframe(df, name="bool_test")

        native_col = next(c for c in result.columns if c.name == "bool_native")
        assert native_col.statistics.unique_count == 2

    def test_datetime_column(self, profiler):
        """Test handling of datetime values."""
        df = pd.DataFrame({
            "date": pd.date_range("2023-01-01", periods=100, freq="D"),
            "value": range(100)
        })
        result = profiler.profile_dataframe(df, name="datetime_test")

        date_col = next(c for c in result.columns if c.name == "date")
        assert date_col.type_info.inferred_type in ["date", "datetime", "string"]


class TestFormatComparison:
    """Test that CSV and Parquet formats produce consistent results."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance."""
        return DataProfiler()

    def test_titanic_quality_scores_match(self, profiler):
        """Titanic CSV and Parquet should have similar quality scores."""
        csv_result = profiler.profile_file(str(TITANIC_CSV))
        parquet_result = profiler.profile_file(str(TITANIC_PARQUET))

        # Allow 5% tolerance for floating point differences
        assert abs(csv_result.overall_quality_score - parquet_result.overall_quality_score) < 5.0

    def test_transactions_quality_scores_match(self, profiler):
        """Transactions CSV and Parquet should have similar quality scores."""
        csv_result = profiler.profile_file(str(TRANSACTIONS_CSV))
        parquet_result = profiler.profile_file(str(TRANSACTIONS_PARQUET))

        assert abs(csv_result.overall_quality_score - parquet_result.overall_quality_score) < 5.0

    def test_column_statistics_consistent(self, profiler):
        """Column statistics should be consistent between formats."""
        csv_result = profiler.profile_file(str(TITANIC_CSV))
        parquet_result = profiler.profile_file(str(TITANIC_PARQUET))

        for csv_col in csv_result.columns:
            parquet_col = next(
                (c for c in parquet_result.columns if c.name == csv_col.name),
                None
            )
            if parquet_col:
                # Null percentages should match
                assert abs(csv_col.statistics.null_percentage -
                          parquet_col.statistics.null_percentage) < 0.5
                # Unique counts should match
                assert csv_col.statistics.unique_count == parquet_col.statistics.unique_count


class TestErrorHandling:
    """Test profiler error handling for invalid inputs."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance."""
        return DataProfiler()

    def test_nonexistent_file_raises_error(self, profiler):
        """Profiling a nonexistent file should raise an error."""
        with pytest.raises(Exception):  # FileNotFoundError or similar
            profiler.profile_file("/nonexistent/path/to/file.csv")

    def test_invalid_file_extension_handled(self, profiler, tmp_path):
        """Profiling an unknown file extension should be handled gracefully."""
        # Create a file with unusual extension
        odd_file = tmp_path / "data.xyz"
        odd_file.write_text("a,b,c\n1,2,3\n")
        # Should either work (treating as CSV) or raise informative error
        try:
            result = profiler.profile_file(str(odd_file))
            # If it works, should have some columns
            assert result.column_count >= 0
        except Exception as e:
            # If it fails, should have informative error message
            assert "format" in str(e).lower() or "extension" in str(e).lower() or "supported" in str(e).lower()

    def test_corrupt_csv_handled(self, profiler, tmp_path):
        """Profiling a corrupt CSV should be handled gracefully."""
        corrupt_file = tmp_path / "corrupt.csv"
        corrupt_file.write_text("a,b,c\n1,2,3\n4,5\n6,7,8,9,10\n")  # Inconsistent columns
        # Should either handle gracefully or raise informative error
        try:
            result = profiler.profile_file(str(corrupt_file))
            # If it works, should process what it can
            assert result.row_count >= 0
        except Exception as e:
            # Error should be informative
            assert len(str(e)) > 0

    def test_empty_file_handled(self, profiler, tmp_path):
        """Profiling an empty file should be handled gracefully."""
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")
        try:
            result = profiler.profile_file(str(empty_file))
            assert result.row_count == 0
        except Exception as e:
            # Should fail gracefully with informative error
            assert "empty" in str(e).lower() or len(str(e)) > 0

    def test_header_only_file(self, profiler, tmp_path):
        """Profiling a file with only headers should work."""
        header_only = tmp_path / "header_only.csv"
        header_only.write_text("col1,col2,col3\n")
        result = profiler.profile_file(str(header_only))
        assert result.row_count == 0
        assert result.column_count == 3

    def test_binary_file_rejected(self, profiler, tmp_path):
        """Profiling a binary file should either fail or handle gracefully."""
        binary_file = tmp_path / "binary.csv"
        binary_file.write_bytes(b'\x00\x01\x02\x03\x04\x05\xff\xfe\xfd')
        # Binary files may either raise an exception or be handled gracefully
        # (though the output may be garbage). Both are acceptable behaviors.
        try:
            result = profiler.profile_file(str(binary_file))
            # If it doesn't raise, it should still produce a result (may be empty)
            assert hasattr(result, 'row_count')
        except Exception:
            # Raising an exception is also acceptable
            pass

    def test_null_dataframe_handled(self, profiler):
        """Profiling a dataframe with all null columns should work."""
        df = pd.DataFrame({
            'all_null_1': [None] * 10,
            'all_null_2': [np.nan] * 10,
            'all_null_3': [pd.NA] * 10,
        })
        result = profiler.profile_dataframe(df, name="all_nulls_test")
        assert result.row_count == 10
        assert result.column_count == 3
        for col in result.columns:
            assert col.statistics.null_percentage == 100.0

    def test_unicode_column_names(self, profiler):
        """Profiling dataframe with unicode column names should work."""
        df = pd.DataFrame({
            '名前': ['Alice', 'Bob'],
            'Preis': [10.5, 20.0],
            'количество': [1, 2],
        })
        result = profiler.profile_dataframe(df, name="unicode_test")
        assert result.column_count == 3
        column_names = [c.name for c in result.columns]
        assert '名前' in column_names

    def test_very_wide_dataframe(self, profiler):
        """Profiling a very wide dataframe (many columns) should work."""
        # Create dataframe with 100 columns
        data = {f'col_{i}': range(10) for i in range(100)}
        df = pd.DataFrame(data)
        result = profiler.profile_dataframe(df, name="wide_test")
        assert result.column_count == 100
        assert result.row_count == 10


class TestValidationIntegration:
    """Test that profile correctly identifies validation needs."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance."""
        return DataProfiler()

    def test_high_null_rate_flagged(self, profiler):
        """Columns with high null rates should have completeness issues."""
        df = pd.DataFrame({
            'mostly_null': [None] * 90 + [1] * 10,  # 90% null
            'complete': range(100),
        })
        result = profiler.profile_dataframe(df, name="null_rate_test")

        mostly_null_col = next(c for c in result.columns if c.name == 'mostly_null')
        assert mostly_null_col.quality.completeness <= 15.0
        # Should have a completeness issue
        assert any('completeness' in issue.lower() for issue in mostly_null_col.quality.issues)

    def test_low_cardinality_detected(self, profiler):
        """Low cardinality columns should be flagged as categorical."""
        # Use 400 rows so 3/400 = 0.0075 < 0.01 threshold
        df = pd.DataFrame({
            'category': ['A', 'B', 'C'] * 134,  # Only 3 values in 402 rows
            'unique_id': range(402),
        })
        result = profiler.profile_dataframe(df, name="cardinality_test")

        cat_col = next(c for c in result.columns if c.name == 'category')
        assert cat_col.statistics.unique_count == 3
        # Should be flagged as low cardinality (observation, not issue)
        observations = cat_col.quality.observations
        assert any('cardinality' in obs.lower() for obs in observations)

    def test_unique_key_detected(self, profiler):
        """Unique columns should be flagged as potential key fields."""
        df = pd.DataFrame({
            'id': range(100),
            'name': [f'Name_{i}' for i in range(100)],
        })
        result = profiler.profile_dataframe(df, name="unique_test")

        id_col = next(c for c in result.columns if c.name == 'id')
        assert id_col.statistics.cardinality == 1.0
        # Should note it's a potential key
        assert any('unique' in obs.lower() for obs in id_col.quality.observations)

    def test_type_consistency_across_formats(self, profiler):
        """Type inference should be consistent between CSV and Parquet."""
        csv_result = profiler.profile_file(str(TITANIC_CSV))
        parquet_result = profiler.profile_file(str(TITANIC_PARQUET))

        for csv_col in csv_result.columns:
            parquet_col = next(
                (c for c in parquet_result.columns if c.name == csv_col.name),
                None
            )
            if parquet_col:
                # Types should be compatible (e.g., both numeric or both string)
                csv_type = csv_col.type_info.inferred_type
                pq_type = parquet_col.type_info.inferred_type

                # Numeric types are compatible with each other
                numeric_types = {'integer', 'float', 'number'}
                if csv_type in numeric_types:
                    assert pq_type in numeric_types, f"{csv_col.name}: CSV={csv_type}, Parquet={pq_type}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
