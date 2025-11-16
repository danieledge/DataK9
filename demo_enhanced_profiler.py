#!/usr/bin/env python3
"""
DataK9 Enhanced Profiler Demonstration

Demonstrates all new profiler capabilities:
- Distribution analysis with outlier detection
- Anomaly detection using multiple methods
- Temporal pattern analysis for dates
- Enhanced pattern detection (emails, phones, PII)
- Functional dependency discovery
- Intelligent validation recommendations

Author: Daniel Edge
Date: November 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from validation_framework.profiler import DataProfiler
from validation_framework.core.pretty_output import PrettyOutput as po


def create_sample_data(filename="/tmp/enhanced_profiler_demo.csv"):
    """
    Create comprehensive sample dataset showcasing all profiler features.

    Returns:
        str: Path to created CSV file
    """
    np.random.seed(42)
    n_rows = 500

    # === Customer ID (unique identifier) ===
    customer_ids = range(1000, 1000 + n_rows)

    # === Email addresses (semantic type detection, PII) ===
    emails = [f"customer{i}@{'example' if i % 3 else 'test'}.com" for i in range(n_rows)]
    # Add some anomalies
    emails[10] = "invalid-email"
    emails[25] = "no-at-sign.com"

    # === Phone numbers (pattern detection, PII) ===
    phones = [f"({np.random.randint(200, 999)}) {np.random.randint(200, 999)}-{np.random.randint(1000, 9999)}"
              for _ in range(n_rows)]
    # Add some anomalies
    phones[15] = "555-CALL-NOW"
    phones[30] = "123"

    # === Age (distribution analysis, outliers) ===
    ages = np.random.normal(35, 12, n_rows).astype(int)
    # Add outliers
    ages[5] = 150  # Obvious error
    ages[20] = -5  # Invalid age
    ages[50] = 95  # Extreme but valid

    # === Transaction amount (distribution, outliers) ===
    amounts = np.random.lognormal(mean=6.0, sigma=1.0, size=n_rows)
    # Add outliers
    amounts[100] = 50000  # Very large transaction
    amounts[200] = 0.01   # Very small transaction

    # === Account status (low cardinality, valid values) ===
    statuses = np.random.choice(['active', 'inactive', 'pending', 'suspended'], n_rows, p=[0.6, 0.2, 0.15, 0.05])

    # === Account code (fixed-length pattern) ===
    account_codes = [f"ACC{i:06d}" for i in range(n_rows)]
    # Add pattern violations
    account_codes[8] = "INVALID"
    account_codes[45] = "AC12345"  # Wrong length

    # === ZIP codes (semantic type, pattern) ===
    zip_codes = [f"{np.random.randint(10000, 99999)}" for _ in range(n_rows)]
    # Add some extended format
    for i in range(0, n_rows, 10):
        zip_codes[i] = f"{zip_codes[i]}-{np.random.randint(1000, 9999)}"

    # === Signup date (temporal analysis, patterns, freshness) ===
    start_date = datetime(2024, 1, 1)
    signup_dates = []
    current = start_date
    for i in range(n_rows):
        # Daily pattern with occasional gaps
        if i % 30 == 0:  # Gap every 30 days
            current += timedelta(days=np.random.randint(5, 15))
        else:
            current += timedelta(days=1)
        signup_dates.append(current.strftime("%Y-%m-%d"))

    # Add some future dates (data quality issue)
    signup_dates[450] = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
    signup_dates[480] = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")

    # === Last login (freshness check) ===
    last_logins = [(datetime.now() - timedelta(days=np.random.randint(0, 90))).strftime("%Y-%m-%d")
                   for _ in range(n_rows)]

    # === Country (functional dependency - status determines country) ===
    # Create dependency: active users are mostly US, inactive mostly CA
    countries = []
    for status in statuses:
        if status == 'active':
            countries.append(np.random.choice(['US', 'CA', 'UK'], p=[0.8, 0.1, 0.1]))
        elif status == 'inactive':
            countries.append(np.random.choice(['US', 'CA', 'UK'], p=[0.2, 0.6, 0.2]))
        else:
            countries.append(np.random.choice(['US', 'CA', 'UK']))

    # Create DataFrame
    df = pd.DataFrame({
        'customer_id': customer_ids,
        'email': emails,
        'phone': phones,
        'age': ages,
        'transaction_amount': np.round(amounts, 2),
        'status': statuses,
        'account_code': account_codes,
        'zip_code': zip_codes,
        'signup_date': signup_dates,
        'last_login': last_logins,
        'country': countries
    })

    # Save to CSV
    df.to_csv(filename, index=False)
    po.success(f"Created sample data: {filename} ({len(df)} rows, {len(df.columns)} columns)")

    return filename


def demonstrate_profiler():
    """Run comprehensive profiler demonstration."""

    po.logo()
    po.header("DataK9 Enhanced Profiler Demonstration")

    # Create sample data
    po.section("Creating Sample Dataset")
    data_file = create_sample_data()
    print()

    # Run profiler
    po.section("Running Enhanced Profiler")
    profiler = DataProfiler(chunk_size=50000)

    po.info("Analyzing data with all enhanced features...")
    result = profiler.profile_file(data_file, file_format='csv')

    po.success(f"Profile completed in {result.processing_time_seconds:.2f} seconds")
    print()

    # Display summary
    po.section("Profile Summary")
    items = [
        ("File", result.file_name, po.INFO),
        ("Rows", f"{result.row_count:,}", po.INFO),
        ("Columns", str(result.column_count), po.INFO),
        ("File Size", f"{result.file_size_bytes / 1024:.1f} KB", po.INFO),
        ("Overall Quality Score", f"{result.overall_quality_score:.1f}/100", po.SUCCESS if result.overall_quality_score >= 80 else po.WARNING),
        ("Validation Suggestions", str(len(result.suggested_validations)), po.INFO)
    ]
    po.summary_box("Data Profile", items)
    print()

    # Detailed column analysis
    po.section("Column Analysis with Enhanced Features")

    for col in result.columns:
        po.subsection(f"Column: {col.name}")

        # Basic info
        po.key_value("Type", col.type_info.inferred_type)
        po.key_value("Quality Score", f"{col.quality.overall_score:.1f}/100")
        po.key_value("Completeness", f"{col.quality.completeness:.1f}%")

        # Distribution analysis (numeric)
        if col.distribution:
            po.info("📊 Distribution Analysis:")
            print(f"    Type: {col.distribution.distribution_type}")
            print(f"    Skewness: {col.distribution.skewness:.2f}")
            print(f"    Kurtosis: {col.distribution.kurtosis:.2f}")
            if col.distribution.outlier_count > 0:
                po.warning(f"    Outliers: {col.distribution.outlier_count} ({col.distribution.outlier_percentage:.1f}%)")
                print(f"    P1: {col.distribution.percentile_1:.2f}, P99: {col.distribution.percentile_99:.2f}")

        # Anomaly detection
        if col.anomalies and col.anomalies.has_anomalies:
            po.warning(f"⚠️  Anomalies Detected: {col.anomalies.anomaly_count} ({col.anomalies.anomaly_percentage:.1f}%)")
            print(f"    Methods: {', '.join(col.anomalies.anomaly_methods)}")
            if col.anomalies.anomaly_samples:
                print(f"    Samples: {', '.join(map(str, col.anomalies.anomaly_samples[:3]))}")

        # Temporal analysis
        if col.temporal:
            po.info("📅 Temporal Analysis:")
            print(f"    Range: {col.temporal.earliest_date} to {col.temporal.latest_date}")
            print(f"    Pattern: {col.temporal.temporal_pattern}")
            if col.temporal.has_gaps:
                po.warning(f"    Gaps: {col.temporal.gap_count} gaps detected (largest: {col.temporal.largest_gap_days} days)")
            if col.temporal.has_future_dates:
                po.error(f"    Future dates: {col.temporal.future_date_count} dates in the future!")
            if col.temporal.is_fresh is False:
                po.warning(f"    Freshness: Latest date is {col.temporal.days_since_latest} days old")

        # Pattern detection
        if col.patterns and col.patterns.semantic_type:
            po.info(f"🔍 Pattern: {col.patterns.semantic_type} ({col.patterns.semantic_confidence:.0f}% confidence)")
            if col.patterns.pii_detected:
                po.warning(f"    ⚠️  PII Detected: {', '.join(col.patterns.pii_types)}")
            if col.patterns.regex_pattern:
                print(f"    Regex: {col.patterns.regex_pattern}")

        # Functional dependencies
        if col.dependencies:
            if col.dependencies.depends_on:
                po.info(f"🔗 Depends on: {', '.join(col.dependencies.depends_on)}")
            if col.dependencies.determines:
                po.info(f"🔗 Determines: {', '.join(col.dependencies.determines)}")

        print()

    # Top validation suggestions
    po.section("Top Validation Suggestions (sorted by confidence)")

    for i, sugg in enumerate(result.suggested_validations[:15], 1):
        confidence_color = (
            'success' if sugg.confidence >= 90 else
            'info' if sugg.confidence >= 80 else
            'warning'
        )

        print(f"\n{i}. {sugg.validation_type} [{sugg.severity}]")

        if confidence_color == 'success':
            po.success(f"   Confidence: {sugg.confidence:.0f}%")
        elif confidence_color == 'info':
            po.info(f"   Confidence: {sugg.confidence:.0f}%")
        else:
            po.warning(f"   Confidence: {sugg.confidence:.0f}%")

        print(f"   Reason: {sugg.reason}")

        # Show params
        if sugg.params:
            print(f"   Params:")
            for key, value in list(sugg.params.items())[:3]:  # Limit to 3 params
                if isinstance(value, list):
                    print(f"     {key}: {value[:3]}{'...' if len(value) > 3 else ''}")
                else:
                    print(f"     {key}: {value}")

    print()

    # Save results
    po.section("Saving Results")

    # Save JSON profile
    import json
    import numpy as np

    # Custom JSON encoder for numpy types
    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            if isinstance(obj, (np.floating, np.float64, np.float32)):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, np.bool_):
                return bool(obj)
            return super().default(obj)

    json_file = "/tmp/enhanced_profile_results.json"
    with open(json_file, 'w') as f:
        json.dump(result.to_dict(), f, indent=2, cls=NumpyEncoder)
    po.success(f"JSON profile saved: {json_file}")

    # Save auto-generated validation config
    yaml_file = "/tmp/auto_generated_validation.yaml"
    with open(yaml_file, 'w') as f:
        f.write(result.generated_config_yaml)
    po.success(f"Auto-generated validation config: {yaml_file}")

    print()

    # Summary
    po.section("Summary")
    print()
    po.success("✅ Enhanced profiler demonstration completed successfully!")
    print()
    po.info("Enhanced Features Demonstrated:")
    print("  • Distribution analysis with outlier detection (IQR, Z-score)")
    print("  • Multi-method anomaly detection")
    print("  • Temporal pattern analysis (gaps, freshness, future dates)")
    print("  • Enhanced pattern detection (emails, phones, PII)")
    print("  • Functional dependency discovery")
    print("  • Intelligent validation recommendations")
    print()
    po.info("Next Steps:")
    print(f"  1. Review JSON profile: {json_file}")
    print(f"  2. Review auto-generated config: {yaml_file}")
    print("  3. Import JSON into DataK9 Studio for visual review")
    print("  4. Run validation: python3 -m validation_framework.cli validate " + yaml_file)
    print()
    po.divider()


if __name__ == "__main__":
    try:
        demonstrate_profiler()
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
        sys.exit(0)
    except Exception as e:
        po.error(f"Demo failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
