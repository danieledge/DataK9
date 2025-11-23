"""
DataK9 Profiler Phase 1 Enhancements - Demonstration

This example demonstrates the three new profiler capabilities:
1. Temporal Analysis - Time-series profiling
2. PII Detection - Sensitive data identification
3. Enhanced Correlation - Multi-method correlation analysis

Author: Daniel Edge
Date: 2025-11-22
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Import Phase 1 enhancements
from validation_framework.profiler.temporal_analysis import TemporalAnalyzer
from validation_framework.profiler.pii_detector import PIIDetector
from validation_framework.profiler.enhanced_correlation import EnhancedCorrelationAnalyzer


def demo_temporal_analysis():
    """Demonstrate temporal analysis capabilities."""
    print("=" * 70)
    print("TEMPORAL ANALYSIS DEMO")
    print("=" * 70)

    # Create sample time-series data
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(365)]
    date_series = pd.Series(dates)

    # Initialize analyzer
    analyzer = TemporalAnalyzer(max_lag=40, min_periods=10)

    # Analyze temporal column
    result = analyzer.analyze_temporal_column(
        date_series,
        column_name="transaction_date"
    )

    # Display results
    print(f"\n✓ Analyzed {result['data_points']} data points")
    print(f"\nDate Range:")
    print(f"  Start: {result['date_range']['start']}")
    print(f"  End: {result['date_range']['end']}")
    print(f"  Span: {result['date_range']['span_days']} days")

    print(f"\nFrequency Detection:")
    freq = result['frequency']
    print(f"  Inferred: {freq['inferred']}")
    print(f"  Confidence: {freq['confidence']:.1%}")
    print(f"  Regular: {freq['is_regular']}")

    print(f"\nGap Analysis:")
    gaps = result['gaps']
    print(f"  Gaps detected: {gaps['gaps_detected']}")
    if gaps['gaps_detected']:
        print(f"  Gap count: {gaps['gap_count']}")

    print(f"\nTrend Analysis:")
    trend = result['trend']
    if trend['available']:
        print(f"  Direction: {trend['direction']}")
        print(f"  Strength: {trend['strength']}")
        print(f"  R²: {trend['r_squared']:.3f}")

    print(f"\nTemporal Patterns:")
    patterns = result['patterns']
    biz = patterns['business_vs_weekend']
    print(f"  Business days: {biz['business_day_percentage']:.1f}%")

    # Generate validation suggestions
    suggestions = analyzer.suggest_temporal_validations(result)
    print(f"\n✓ Generated {len(suggestions)} validation suggestions")
    for i, suggestion in enumerate(suggestions[:3], 1):
        print(f"  {i}. {suggestion['validation_type']} ({suggestion['confidence']:.0f}% confidence)")


def demo_pii_detection():
    """Demonstrate PII detection capabilities."""
    print("\n" + "=" * 70)
    print("PII DETECTION DEMO")
    print("=" * 70)

    # Create sample data with PII
    sample_data = {
        "customer_id": ["CUST001", "CUST002", "CUST003"],
        "email": [
            "john.doe@example.com",
            "jane.smith@company.org",
            "admin@test-domain.com"
        ],
        "phone": [
            "(555) 123-4567",
            "555-987-6543",
            "(555) 456-7890"
        ],
        "ssn": [
            "123-45-6789",
            "987-65-4321",
            "555-12-3456"
        ],
        "amount": [100.50, 250.75, 99.99]
    }

    # Initialize detector
    detector = PIIDetector(min_confidence=0.5, sample_size=1000)

    print("\nAnalyzing columns for PII...")

    pii_columns = []

    # Analyze each column
    for column_name, values in sample_data.items():
        result = detector.detect_pii_in_column(
            column_name,
            values,
            total_rows=len(values)
        )

        if result["detected"]:
            pii_columns.append(result)

            print(f"\n⚠ PII DETECTED in column '{column_name}':")
            for pii in result["pii_types"]:
                print(f"  Type: {pii['name']}")
                print(f"  Confidence: {pii['confidence']:.1%}")
                print(f"  Detection: {pii['detection_method']}")

            print(f"  Risk Score: {result['risk_score']}/100")
            print(f"  Regulatory: {', '.join(result['regulatory_frameworks'][:2])}")

            # Show redaction strategy
            strategy = result['redaction_strategy']
            print(f"  Redaction: {strategy['description']}")

    # Dataset-level risk assessment
    print(f"\n" + "=" * 70)
    print("DATASET PRIVACY RISK ASSESSMENT")
    print("=" * 70)

    dataset_risk = detector.calculate_dataset_privacy_risk(
        pii_columns=pii_columns,
        total_columns=len(sample_data),
        total_rows=len(sample_data["customer_id"])
    )

    print(f"\n✓ Privacy Risk Score: {dataset_risk['risk_score']}/100")
    print(f"  Risk Level: {dataset_risk['risk_level'].upper()}")
    print(f"  PII Columns: {dataset_risk['pii_column_count']}/{len(sample_data)}")

    print(f"\nRecommendations:")
    for i, rec in enumerate(dataset_risk['recommendations'][:3], 1):
        print(f"  {i}. {rec}")


def demo_enhanced_correlation():
    """Demonstrate enhanced correlation analysis."""
    print("\n" + "=" * 70)
    print("ENHANCED CORRELATION DEMO")
    print("=" * 70)

    # Create sample data with different correlation patterns
    np.random.seed(42)
    x = np.linspace(0, 10, 100)

    sample_data = {
        "x": x.tolist(),
        "y_linear": (2 * x + 3 + np.random.normal(0, 1, 100)).tolist(),  # Linear
        "y_monotonic": (np.log(x + 1) * 10 + np.random.normal(0, 1, 100)).tolist(),  # Monotonic
        "y_independent": np.random.normal(50, 10, 100).tolist()  # Independent
    }

    # Initialize analyzer
    analyzer = EnhancedCorrelationAnalyzer(
        max_correlation_columns=20,
        min_correlation_threshold=0.5
    )

    print("\nCalculating multi-method correlations...")

    # Calculate correlations with multiple methods
    result = analyzer.calculate_correlations_multi_method(
        sample_data,
        row_count=100,
        methods=['pearson', 'spearman']
    )

    print(f"\n✓ Analyzed {result['columns_analyzed']} columns")
    print(f"  Methods used: {', '.join(result['methods_used'])}")
    print(f"  Data points: {result['data_points']}")

    # Display correlation pairs
    print(f"\n✓ Found {len(result['correlation_pairs'])} significant correlations:")

    for pair in result['correlation_pairs']:
        print(f"\n  {pair['column1']} <-> {pair['column2']}")
        print(f"    Method: {pair['method']}")
        print(f"    Correlation: {pair['correlation']:.3f}")
        print(f"    Strength: {pair['strength']}")
        print(f"    Direction: {pair['direction']}")
        if pair.get('p_value') is not None:
            print(f"    P-value: {pair['p_value']:.4f}")
            print(f"    Significant: {pair['is_significant']}")

    # Show method comparison if available
    if result.get('method_comparison'):
        print(f"\n✓ Method Comparison Available:")
        for comp in result['method_comparison'][:2]:
            print(f"\n  {comp['column1']} <-> {comp['column2']}")
            for method, corr_value in comp['correlations'].items():
                print(f"    {method.capitalize()}: {corr_value:.3f}")
            print(f"    Recommended: {comp['recommended_method']}")
            print(f"    Interpretation: {comp['interpretation']}")


def main():
    """Run all demos."""
    print("\n" + "=" * 70)
    print("DataK9 Profiler Phase 1 Enhancements - Demonstration")
    print("=" * 70)
    print("\nDemonstrating three new profiler capabilities:")
    print("  1. Temporal Analysis")
    print("  2. PII Detection")
    print("  3. Enhanced Correlation")
    print()

    try:
        # Run demos
        demo_temporal_analysis()
        demo_pii_detection()
        demo_enhanced_correlation()

        # Summary
        print("\n" + "=" * 70)
        print("DEMO COMPLETE")
        print("=" * 70)
        print("\n✓ All three Phase 1 enhancements demonstrated successfully!")
        print("\nNext Steps:")
        print("  1. Integrate these modules into the profiler engine")
        print("  2. Update HTML reporter with new sections")
        print("  3. Add CLI flags for enabling/disabling features")
        print("\nFor more information, see:")
        print("  - wip/ENHANCED_PROFILER_DESIGN.md")
        print("  - wip/PHASE1_COMPLETE_SUMMARY.md")
        print("  - wip/FINAL_STATUS_REPORT.md")

    except Exception as e:
        print(f"\n✗ Error during demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
