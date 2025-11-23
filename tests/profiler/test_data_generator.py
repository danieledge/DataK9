"""
Generate test data for profiler enhancement testing.

Creates test datasets with temporal patterns, PII, and correlation structures
from existing ecommerce parquet data.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random


def generate_temporal_test_data(output_path: str, num_rows: int = 1000):
    """
    Generate test data with temporal patterns.

    Creates:
    - Regular daily timestamps with some gaps
    - Weekly seasonality
    - Linear trend
    """
    print(f"Generating temporal test data ({num_rows} rows)...")

    # Start date
    start_date = datetime(2023, 1, 1)

    # Generate daily timestamps with gaps
    dates = []
    current_date = start_date

    while len(dates) < num_rows:
        dates.append(current_date)

        # Most days increment by 1, but add gaps (5% probability)
        if random.random() < 0.05:
            gap_days = random.randint(2, 7)
            current_date += timedelta(days=gap_days)
        else:
            current_date += timedelta(days=1)

    # Generate values with trend and seasonality
    trend = np.linspace(100, 200, num_rows)
    seasonality = 20 * np.sin(2 * np.pi * np.arange(num_rows) / 7)  # Weekly pattern
    noise = np.random.normal(0, 5, num_rows)
    values = trend + seasonality + noise

    df = pd.DataFrame({
        'date': dates[:num_rows],
        'value': values,
        'day_of_week': [d.strftime('%A') for d in dates[:num_rows]],
        'is_business_day': [(d.weekday() < 5) for d in dates[:num_rows]]
    })

    df.to_csv(output_path, index=False)
    print(f"✓ Created: {output_path}")
    return output_path


def generate_pii_test_data(output_path: str, num_rows: int = 500):
    """
    Generate test data with various PII types.

    Includes:
    - Email addresses
    - Phone numbers (US format)
    - SSNs
    - Credit card numbers (valid Luhn)
    - IP addresses
    - Postal codes
    """
    print(f"Generating PII test data ({num_rows} rows)...")

    # Generate fake PII data
    emails = [f"user{i}@example.com" for i in range(num_rows)]

    # US phone numbers
    phones = [
        f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"
        for i in range(num_rows)
    ]

    # SSNs (fake format)
    ssns = [
        f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
        for i in range(num_rows)
    ]

    # Credit card numbers (fake but valid Luhn)
    def generate_luhn_valid_cc():
        # Start with 15 random digits
        digits = [random.randint(0, 9) for _ in range(15)]

        # Calculate Luhn check digit
        checksum = 0
        for i, digit in enumerate(digits):
            if i % 2 == 0:
                doubled = digit * 2
                checksum += doubled - 9 if doubled > 9 else doubled
            else:
                checksum += digit

        check_digit = (10 - (checksum % 10)) % 10
        digits.append(check_digit)

        # Format as XXXX-XXXX-XXXX-XXXX
        cc_str = ''.join(map(str, digits))
        return f"{cc_str[0:4]}-{cc_str[4:8]}-{cc_str[8:12]}-{cc_str[12:16]}"

    credit_cards = [generate_luhn_valid_cc() for _ in range(num_rows)]

    # IP addresses
    ips = [
        f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"
        for i in range(num_rows)
    ]

    # US Postal codes
    postal_codes = [f"{random.randint(10000, 99999)}" for i in range(num_rows)]

    # Regular data (non-PII)
    customer_ids = [f"CUST{i:06d}" for i in range(num_rows)]
    amounts = np.random.uniform(10, 1000, num_rows).round(2)

    df = pd.DataFrame({
        'customer_id': customer_ids,
        'email': emails,
        'phone': phones,
        'ssn': ssns,
        'credit_card': credit_cards,
        'ip_address': ips,
        'postal_code': postal_codes,
        'amount': amounts,
        'transaction_date': pd.date_range(start='2023-01-01', periods=num_rows, freq='H')
    })

    df.to_csv(output_path, index=False)
    print(f"✓ Created: {output_path}")
    return output_path


def generate_correlation_test_data(output_path: str, num_rows: int = 1000):
    """
    Generate test data with various correlation patterns.

    Creates:
    - Strong linear correlation (Pearson)
    - Strong monotonic correlation (Spearman)
    - Ordinal correlation (Kendall)
    - Non-linear correlation (for mutual info)
    """
    print(f"Generating correlation test data ({num_rows} rows)...")

    np.random.seed(42)

    # Base variable
    x = np.linspace(0, 10, num_rows)

    # Linear correlation (high Pearson)
    y_linear = 2 * x + 3 + np.random.normal(0, 1, num_rows)

    # Monotonic but non-linear (high Spearman, lower Pearson)
    y_monotonic = np.log(x + 1) * 10 + np.random.normal(0, 1, num_rows)

    # Non-linear U-shaped (low Pearson/Spearman, high mutual info)
    y_nonlinear = (x - 5) ** 2 + np.random.normal(0, 1, num_rows)

    # Independent variable (no correlation)
    y_independent = np.random.normal(50, 10, num_rows)

    # Ordinal-like data
    y_ordinal = np.digitize(x, bins=[0, 2.5, 5, 7.5, 10]) + np.random.normal(0, 0.2, num_rows)

    df = pd.DataFrame({
        'x_base': x,
        'y_linear': y_linear,
        'y_monotonic': y_monotonic,
        'y_nonlinear': y_nonlinear,
        'y_independent': y_independent,
        'y_ordinal': y_ordinal,
        'category': np.random.choice(['A', 'B', 'C'], num_rows)
    })

    df.to_csv(output_path, index=False)
    print(f"✓ Created: {output_path}")
    return output_path


def generate_from_ecommerce_parquet(
    parquet_path: str,
    output_dir: str,
    num_rows: int = 1000
):
    """
    Generate enhanced test data from existing ecommerce parquet file.

    Args:
        parquet_path: Path to ecommerce_transactions.parquet
        output_dir: Output directory for test data
        num_rows: Number of rows to sample
    """
    print(f"\nGenerating test data from: {parquet_path}")

    # Read parquet file
    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df)} rows from parquet file")

    # Sample data
    if len(df) > num_rows:
        df_sample = df.sample(n=num_rows, random_state=42).reset_index(drop=True)
    else:
        df_sample = df.copy()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Enhanced temporal data
    if 'timestamp' in df_sample.columns or 'date' in df_sample.columns:
        temporal_col = 'timestamp' if 'timestamp' in df_sample.columns else 'date'
        temporal_df = df_sample[[temporal_col]].copy()

        # Add temporal features
        temporal_df['timestamp'] = pd.to_datetime(temporal_df[temporal_col])
        temporal_df['value'] = np.random.uniform(100, 500, len(temporal_df))
        temporal_df = temporal_df.sort_values('timestamp').reset_index(drop=True)

        temporal_output = output_dir / 'temporal_test_data.csv'
        temporal_df.to_csv(temporal_output, index=False)
        print(f"✓ Created temporal test data: {temporal_output}")

    # 2. Enhanced PII data
    pii_df = df_sample.copy()

    # Add synthetic PII columns
    pii_df['email'] = [f"customer{i}@example.com" for i in range(len(pii_df))]
    pii_df['phone'] = [
        f"({random.randint(200, 999)}) {random.randint(200, 999)}-{random.randint(1000, 9999)}"
        for _ in range(len(pii_df))
    ]

    pii_output = output_dir / 'pii_test_data.csv'
    pii_df.to_csv(pii_output, index=False)
    print(f"✓ Created PII test data: {pii_output}")

    # 3. Enhanced correlation data
    # Select numeric columns
    numeric_cols = df_sample.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) >= 2:
        corr_df = df_sample[list(numeric_cols[:5])].copy()

        # Add correlated columns
        if len(corr_df.columns) > 0:
            base_col = corr_df.columns[0]
            corr_df['linear_corr'] = corr_df[base_col] * 1.5 + np.random.normal(0, 10, len(corr_df))
            corr_df['monotonic_corr'] = np.log(corr_df[base_col].abs() + 1) * 50

        corr_output = output_dir / 'correlation_test_data.csv'
        corr_df.to_csv(corr_output, index=False)
        print(f"✓ Created correlation test data: {corr_output}")

    print(f"\n✓ Test data generation complete!")
    print(f"  Output directory: {output_dir}")


def main():
    """Generate all test datasets."""
    # Setup paths
    project_root = Path(__file__).parent.parent.parent
    test_data_dir = project_root / 'tests' / 'profiler' / 'test_data'
    test_data_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("DataK9 Profiler Enhancement - Test Data Generator")
    print("=" * 70)

    # Generate synthetic test data
    generate_temporal_test_data(test_data_dir / 'temporal_patterns.csv', num_rows=500)
    generate_pii_test_data(test_data_dir / 'pii_samples.csv', num_rows=300)
    generate_correlation_test_data(test_data_dir / 'correlation_patterns.csv', num_rows=500)

    # Generate from ecommerce parquet if available
    ecommerce_parquet = project_root.parent / 'test-data' / 'ecommerce_transactions.parquet'
    if ecommerce_parquet.exists():
        generate_from_ecommerce_parquet(
            str(ecommerce_parquet),
            str(test_data_dir),
            num_rows=1000
        )
    else:
        print(f"\n⚠ Ecommerce parquet not found: {ecommerce_parquet}")
        print("  Skipping parquet-based test data generation")

    print("\n" + "=" * 70)
    print("Test data generation complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
