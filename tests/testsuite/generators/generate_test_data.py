"""
DataK9 Test Suite - Test Data Generator

Generates test datasets for profiler enhancement testing.
All output files are prefixed with 'testsuite_' for easy identification.

Usage:
    python -m tests.testsuite.generators.generate_test_data

    # Or from command line:
    python tests/testsuite/generators/generate_test_data.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random


def generate_temporal_test_data(output_path: Path, num_rows: int = 500):
    """
    Generate test data with temporal patterns.

    Creates:
    - Regular daily timestamps with some gaps
    - Weekly seasonality
    - Linear trend
    """
    print(f"Generating temporal test data ({num_rows} rows)...")

    random.seed(42)
    np.random.seed(42)

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
    print(f"  Created: {output_path.name}")
    return output_path


def generate_pii_test_data(output_path: Path, num_rows: int = 300):
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

    random.seed(42)
    np.random.seed(42)

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
        digits = [random.randint(0, 9) for _ in range(15)]
        checksum = 0
        for i, digit in enumerate(digits):
            if i % 2 == 0:
                doubled = digit * 2
                checksum += doubled - 9 if doubled > 9 else doubled
            else:
                checksum += digit
        check_digit = (10 - (checksum % 10)) % 10
        digits.append(check_digit)
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
    print(f"  Created: {output_path.name}")
    return output_path


def generate_correlation_test_data(output_path: Path, num_rows: int = 500):
    """
    Generate test data with various correlation patterns.

    Creates:
    - Strong linear correlation (Pearson)
    - Strong monotonic correlation (Spearman)
    - Non-linear correlation (for mutual info)
    - Independent variable (no correlation)
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
    print(f"  Created: {output_path.name}")
    return output_path


def generate_cross_file_test_data(output_dir: Path):
    """
    Generate cross-file validation test data.

    Creates:
    - testsuite_customers.csv: Customer master data
    - testsuite_orders.csv: Order data with FK to customers
    """
    print("Generating cross-file validation test data...")

    # Customers
    customers_df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice Smith', 'Bob Johnson', 'Carol White', 'David Brown', 'Eve Davis'],
        'email': ['alice@example.com', 'bob@example.com', 'carol@example.com',
                  'david@example.com', 'eve@example.com'],
        'tier': ['Premium', 'Standard', 'Premium', 'Standard', 'Premium']
    })
    customers_path = output_dir / 'testsuite_customers.csv'
    customers_df.to_csv(customers_path, index=False)
    print(f"  Created: {customers_path.name}")

    # Orders (includes 2 invalid customer_ids: 99 and 88)
    orders_df = pd.DataFrame({
        'order_id': [1, 2, 3, 4, 5, 6, 7],
        'customer_id': [1, 2, 3, 99, 1, 88, 2],
        'amount': [150.00, 250.00, 350.00, 100.00, 200.00, 175.00, 425.00],
        'order_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18',
                       '2024-01-19', '2024-01-20', '2024-01-21']
    })
    orders_path = output_dir / 'testsuite_orders.csv'
    orders_df.to_csv(orders_path, index=False)
    print(f"  Created: {orders_path.name}")


def generate_regression_test_data(output_dir: Path):
    """
    Generate regression test data with designed quality issues.

    Creates comprehensive test data for validation framework testing.
    """
    print("Generating regression test data...")

    np.random.seed(42)
    random.seed(42)

    num_rows = 25

    # Generate data with designed quality issues
    df = pd.DataFrame({
        'employee_id': [f'EMP{i:04d}' for i in range(1, num_rows + 1)],
        'first_name': ['Alice', 'Bob', 'Carol', 'David', 'Eve', 'Frank', 'Grace', 'Henry',
                       'Iris', 'Jack', 'Kate', 'Leo', 'Mia', 'Noah', 'Olivia', 'Paul',
                       'Quinn', 'Rose', 'Sam', 'Tina', 'Uma', 'Victor', 'Wendy', 'Xavier', 'Zoe'],
        'last_name': ['Smith', 'Johnson', 'White', 'Brown', 'Davis', 'Miller', 'Wilson',
                      'Taylor', 'Lee', 'Harris', 'Clark', 'Lewis', 'King', 'Hall', 'Allen',
                      'Young', 'Wright', 'Lopez', 'Scott', 'Green', 'Adams', 'Baker',
                      'Rivera', 'Carter', 'Mitchell'],
        'email': [
            'john.doe@company.com',  # Duplicate with row 21
            None,  # Missing
            'carol.white@company.com',
            'david.brown@company.com',
            None,  # Missing
            'frank.miller@company.com',
            'grace.wilson@company.com',
            'henry.taylor@company.com',
            'iris.lee@company.com',
            'jack.harris@company.com',
            'kate.clark@company.com',
            'leo.lewis@company.com',
            'mia.king@company.com',
            'noah.hall@company.com',
            'olivia.allen@company.com',
            'paul.young@company.com',
            'quinn.wright@company.com',
            'rose.lopez@company.com',
            'sam.scott@company.com',
            'tina.green@company.com',
            'john.doe@company.com',  # Duplicate with row 1
            'victor.baker@company.com',
            'wendy.rivera@company.com',
            'xavier.carter@company.com',
            'zoe.mitchell@company.com'
        ],
        'phone': [
            '(555) 123-4567',
            '(555) 234-5678',
            None,  # Missing
            '(555) 345-6789',
            '(555) 456-7890',
            None,  # Missing
            '(555) 567-8901',
            '(555) 678-9012',
            '(555) 789-0123',
            '(555) 890-1234',
            '(555) 901-2345',
            '(555) 012-3456',
            None,  # Missing
            '(555) 123-4567',
            '(555) 234-5678',
            '(555) 345-6789',
            '(555) 456-7890',
            '(555) 567-8901',
            '(555) 678-9012',
            '(555) 789-0123',
            None,  # Missing
            '(555) 890-1234',
            '(555) 901-2345',
            '(555) 012-3456',
            '(555) 123-4567'
        ],
        'dept_id': [1, 2, 1, 3, 2, 1, 3, 2, 1, 3, 2, 1, 3, 2, 1, 3, 2, 1, 3, 2, 1, 3, 2, 1, 3],
        'salary': [
            50000, 55000, 60000, 45000, 52000, 58000, 62000, 48000, 54000, 250000,  # Outlier
            51000, 56000, 61000, 46000, 53000, 59000, 63000, 49000, 55000, 57000,
            52000, 58000, 64000, 47000, 500000  # Outlier
        ],
        'hire_date': pd.date_range('2020-01-01', periods=num_rows, freq='M'),
        'status': ['active'] * 20 + ['inactive'] * 5,
        'performance_score': np.random.randint(1, 6, num_rows),
        'years_experience': np.random.randint(0, 20, num_rows),
        'manager_id': [None] + [f'EMP{random.randint(1, i):04d}' for i in range(1, num_rows)],
        'last_review_date': pd.date_range('2023-01-01', periods=num_rows, freq='W')
    })

    output_path = output_dir / 'testsuite_regression_data.csv'
    df.to_csv(output_path, index=False)
    print(f"  Created: {output_path.name}")


def main():
    """Generate all test datasets for the testsuite."""
    # Get testsuite data directory
    testsuite_dir = Path(__file__).parent.parent / 'data'

    print("=" * 70)
    print("DataK9 Test Suite - Test Data Generator")
    print("=" * 70)
    print(f"Output directory: {testsuite_dir}")
    print()

    # Ensure directories exist
    (testsuite_dir / 'profiler').mkdir(parents=True, exist_ok=True)
    (testsuite_dir / 'cross_file').mkdir(parents=True, exist_ok=True)
    (testsuite_dir / 'regression').mkdir(parents=True, exist_ok=True)

    # Generate profiler test data
    print("\n[Profiler Test Data]")
    generate_temporal_test_data(
        testsuite_dir / 'profiler' / 'testsuite_temporal_patterns.csv',
        num_rows=500
    )
    generate_pii_test_data(
        testsuite_dir / 'profiler' / 'testsuite_pii_samples.csv',
        num_rows=300
    )
    generate_correlation_test_data(
        testsuite_dir / 'profiler' / 'testsuite_correlation_patterns.csv',
        num_rows=500
    )

    # Generate cross-file validation data
    print("\n[Cross-File Validation Data]")
    generate_cross_file_test_data(testsuite_dir / 'cross_file')

    # Generate regression test data
    print("\n[Regression Test Data]")
    generate_regression_test_data(testsuite_dir / 'regression')

    print("\n" + "=" * 70)
    print("Test data generation complete!")
    print("=" * 70)

    # List generated files
    print("\nGenerated files:")
    for subdir in ['profiler', 'cross_file', 'regression']:
        subpath = testsuite_dir / subdir
        if subpath.exists():
            for f in sorted(subpath.glob('*.csv')):
                print(f"  {subdir}/{f.name}")


if __name__ == '__main__':
    main()
