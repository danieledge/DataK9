#!/usr/bin/env python3
"""
Generate comprehensive test data for validation testing.

This script creates test datasets that exercise all 36 validation types
with both passing and failing scenarios.

Usage:
    python3 generate_validation_test_data.py

Output:
    tests/testsuite/data/validation/testsuite_validation_comprehensive.csv
    tests/testsuite/data/validation/testsuite_validation_comprehensive.parquet
    tests/testsuite/data/validation/testsuite_validation_customers.csv
    tests/testsuite/data/validation/testsuite_validation_orders.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import random
import string

# Ensure reproducibility
np.random.seed(42)
random.seed(42)

# Output directory
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "data" / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_comprehensive_dataset(n_rows=1000):
    """
    Generate main comprehensive test dataset.

    Dataset structure:
    - 80% clean data (rows 1-800)
    - 20% data with intentional quality issues (rows 801-1000)
    """
    print(f"Generating comprehensive dataset with {n_rows} rows...")

    # Generate base clean data
    data = {
        'record_id': list(range(1, n_rows + 1)),
        'customer_ref': [f'CUST-{i:05d}' for i in range(1, n_rows + 1)],
        'email': [f'user{i}@example.com' for i in range(1, n_rows + 1)],
        'phone': [f'+1{random.randint(2000000000, 9999999999)}' for _ in range(n_rows)],
        'amount': [round(random.uniform(10, 5000), 2) for _ in range(n_rows)],
        'quantity': [random.randint(1, 100) for _ in range(n_rows)],
        'status': [random.choice(['active', 'pending', 'completed', 'cancelled']) for _ in range(n_rows)],
        'category': [random.choice(['electronics', 'clothing', 'food', 'services']) for _ in range(n_rows)],
        'created_date': [(datetime.now() - timedelta(days=random.randint(1, 300))).strftime('%Y-%m-%d') for _ in range(n_rows)],
        'updated_date': [(datetime.now() - timedelta(days=random.randint(0, 100))).strftime('%Y-%m-%d') for _ in range(n_rows)],
        'description': [''.join(random.choices(string.ascii_letters + ' ', k=random.randint(10, 200))) for _ in range(n_rows)],
        'score': [round(random.gauss(75, 15), 2) for _ in range(n_rows)],
        'flag': [random.choice([True, False]) for _ in range(n_rows)],
        'region': [random.choice(['NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL']) for _ in range(n_rows)],
        'notes': [f'Note for record {i}' if random.random() > 0.3 else '' for i in range(1, n_rows + 1)],
    }

    df = pd.DataFrame(data)

    # Ensure updated_date >= created_date for clean rows
    for i in range(800):
        created = datetime.strptime(df.loc[i, 'created_date'], '%Y-%m-%d')
        updated = created + timedelta(days=random.randint(0, 30))
        df.loc[i, 'updated_date'] = updated.strftime('%Y-%m-%d')

    # =========================================================================
    # Introduce intentional data quality issues (rows 801-1000)
    # Each issue type gets 20 rows
    # =========================================================================

    print("  Injecting data quality issues...")

    # 801-820: Missing mandatory fields
    df.loc[800:819, 'email'] = None
    df.loc[800:809, 'amount'] = None

    # 821-840: Invalid email formats
    invalid_emails = ['invalid-email', 'no-at-sign.com', '@nodomain', 'spaces in@email.com']
    df.loc[820:839, 'email'] = [invalid_emails[i % len(invalid_emails)] for i in range(20)]

    # 841-860: Out of range amounts
    out_of_range = [-100, -50, 150000, 200000]
    df.loc[840:859, 'amount'] = [out_of_range[i % len(out_of_range)] for i in range(20)]

    # 861-880: Invalid date formats
    invalid_dates = ['2025/01/01', '01-01-2025', 'invalid', 'Jan 1, 2025']
    df.loc[860:879, 'created_date'] = [invalid_dates[i % len(invalid_dates)] for i in range(20)]

    # 881-900: Duplicate record_ids
    df.loc[880:899, 'record_id'] = [1, 2, 3, 4, 5] * 4

    # 901-920: Invalid status values
    invalid_status = ['ACTIVE', 'unknown', 'invalid', 'STATUS']
    df.loc[900:919, 'status'] = [invalid_status[i % len(invalid_status)] for i in range(20)]

    # 921-940: Statistical outliers in quantity
    outlier_values = [1000, 2000, 5000, 10000]
    df.loc[920:939, 'quantity'] = [outlier_values[i % len(outlier_values)] for i in range(20)]

    # 941-960: String length violations (over 500 chars)
    df.loc[940:959, 'description'] = ['X' * 600] * 20

    # 961-980: Cross-field violations (updated_date < created_date)
    df.loc[960:979, 'created_date'] = '2025-12-01'
    df.loc[960:979, 'updated_date'] = '2025-01-01'

    # 981-1000: Orphan foreign keys (references non-existent customers)
    df.loc[980:999, 'customer_ref'] = 'CUST-99999'

    return df


def generate_customers(n_rows=500):
    """Generate customer reference data for cross-file validation tests."""
    print(f"Generating customers dataset with {n_rows} rows...")
    return pd.DataFrame({
        'customer_id': [f'CUST-{i:05d}' for i in range(1, n_rows + 1)],
        'name': [f'Customer {i}' for i in range(1, n_rows + 1)],
        'email': [f'customer{i}@company.com' for i in range(1, n_rows + 1)],
        'status': [random.choice(['active', 'inactive']) for _ in range(n_rows)],
        'created_date': [(datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d') for _ in range(n_rows)],
    })


def generate_orders(n_rows=2000, customer_count=500):
    """Generate orders with foreign keys to customers for cross-file validation."""
    print(f"Generating orders dataset with {n_rows} rows...")
    return pd.DataFrame({
        'order_id': list(range(1, n_rows + 1)),
        'customer_id': [f'CUST-{random.randint(1, customer_count):05d}' for _ in range(n_rows)],
        'product_id': [f'PROD-{random.randint(1, 100):03d}' for _ in range(n_rows)],
        'amount': [round(random.uniform(10, 1000), 2) for _ in range(n_rows)],
        'quantity': [random.randint(1, 10) for _ in range(n_rows)],
        'order_date': [(datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d') for _ in range(n_rows)],
        'status': [random.choice(['pending', 'shipped', 'delivered', 'cancelled']) for _ in range(n_rows)],
    })


def generate_products(n_rows=100):
    """Generate products reference data."""
    print(f"Generating products dataset with {n_rows} rows...")
    categories = ['electronics', 'clothing', 'food', 'services', 'home', 'sports']
    return pd.DataFrame({
        'product_id': [f'PROD-{i:03d}' for i in range(1, n_rows + 1)],
        'name': [f'Product {i}' for i in range(1, n_rows + 1)],
        'category': [random.choice(categories) for _ in range(n_rows)],
        'price': [round(random.uniform(5, 500), 2) for _ in range(n_rows)],
        'in_stock': [random.choice([True, False]) for _ in range(n_rows)],
    })


def main():
    """Generate all test datasets."""
    print("=" * 60)
    print("DataK9 Validation Test Data Generator")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    # Main comprehensive dataset
    df_main = generate_comprehensive_dataset(1000)

    # Save as CSV
    csv_path = OUTPUT_DIR / "testsuite_validation_comprehensive.csv"
    df_main.to_csv(csv_path, index=False)
    print(f"  Created: {csv_path.name} ({len(df_main)} rows)")

    # Save as Parquet
    parquet_path = OUTPUT_DIR / "testsuite_validation_comprehensive.parquet"
    df_main.to_parquet(parquet_path, index=False)
    print(f"  Created: {parquet_path.name}")

    print()

    # Customer reference data
    df_customers = generate_customers(500)
    customers_path = OUTPUT_DIR / "testsuite_validation_customers.csv"
    df_customers.to_csv(customers_path, index=False)
    print(f"  Created: {customers_path.name} ({len(df_customers)} rows)")

    # Orders with FK to customers
    df_orders = generate_orders(2000)
    orders_path = OUTPUT_DIR / "testsuite_validation_orders.csv"
    df_orders.to_csv(orders_path, index=False)
    print(f"  Created: {orders_path.name} ({len(df_orders)} rows)")

    # Products reference data
    df_products = generate_products(100)
    products_path = OUTPUT_DIR / "testsuite_validation_products.csv"
    df_products.to_csv(products_path, index=False)
    print(f"  Created: {products_path.name} ({len(df_products)} rows)")

    print()
    print("=" * 60)
    print("Test data generation complete!")
    print("=" * 60)
    print()
    print("Data quality issues in comprehensive dataset (rows 801-1000):")
    print("  801-820: Missing mandatory fields (email, amount)")
    print("  821-840: Invalid email formats")
    print("  841-860: Out-of-range amounts (negative and >100000)")
    print("  861-880: Invalid date formats")
    print("  881-900: Duplicate record_ids")
    print("  901-920: Invalid status values")
    print("  921-940: Statistical outliers in quantity")
    print("  941-960: String length violations (>500 chars)")
    print("  961-980: Cross-field date violations (updated < created)")
    print("  981-1000: Orphan foreign keys")
    print()


if __name__ == "__main__":
    main()
