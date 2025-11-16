"""
Generate sample financial data for testing the validation framework.

This script creates synthetic CSV files with various data quality issues
to demonstrate the validation capabilities.

Author: daniel edge
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os


def generate_customers_csv(output_path: str, num_rows: int = 1000):
    """
    Generate sample customer data with various data quality issues.

    Issues included:
    - Some missing mandatory fields
    - Invalid email formats
    - Duplicate customer IDs
    - Invalid status values
    - Out of range balances
    - Blank rows
    """
    print(f"Generating {num_rows} customer records...")

    data = []
    for i in range(num_rows):
        # Introduce various issues
        customer_id = i + 1

        # 2% duplicate IDs
        if random.random() < 0.02 and i > 0:
            customer_id = random.randint(1, i)

        # 1% missing first names
        first_name = f"FirstName{i}" if random.random() > 0.01 else ""

        # 1% missing last names
        last_name = f"LastName{i}" if random.random() > 0.01 else ""

        # 3% invalid emails
        if random.random() > 0.03:
            email = f"user{i}@example.com"
        else:
            email = f"invalid_email_{i}"  # Invalid format

        # Generate phone number (some invalid)
        if random.random() > 0.05:
            phone = f"+1{random.randint(1000000000, 9999999999)}"
        else:
            phone = f"{random.randint(100, 999)}-BAD"  # Invalid format

        # Account balance (some negative or too high)
        if random.random() > 0.02:
            account_balance = round(random.uniform(0, 50000), 2)
        else:
            account_balance = round(random.uniform(-1000, 15000000), 2)

        # Registration date
        days_ago = random.randint(1, 1000)
        reg_date = datetime.now() - timedelta(days=days_ago)

        # 2% invalid date formats
        if random.random() > 0.02:
            registration_date = reg_date.strftime("%Y-%m-%d")
        else:
            registration_date = reg_date.strftime("%m/%d/%Y")  # Wrong format

        # Status (2% invalid values)
        if random.random() > 0.02:
            status = random.choice(["ACTIVE", "INACTIVE", "SUSPENDED", "PENDING"])
        else:
            status = random.choice(["UNKNOWN", "DELETED", ""])

        data.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "account_balance": account_balance,
            "registration_date": registration_date,
            "status": status
        })

    # Add 2% completely blank rows
    num_blank = int(num_rows * 0.02)
    for _ in range(num_blank):
        data.append({
            "customer_id": "",
            "first_name": "",
            "last_name": "",
            "email": "",
            "phone": "",
            "account_balance": "",
            "registration_date": "",
            "status": ""
        })

    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"✓ Generated: {output_path}")
    print(f"  - Total rows: {len(df)}")
    print(f"  - Columns: {len(df.columns)}")
    print(f"  - File size: {os.path.getsize(output_path) / 1024:.2f} KB\n")


def generate_transactions_parquet(output_path: str, num_rows: int = 5000):
    """
    Generate sample transaction data in Parquet format.

    Issues included:
    - Missing mandatory fields
    - Duplicate transaction IDs
    - Invalid transaction amounts
    - Invalid transaction types
    """
    print(f"Generating {num_rows} transaction records...")

    data = []
    for i in range(num_rows):
        # 1% duplicate transaction IDs
        if random.random() < 0.01 and i > 0:
            transaction_id = random.randint(1, i)
        else:
            transaction_id = i + 1

        # Customer ID (1% missing)
        customer_id = random.randint(1, 1000) if random.random() > 0.01 else None

        # Transaction amount (2% negative or zero, 1% too large)
        if random.random() > 0.03:
            amount = round(random.uniform(10, 5000), 2)
        elif random.random() > 0.5:
            amount = round(random.uniform(-500, 0), 2)  # Negative
        else:
            amount = round(random.uniform(1000001, 2000000), 2)  # Too large

        # Transaction date
        days_ago = random.randint(0, 365)
        transaction_date = datetime.now() - timedelta(days=days_ago)

        # Transaction type (2% invalid)
        if random.random() > 0.02:
            transaction_type = random.choice(["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"])
        else:
            transaction_type = random.choice(["REFUND", "CHARGEBACK", ""])

        # Description (some missing)
        description = f"Transaction {i}" if random.random() > 0.05 else ""

        data.append({
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "amount": amount,
            "transaction_date": transaction_date,
            "transaction_type": transaction_type,
            "description": description
        })

    df = pd.DataFrame(data)
    df.to_parquet(output_path, index=False)
    print(f"✓ Generated: {output_path}")
    print(f"  - Total rows: {len(df)}")
    print(f"  - Columns: {len(df.columns)}")
    print(f"  - File size: {os.path.getsize(output_path) / 1024:.2f} KB\n")


def generate_accounts_excel(output_path: str, num_rows: int = 500):
    """
    Generate sample account data in Excel format.

    Issues included:
    - Invalid account numbers (not 8 digits)
    - Missing mandatory fields
    - Invalid account types
    """
    print(f"Generating {num_rows} account records...")

    data = []
    for i in range(num_rows):
        account_id = i + 1

        # Account number (10% invalid format)
        if random.random() > 0.10:
            account_number = f"{random.randint(10000000, 99999999)}"  # Valid 8 digits
        else:
            # Invalid formats
            invalid_formats = [
                f"{random.randint(100, 999)}",  # Too short
                f"{random.randint(100000000, 999999999)}",  # Too long
                f"ABC{random.randint(10000, 99999)}",  # Contains letters
                ""  # Empty
            ]
            account_number = random.choice(invalid_formats)

        # Customer ID (2% missing)
        customer_id = random.randint(1, 1000) if random.random() > 0.02 else None

        # Account type (3% invalid)
        if random.random() > 0.03:
            account_type = random.choice(["CHECKING", "SAVINGS", "CREDIT", "INVESTMENT"])
        else:
            account_type = random.choice(["CRYPTO", "OFFSHORE", ""])

        # Opening balance
        opening_balance = round(random.uniform(0, 10000), 2)

        # Opening date
        days_ago = random.randint(30, 3650)
        opening_date = datetime.now() - timedelta(days=days_ago)

        data.append({
            "account_id": account_id,
            "account_number": account_number,
            "customer_id": customer_id,
            "account_type": account_type,
            "opening_balance": opening_balance,
            "opening_date": opening_date
        })

    df = pd.DataFrame(data)
    df.to_excel(output_path, index=False, sheet_name="Accounts")
    print(f"✓ Generated: {output_path}")
    print(f"  - Total rows: {len(df)}")
    print(f"  - Columns: {len(df.columns)}")
    print(f"  - File size: {os.path.getsize(output_path) / 1024:.2f} KB\n")


def main():
    """Generate all sample datasets."""
    print("\n" + "="*80)
    print("Generating Sample Financial Data for Testing")
    print("="*80 + "\n")

    # Create output directory
    output_dir = "examples/sample_data"
    os.makedirs(output_dir, exist_ok=True)

    # Generate datasets
    generate_customers_csv(f"{output_dir}/customers.csv", num_rows=1000)

    # Try parquet, fallback to CSV if pyarrow not available
    try:
        generate_transactions_parquet(f"{output_dir}/transactions.parquet", num_rows=5000)
    except Exception as e:
        print(f"Warning: Could not generate Parquet file ({e})")
        print("Generating transactions as CSV instead...\n")
        # Generate as CSV fallback
        data = []
        for i in range(5000):
            transaction_id = i + 1
            customer_id = random.randint(1, 1000) if random.random() > 0.01 else None
            amount = round(random.uniform(10, 5000), 2) if random.random() > 0.03 else round(random.uniform(-500, 0), 2)
            days_ago = random.randint(0, 365)
            transaction_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            transaction_type = random.choice(["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"]) if random.random() > 0.02 else ""
            description = f"Transaction {i}" if random.random() > 0.05 else ""
            data.append({
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "amount": amount,
                "transaction_date": transaction_date,
                "transaction_type": transaction_type,
                "description": description
            })
        df = pd.DataFrame(data)
        csv_path = f"{output_dir}/transactions.csv"
        df.to_csv(csv_path, index=False)
        print(f"✓ Generated: {csv_path}")
        print(f"  - Total rows: {len(df)}")
        print(f"  - File size: {os.path.getsize(csv_path) / 1024:.2f} KB\n")

    # Try Excel, fallback to CSV if openpyxl not available
    try:
        generate_accounts_excel(f"{output_dir}/accounts.xlsx", num_rows=500)
    except Exception as e:
        print(f"Warning: Could not generate Excel file ({e})")
        print("Generating accounts as CSV instead...\n")
        # Generate as CSV fallback
        data = []
        for i in range(500):
            account_id = i + 1
            account_number = f"{random.randint(10000000, 99999999)}" if random.random() > 0.10 else f"{random.randint(100, 999)}"
            customer_id = random.randint(1, 1000) if random.random() > 0.02 else None
            account_type = random.choice(["CHECKING", "SAVINGS", "CREDIT", "INVESTMENT"]) if random.random() > 0.03 else ""
            opening_balance = round(random.uniform(0, 10000), 2)
            days_ago = random.randint(30, 3650)
            opening_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
            data.append({
                "account_id": account_id,
                "account_number": account_number,
                "customer_id": customer_id,
                "account_type": account_type,
                "opening_balance": opening_balance,
                "opening_date": opening_date
            })
        df = pd.DataFrame(data)
        csv_path = f"{output_dir}/accounts.csv"
        df.to_csv(csv_path, index=False)
        print(f"✓ Generated: {csv_path}")
        print(f"  - Total rows: {len(df)}")
        print(f"  - File size: {os.path.getsize(csv_path) / 1024:.2f} KB\n")

    print("="*80)
    print("Sample data generation complete!")
    print("="*80)
    print("\nData quality issues included:")
    print("  - Missing mandatory fields")
    print("  - Invalid email/phone formats")
    print("  - Duplicate IDs")
    print("  - Out of range values")
    print("  - Invalid status/type values")
    print("  - Blank rows")
    print("  - Invalid account numbers")
    print("\nRun validation with:")
    print("  data-validate validate examples/sample_config.yaml")
    print()


if __name__ == "__main__":
    main()
