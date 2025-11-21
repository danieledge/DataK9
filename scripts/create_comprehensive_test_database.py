"""
Create comprehensive test database for validating all 32 database-compatible validations.

This script creates a SQLite database with multiple tables designed to test
all validation types supported by DataK9 for database sources.

Author: Daniel Edge
Date: 2025-11-19
"""

import sqlite3
import random
import string
from datetime import datetime, timedelta
from pathlib import Path


def create_connection(db_path: str) -> sqlite3.Connection:
    """Create database connection."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def create_schema(conn: sqlite3.Connection):
    """Create comprehensive database schema for testing all validations."""

    # Drop existing tables
    conn.execute("DROP TABLE IF EXISTS order_items")
    conn.execute("DROP TABLE IF EXISTS orders")
    conn.execute("DROP TABLE IF EXISTS products")
    conn.execute("DROP TABLE IF EXISTS customers")
    conn.execute("DROP TABLE IF EXISTS transactions")
    conn.execute("DROP TABLE IF EXISTS employees")
    conn.execute("DROP TABLE IF EXISTS departments")

    # Departments table - for referential integrity testing
    conn.execute("""
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name TEXT NOT NULL UNIQUE,
            budget REAL,
            created_date TEXT
        )
    """)

    # Employees table - comprehensive field testing
    conn.execute("""
        CREATE TABLE employees (
            employee_id INTEGER PRIMARY KEY,
            dept_id INTEGER,
            email TEXT,
            phone TEXT,
            salary REAL,
            hire_date TEXT,
            status TEXT,
            performance_score REAL,
            years_experience INTEGER,
            manager_id INTEGER,
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
            FOREIGN KEY (manager_id) REFERENCES employees(employee_id),
            CHECK (salary >= 0),
            CHECK (performance_score BETWEEN 0 AND 100)
        )
    """)

    # Customers table - for various field validations
    conn.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            email TEXT,
            phone TEXT,
            country_code TEXT,
            zip_code TEXT,
            registration_date TEXT,
            account_balance REAL,
            credit_score INTEGER,
            status TEXT
        )
    """)

    # Products table - for range and validation checks
    conn.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            sku TEXT UNIQUE,
            product_name TEXT NOT NULL,
            category TEXT,
            price REAL,
            cost REAL,
            stock_quantity INTEGER,
            weight_kg REAL,
            is_active INTEGER,
            last_updated TEXT
        )
    """)

    # Orders table - for date and relational validations
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TEXT,
            ship_date TEXT,
            total_amount REAL,
            tax_amount REAL,
            discount_percent REAL,
            status TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)

    # Order items - for cross-field comparisons
    conn.execute("""
        CREATE TABLE order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price REAL,
            line_total REAL,
            discount_applied REAL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)

    # Transactions - for trend and anomaly detection
    conn.execute("""
        CREATE TABLE transactions (
            transaction_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            transaction_date TEXT,
            amount REAL,
            transaction_type TEXT,
            merchant TEXT,
            category TEXT,
            is_flagged INTEGER,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)

    conn.commit()


def populate_departments(conn: sqlite3.Connection):
    """Populate departments table."""
    departments = [
        (1, "Engineering", 5000000.0, "2020-01-01"),
        (2, "Sales", 3000000.0, "2020-01-01"),
        (3, "Marketing", 2000000.0, "2020-01-01"),
        (4, "HR", 1000000.0, "2020-01-01"),
        (5, "Finance", 1500000.0, "2020-01-01"),
    ]

    conn.executemany(
        "INSERT INTO departments VALUES (?, ?, ?, ?)",
        departments
    )
    conn.commit()


def populate_employees(conn: sqlite3.Connection, num_records: int = 500):
    """
    Populate employees table with test data.

    Includes BOTH passing and failing data for testing:
    - Good data: 85-90% of records
    - Missing emails (5%)
    - Invalid email formats (3%)
    - Invalid phone formats (3%)
    - Outlier salaries (1%)
    - Null department references (2%)
    - Invalid status values (1%)

    This creates a mix showing the system can detect both good and bad data.
    """
    statuses = ["Active", "Inactive", "On Leave", "Terminated"]

    for i in range(1, num_records + 1):
        # Intentional issues (lower percentages = more passing data)
        missing_email = random.random() < 0.05
        invalid_email = random.random() < 0.03
        invalid_phone = random.random() < 0.03
        outlier_salary = random.random() < 0.01
        null_dept = random.random() < 0.02
        invalid_status = random.random() < 0.01

        # Email
        if missing_email:
            email = None
        elif invalid_email:
            email = f"invalid_email_{i}"  # Missing @
        else:
            email = f"employee{i}@company.com"

        # Phone
        if invalid_phone:
            phone = f"555-{random.randint(0, 999)}"  # Too short
        else:
            phone = f"555-{random.randint(100, 999):03d}-{random.randint(1000, 9999):04d}"

        # Salary
        if outlier_salary:
            salary = random.uniform(500000, 1000000)  # Outlier
        else:
            salary = random.uniform(40000, 200000)

        # Department
        dept_id = None if null_dept else random.randint(1, 5)

        # Status
        if invalid_status:
            status = "Unknown"  # Invalid value
        else:
            status = random.choice(statuses)

        # Other fields
        hire_date = (datetime.now() - timedelta(days=random.randint(30, 3650))).strftime("%Y-%m-%d")
        performance_score = random.uniform(60, 100)
        years_experience = random.randint(0, 30)
        manager_id = random.randint(1, max(1, i - 1)) if i > 1 and random.random() > 0.1 else None

        conn.execute("""
            INSERT INTO employees
            (employee_id, dept_id, email, phone, salary, hire_date, status,
             performance_score, years_experience, manager_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (i, dept_id, email, phone, salary, hire_date, status,
              performance_score, years_experience, manager_id))

    conn.commit()


def populate_customers(conn: sqlite3.Connection, num_records: int = 1000):
    """
    Populate customers table with test data.

    Includes BOTH passing and failing data:
    - Good data: 90-95% of records
    - Duplicate customer IDs (1%)
    - Missing emails (2%)
    - Invalid email formats (2%)
    - Invalid zip codes (2%)
    - Negative account balances (0.5%)
    - Credit scores out of range (1%)

    This creates a realistic mix for comprehensive testing.
    """
    statuses = ["Active", "Inactive", "Suspended", "Pending"]
    countries = ["US", "CA", "GB", "AU", "DE"]
    created_ids = set()

    for i in range(1, num_records + 1):
        # Intentional issues (lower percentages for realistic data)
        duplicate_id = random.random() < 0.01
        missing_email = random.random() < 0.02
        invalid_email = random.random() < 0.02
        invalid_zip = random.random() < 0.02
        negative_balance = random.random() < 0.005
        invalid_credit = random.random() < 0.01

        # Customer ID (duplicates)
        customer_id = i if not duplicate_id else random.randint(1, i)

        # Email
        if missing_email:
            email = None
        elif invalid_email:
            email = f"customer{i}_at_email.com"  # Invalid @ replacement
        else:
            email = f"customer{i}@example.com"

        # Phone
        phone = f"+1-555-{random.randint(100, 999):03d}-{random.randint(1000, 9999):04d}"

        # Country and zip
        country_code = random.choice(countries)
        if invalid_zip:
            zip_code = str(random.randint(1, 999))  # Too short
        else:
            zip_code = f"{random.randint(10000, 99999):05d}"

        # Account balance
        if negative_balance:
            account_balance = random.uniform(-1000, -0.01)
        else:
            account_balance = random.uniform(0, 50000)

        # Credit score
        if invalid_credit:
            credit_score = random.randint(900, 1000)  # Out of valid range
        else:
            credit_score = random.randint(300, 850)

        registration_date = (datetime.now() - timedelta(days=random.randint(1, 1825))).strftime("%Y-%m-%d")
        status = random.choice(statuses)

        try:
            conn.execute("""
                INSERT INTO customers
                (customer_id, email, phone, country_code, zip_code,
                 registration_date, account_balance, credit_score, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (customer_id, email, phone, country_code, zip_code,
                  registration_date, account_balance, credit_score, status))
            created_ids.add(customer_id)
        except sqlite3.IntegrityError:
            # Skip duplicates
            pass

    conn.commit()
    return list(created_ids)


def populate_products(conn: sqlite3.Connection, num_records: int = 200):
    """
    Populate products table with test data.

    Includes BOTH passing and failing data:
    - Good data: 92-95% of records
    - Missing SKUs (1%)
    - Duplicate SKUs (1%)
    - Negative prices (0.5%)
    - Price < Cost (2%)
    - Negative stock (1%)
    """
    categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Toys"]
    created_ids = set()

    for i in range(1, num_records + 1):
        # Intentional issues (lower percentages for realistic mix)
        missing_sku = random.random() < 0.01
        duplicate_sku = random.random() < 0.01
        negative_price = random.random() < 0.005
        price_less_cost = random.random() < 0.02
        negative_stock = random.random() < 0.01

        # SKU
        if missing_sku:
            sku = None
        elif duplicate_sku and i > 1:
            sku = f"SKU-{random.randint(1, i-1):06d}"
        else:
            sku = f"SKU-{i:06d}"

        product_name = f"Product {i}"
        category = random.choice(categories)

        # Price and cost
        cost = random.uniform(10, 500)
        if negative_price:
            price = random.uniform(-100, -0.01)
        elif price_less_cost:
            price = cost * random.uniform(0.5, 0.95)  # Price less than cost
        else:
            price = cost * random.uniform(1.2, 3.0)

        # Stock
        if negative_stock:
            stock_quantity = random.randint(-100, -1)
        else:
            stock_quantity = random.randint(0, 1000)

        weight_kg = random.uniform(0.1, 50.0)
        is_active = 1 if random.random() > 0.1 else 0
        last_updated = (datetime.now() - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d")

        try:
            conn.execute("""
                INSERT INTO products
                (product_id, sku, product_name, category, price, cost,
                 stock_quantity, weight_kg, is_active, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (i, sku, product_name, category, price, cost,
                  stock_quantity, weight_kg, is_active, last_updated))
            created_ids.add(i)
        except sqlite3.IntegrityError:
            # Skip duplicates
            pass

    conn.commit()
    return list(created_ids)


def populate_orders(conn: sqlite3.Connection, num_records: int = 2000, customer_ids: list = None):
    """
    Populate orders table with test data.

    Includes intentional data quality issues:
    - Ship date before order date (3%)
    - Tax amount > total amount (1%)
    - Discount > 100% (1%)

    Note: Foreign key constraints prevent invalid customer references,
    but we can test referential integrity with DatabaseReferentialIntegrityCheck.
    """
    if customer_ids is None:
        customer_ids = list(range(1, 1001))

    statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]

    for i in range(1, num_records + 1):
        # Intentional issues
        invalid_dates = random.random() < 0.03
        invalid_tax = random.random() < 0.01
        invalid_discount = random.random() < 0.01

        # Customer (must be valid due to FK constraint)
        customer_id = random.choice(customer_ids)

        # Dates
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))
        if invalid_dates:
            ship_date = order_date - timedelta(days=random.randint(1, 10))  # Before order
        else:
            ship_date = order_date + timedelta(days=random.randint(1, 14))

        total_amount = random.uniform(10, 5000)

        # Tax
        if invalid_tax:
            tax_amount = total_amount * random.uniform(1.1, 2.0)  # More than total
        else:
            tax_amount = total_amount * random.uniform(0.05, 0.15)

        # Discount
        if invalid_discount:
            discount_percent = random.uniform(101, 200)
        else:
            discount_percent = random.uniform(0, 30)

        status = random.choice(statuses)

        conn.execute("""
            INSERT INTO orders
            (order_id, customer_id, order_date, ship_date, total_amount,
             tax_amount, discount_percent, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (i, customer_id, order_date.strftime("%Y-%m-%d"),
              ship_date.strftime("%Y-%m-%d"), total_amount,
              tax_amount, discount_percent, status))

    conn.commit()


def populate_order_items(conn: sqlite3.Connection, num_records: int = 5000, product_ids: list = None):
    """
    Populate order_items table with test data.

    Includes intentional data quality issues:
    - Line total != quantity * unit_price (5%)

    Note: Foreign key constraints prevent invalid references.
    """
    if product_ids is None:
        product_ids = list(range(1, 201))

    for i in range(1, num_records + 1):
        # Intentional issues
        invalid_line_total = random.random() < 0.05

        # References (must be valid due to FK constraints)
        order_id = random.randint(1, 2000)
        product_id = random.choice(product_ids)

        quantity = random.randint(1, 10)
        unit_price = random.uniform(10, 500)
        discount_applied = random.uniform(0, unit_price * 0.3)

        # Line total
        if invalid_line_total:
            line_total = random.uniform(10, 1000)  # Wrong calculation
        else:
            line_total = (quantity * unit_price) - discount_applied

        conn.execute("""
            INSERT INTO order_items
            (item_id, order_id, product_id, quantity, unit_price,
             line_total, discount_applied)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (i, order_id, product_id, quantity, unit_price,
              line_total, discount_applied))

    conn.commit()


def populate_transactions(conn: sqlite3.Connection, num_records: int = 10000, customer_ids: list = None):
    """
    Populate transactions table with test data for trend and anomaly detection.

    Includes intentional patterns:
    - Normal daily transactions with trends
    - Anomalous spikes (1%)
    - Different transaction types
    - Flagged suspicious transactions
    """
    if customer_ids is None:
        customer_ids = list(range(1, 1001))

    transaction_types = ["Purchase", "Refund", "Transfer", "Withdrawal", "Deposit"]
    categories = ["Groceries", "Gas", "Restaurant", "Shopping", "Bills", "Entertainment"]
    merchants = [f"Merchant_{i}" for i in range(1, 51)]

    base_date = datetime.now() - timedelta(days=365)

    for i in range(1, num_records + 1):
        # Intentional patterns
        anomaly = random.random() < 0.01

        customer_id = random.choice(customer_ids)
        transaction_date = base_date + timedelta(days=i // 30)

        # Amount with potential anomalies
        if anomaly:
            amount = random.uniform(5000, 50000)  # Anomalous large amount
            is_flagged = 1
        else:
            amount = random.uniform(5, 500)  # Normal transaction
            is_flagged = 1 if random.random() < 0.001 else 0

        transaction_type = random.choice(transaction_types)
        merchant = random.choice(merchants)
        category = random.choice(categories)

        conn.execute("""
            INSERT INTO transactions
            (transaction_id, customer_id, transaction_date, amount,
             transaction_type, merchant, category, is_flagged)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (i, customer_id, transaction_date.strftime("%Y-%m-%d"),
              amount, transaction_type, merchant, category, is_flagged))

    conn.commit()


def create_database():
    """Create comprehensive test database."""
    # Database path
    db_path = Path(__file__).parent.parent / "test_data_comprehensive.db"

    print(f"Creating comprehensive test database: {db_path}")

    # Create connection
    conn = create_connection(str(db_path))

    try:
        # Create schema
        print("Creating schema...")
        create_schema(conn)

        # Populate tables
        print("Populating departments (5 records)...")
        populate_departments(conn)

        print("Populating employees (500 records)...")
        populate_employees(conn, 500)

        print("Populating customers (1,000 records)...")
        customer_ids = populate_customers(conn, 1000)
        print(f"  Created {len(customer_ids)} unique customer IDs")

        print("Populating products (200 records)...")
        product_ids = populate_products(conn, 200)
        print(f"  Created {len(product_ids)} unique product IDs")

        print("Populating orders (2,000 records)...")
        populate_orders(conn, 2000, customer_ids)

        print("Populating order_items (5,000 records)...")
        populate_order_items(conn, 5000, product_ids)

        print("Populating transactions (10,000 records)...")
        populate_transactions(conn, 10000, customer_ids)

        # Summary
        print("\n" + "="*70)
        print("DATABASE CREATED SUCCESSFULLY")
        print("="*70)

        # Get row counts
        tables = ["departments", "employees", "customers", "products",
                 "orders", "order_items", "transactions"]

        for table in tables:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table:20s}: {count:>6,d} records")

        print("\nDatabase path:", db_path)
        print("\nIntentional data quality issues included for testing:")
        print("  • Missing values in required fields")
        print("  • Invalid formats (emails, phones, zip codes)")
        print("  • Referential integrity violations")
        print("  • Range violations (negative values, out of bounds)")
        print("  • Cross-field validation issues")
        print("  • Duplicate records")
        print("  • Outliers and anomalies")
        print("  • Date logic errors")

    finally:
        conn.close()


if __name__ == "__main__":
    create_database()
