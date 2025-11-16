# E-Commerce: Data Validation Examples

**DataK9 for E-Commerce Platforms**

This guide provides production-ready validation examples for e-commerce data. DataK9 guards your e-commerce data with K9 vigilance!

---

## Table of Contents

1. [Overview](#overview)
2. [Customer Data](#customer-data)
3. [Product Catalog](#product-catalog)
4. [Order Data](#order-data)
5. [Inventory Management](#inventory-management)
6. [Shopping Cart Data](#shopping-cart-data)
7. [Reviews and Ratings](#reviews-and-ratings)
8. [Payment Transactions](#payment-transactions)
9. [Shipping and Fulfillment](#shipping-and-fulfillment)
10. [Best Practices](#best-practices)

---

## Overview

### Common E-Commerce Data Challenges

- **Data consistency** (prices, inventory across channels)
- **Real-time requirements** (inventory, pricing updates)
- **Data completeness** (product attributes, customer info)
- **Order integrity** (order items match products)
- **Payment accuracy** (amounts, currency, precision)
- **Multi-channel sync** (web, mobile, marketplaces)

### E-Commerce Validation Priorities

1. **ERROR Severity:**
   - Missing product SKUs
   - Invalid prices or currencies
   - Order total mismatches
   - Failed referential integrity

2. **WARNING Severity:**
   - Missing optional product attributes
   - Low inventory levels
   - Unusual order patterns
   - Data completeness issues

---

## Customer Data

### Scenario: Customer Master Data

**Data:** Customer accounts from e-commerce platform

### Configuration

```yaml
validation_job:
  name: "Customer Data Validation"
  description: "Daily customer data validation for e-commerce platform"

settings:
  chunk_size: 50000
  max_sample_failures: 100

files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"

    validations:
      # File-level checks
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      # Schema validation
      - type: "ColumnPresenceCheck"
        severity: "ERROR"
        description: "Required customer fields"
        params:
          columns:
            - "customer_id"
            - "email"
            - "first_name"
            - "last_name"
            - "registration_date"
            - "customer_status"
            - "email_opt_in"

      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Core customer identifiers"
        params:
          fields:
            - "customer_id"
            - "email"
            - "registration_date"

      # Customer ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        description: "Customer IDs must be unique"
        params:
          fields: ["customer_id"]

      # Email uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        description: "Email addresses must be unique"
        params:
          fields: ["email"]

      # Email format validation
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid email format"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
          message: "Invalid email format"

      # Phone format (international)
      - type: "RegexCheck"
        severity: "WARNING"
        description: "Phone number format"
        params:
          field: "phone"
          pattern: "^\\+?[1-9][0-9]{7,14}$"

      # Customer status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "customer_status"
          valid_values:
            - "ACTIVE"
            - "INACTIVE"
            - "SUSPENDED"
            - "DELETED"

      # Email opt-in validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "email_opt_in"
          valid_values: ["YES", "NO"]

      # Registration date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "registration_date"
          format: "%Y-%m-%d"

      # Registration date not in future
      - type: "FreshnessCheck"
        severity: "ERROR"
        description: "Registration date cannot be in future"
        params:
          check_type: "field_value"
          field: "registration_date"
          max_age_hours: 0

      # Loyalty tier validation (if present)
      - type: "ValidValuesCheck"
        severity: "WARNING"
        params:
          field: "loyalty_tier"
          valid_values:
            - "BRONZE"
            - "SILVER"
            - "GOLD"
            - "PLATINUM"
```

---

## Product Catalog

### Scenario: Product Master Data

**Data:** Product catalog with pricing and attributes

### Configuration

```yaml
validation_job:
  name: "Product Catalog Validation"
  description: "Daily product catalog validation"

files:
  - name: "products"
    path: "data/products.csv"

    validations:
      # File checks
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Required product fields"
        params:
          fields:
            - "product_id"
            - "sku"
            - "product_name"
            - "category"
            - "price"
            - "currency"
            - "status"

      # Product ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["product_id"]

      # SKU uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        description: "SKUs must be unique"
        params:
          fields: ["sku"]

      # SKU format validation
      - type: "RegexCheck"
        severity: "ERROR"
        description: "SKU format"
        params:
          field: "sku"
          pattern: "^[A-Z0-9]{6,20}$"
          message: "SKU must be 6-20 alphanumeric characters"

      # Product name length
      - type: "StringLengthCheck"
        severity: "ERROR"
        description: "Product name length"
        params:
          field: "product_name"
          min_length: 3
          max_length: 200

      # Category validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid product categories"
        params:
          field: "category"
          valid_values:
            - "ELECTRONICS"
            - "CLOTHING"
            - "HOME_GARDEN"
            - "SPORTS"
            - "TOYS"
            - "BOOKS"
            - "BEAUTY"
            - "FOOD"
            - "AUTOMOTIVE"
            - "HEALTH"

      # Price validation
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Price must be positive"
        params:
          field: "price"
          min_value: 0.01
          max_value: 100000.00

      # Price precision (2 decimal places)
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        description: "Price must have max 2 decimal places"
        params:
          field: "price"
          max_decimal_places: 2

      # Currency validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid ISO currency codes"
        params:
          field: "currency"
          valid_values: ["USD", "EUR", "GBP", "CAD", "AUD"]

      # Product status
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "status"
          valid_values:
            - "ACTIVE"
            - "INACTIVE"
            - "DISCONTINUED"
            - "OUT_OF_STOCK"

      # Weight range (if present)
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Product weight validation"
        params:
          field: "weight_kg"
          min_value: 0.001
          max_value: 1000.0

      # Dimensions validation
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Product dimensions"
        params:
          field: "length_cm"
          min_value: 0.1
          max_value: 500.0

      # Discount price must be less than regular price
      - type: "CrossFieldComparisonCheck"
        severity: "ERROR"
        description: "Discount price must be less than regular price"
        params:
          field1: "discount_price"
          field2: "price"
          operator: "<"

      # Price outlier detection
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        description: "Detect unusual prices"
        params:
          field: "price"
          method: "iqr"
          threshold: 1.5
```

---

## Order Data

### Scenario: Customer Orders

**Data:** E-commerce order data with line items

### Configuration

```yaml
validation_job:
  name: "Order Data Validation"
  description: "Real-time order validation"

settings:
  chunk_size: 100000

files:
  - name: "orders"
    path: "data/orders.parquet"
    format: "parquet"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "order_id"
            - "customer_id"
            - "order_date"
            - "order_status"
            - "subtotal"
            - "tax"
            - "shipping"
            - "total"
            - "currency"
            - "payment_method"

      # Order ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["order_id"]

      # Customer reference check
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        description: "Customer must exist"
        params:
          foreign_key: "customer_id"
          reference_file: "data/customers.csv"
          reference_key: "customer_id"

      # Order status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "order_status"
          valid_values:
            - "PENDING"
            - "CONFIRMED"
            - "PROCESSING"
            - "SHIPPED"
            - "DELIVERED"
            - "CANCELLED"
            - "REFUNDED"

      # All monetary fields must be non-negative
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Subtotal must be non-negative"
        params:
          field: "subtotal"
          min_value: 0

      - type: "RangeCheck"
        severity: "ERROR"
        description: "Tax must be non-negative"
        params:
          field: "tax"
          min_value: 0

      - type: "RangeCheck"
        severity: "ERROR"
        description: "Shipping must be non-negative"
        params:
          field: "shipping"
          min_value: 0

      # Monetary precision (2 decimal places)
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "subtotal"
          max_decimal_places: 2

      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "tax"
          max_decimal_places: 2

      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "total"
          max_decimal_places: 2

      # Total calculation validation (within 0.01 tolerance for rounding)
      # Note: This is complex - usually done in custom validation
      # Simplified: total should be close to subtotal + tax + shipping

      # Currency validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "currency"
          valid_values: ["USD", "EUR", "GBP", "CAD", "AUD"]

      # Payment method validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "payment_method"
          valid_values:
            - "CREDIT_CARD"
            - "DEBIT_CARD"
            - "PAYPAL"
            - "APPLE_PAY"
            - "GOOGLE_PAY"
            - "BANK_TRANSFER"
            - "COD"  # Cash on delivery

      # Order date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "order_date"
          format: "%Y-%m-%d %H:%M:%S"

      # Order must be within reasonable time (not future)
      - type: "FreshnessCheck"
        severity: "ERROR"
        description: "Order date cannot be in future"
        params:
          check_type: "field_value"
          field: "order_date"
          max_age_hours: 0

      # Conditional: Shipped orders need tracking
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Shipped orders need tracking number"
        params:
          fields: ["tracking_number", "carrier"]
        condition: "order_status == 'SHIPPED'"

      # Conditional: Refunded orders need refund details
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Refunded orders need refund details"
        params:
          fields: ["refund_amount", "refund_date", "refund_reason"]
        condition: "order_status == 'REFUNDED'"

      # Large order flagging
      - type: "MandatoryFieldCheck"
        severity: "WARNING"
        description: "Large orders should be flagged for review"
        params:
          fields: ["fraud_review_completed"]
        condition: "total > 5000"

      # Outlier detection for order totals
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        description: "Detect unusual order amounts"
        params:
          field: "total"
          method: "zscore"
          threshold: 3.0

  # Order Line Items
  - name: "order_items"
    path: "data/order_items.parquet"
    format: "parquet"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "order_item_id"
            - "order_id"
            - "product_id"
            - "sku"
            - "quantity"
            - "unit_price"
            - "line_total"

      # Order item ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["order_item_id"]

      # Order reference check
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        description: "Order must exist"
        params:
          foreign_key: "order_id"
          reference_file: "data/orders.parquet"
          reference_key: "order_id"
          reference_file_format: "parquet"

      # Product reference check
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        description: "Product must exist"
        params:
          foreign_key: "product_id"
          reference_file: "data/products.csv"
          reference_key: "product_id"

      # Quantity validation
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Quantity must be positive integer"
        params:
          field: "quantity"
          min_value: 1
          max_value: 1000

      # Unit price validation
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "unit_price"
          min_value: 0.01
          max_value: 100000.00

      # Price precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "unit_price"
          max_decimal_places: 2

      # Line total precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "line_total"
          max_decimal_places: 2
```

---

## Inventory Management

### Scenario: Inventory Levels

**Data:** Real-time inventory data

### Configuration

```yaml
validation_job:
  name: "Inventory Validation"
  description: "Inventory level validation"

files:
  - name: "inventory"
    path: "data/inventory.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "inventory_id"
            - "sku"
            - "warehouse_id"
            - "quantity_on_hand"
            - "quantity_reserved"
            - "quantity_available"
            - "last_updated"

      # Composite key uniqueness (SKU + Warehouse)
      - type: "DuplicateRowCheck"
        severity: "ERROR"
        description: "SKU + Warehouse must be unique"
        params:
          key_fields: ["sku", "warehouse_id"]

      # SKU reference check
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        description: "SKU must exist in product catalog"
        params:
          foreign_key: "sku"
          reference_file: "data/products.csv"
          reference_key: "sku"

      # Quantity validations
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Quantity on hand must be non-negative"
        params:
          field: "quantity_on_hand"
          min_value: 0
          max_value: 1000000

      - type: "RangeCheck"
        severity: "ERROR"
        description: "Quantity reserved must be non-negative"
        params:
          field: "quantity_reserved"
          min_value: 0

      # Quantity available calculation check
      # available = on_hand - reserved
      - type: "CrossFieldComparisonCheck"
        severity: "ERROR"
        description: "Reserved cannot exceed on-hand"
        params:
          field1: "quantity_reserved"
          field2: "quantity_on_hand"
          operator: "<="

      # Low stock warning
      - type: "MandatoryFieldCheck"
        severity: "WARNING"
        description: "Low stock should trigger reorder"
        params:
          fields: ["reorder_triggered"]
        condition: "quantity_available < reorder_point"

      # Negative available quantity error
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Available quantity cannot be negative"
        params:
          field: "quantity_available"
          min_value: 0

      # Last updated timestamp
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "last_updated"
          format: "%Y-%m-%d %H:%M:%S"

      # Inventory freshness (updated within 1 hour)
      - type: "FreshnessCheck"
        severity: "WARNING"
        description: "Inventory should be updated hourly"
        params:
          check_type: "field_value"
          field: "last_updated"
          max_age_hours: 1
```

---

## Shopping Cart Data

### Scenario: Active Shopping Carts

**Data:** Customer shopping cart sessions

### Configuration

```yaml
validation_job:
  name: "Shopping Cart Validation"
  description: "Active cart data validation"

files:
  - name: "shopping_carts"
    path: "data/carts.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "cart_id"
            - "customer_id"
            - "session_id"
            - "created_date"
            - "last_updated"
            - "cart_status"

      # Cart ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["cart_id"]

      # Customer reference (can be null for guest carts)
      - type: "ReferentialIntegrityCheck"
        severity: "WARNING"
        description: "Customer should exist (if not guest)"
        params:
          foreign_key: "customer_id"
          reference_file: "data/customers.csv"
          reference_key: "customer_id"
          allow_null: true

      # Cart status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "cart_status"
          valid_values:
            - "ACTIVE"
            - "ABANDONED"
            - "CONVERTED"
            - "EXPIRED"

      # Date formats
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "created_date"
          format: "%Y-%m-%d %H:%M:%S"

      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "last_updated"
          format: "%Y-%m-%d %H:%M:%S"

      # Last updated must be after or equal to created
      - type: "CrossFieldComparisonCheck"
        severity: "ERROR"
        description: "Last updated must be after created"
        params:
          field1: "created_date"
          field2: "last_updated"
          operator: "<="

      # Abandoned cart detection (not updated in 24 hours)
      - type: "FreshnessCheck"
        severity: "WARNING"
        description: "Mark old carts as abandoned"
        params:
          check_type: "field_value"
          field: "last_updated"
          max_age_hours: 24
```

---

## Reviews and Ratings

### Scenario: Product Reviews

**Data:** Customer product reviews

### Configuration

```yaml
validation_job:
  name: "Product Reviews Validation"
  description: "Customer review validation"

files:
  - name: "reviews"
    path: "data/reviews.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "review_id"
            - "product_id"
            - "customer_id"
            - "rating"
            - "review_date"
            - "status"

      # Review ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["review_id"]

      # Product reference
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "product_id"
          reference_file: "data/products.csv"
          reference_key: "product_id"

      # Customer reference
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "customer_id"
          reference_file: "data/customers.csv"
          reference_key: "customer_id"

      # Rating validation (1-5 stars)
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Rating must be 1-5"
        params:
          field: "rating"
          min_value: 1
          max_value: 5

      # Review status
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "status"
          valid_values:
            - "PENDING"
            - "APPROVED"
            - "REJECTED"
            - "FLAGGED"

      # Review text length
      - type: "StringLengthCheck"
        severity: "WARNING"
        description: "Review text length"
        params:
          field: "review_text"
          min_length: 10
          max_length: 5000

      # Review date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "review_date"
          format: "%Y-%m-%d"

      # Review cannot be in future
      - type: "FreshnessCheck"
        severity: "ERROR"
        params:
          check_type: "field_value"
          field: "review_date"
          max_age_hours: 0

      # Verified purchase check
      - type: "ValidValuesCheck"
        severity: "WARNING"
        params:
          field: "verified_purchase"
          valid_values: ["YES", "NO"]
```

---

## Payment Transactions

### Scenario: Payment Processing

**Data:** Payment transaction logs

### Configuration

```yaml
validation_job:
  name: "Payment Transaction Validation"
  description: "Payment processing validation"

files:
  - name: "payments"
    path: "data/payments.parquet"
    format: "parquet"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "payment_id"
            - "order_id"
            - "payment_method"
            - "amount"
            - "currency"
            - "status"
            - "transaction_timestamp"

      # Payment ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["payment_id"]

      # Order reference
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "order_id"
          reference_file: "data/orders.parquet"
          reference_key: "order_id"
          reference_file_format: "parquet"

      # Payment method validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "payment_method"
          valid_values:
            - "CREDIT_CARD"
            - "DEBIT_CARD"
            - "PAYPAL"
            - "STRIPE"
            - "APPLE_PAY"
            - "GOOGLE_PAY"

      # Amount validation
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "amount"
          min_value: 0.01
          max_value: 100000.00

      # Amount precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "amount"
          max_decimal_places: 2

      # Currency validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "currency"
          valid_values: ["USD", "EUR", "GBP", "CAD", "AUD"]

      # Payment status
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "status"
          valid_values:
            - "PENDING"
            - "AUTHORIZED"
            - "CAPTURED"
            - "FAILED"
            - "REFUNDED"
            - "CANCELLED"

      # Timestamp format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "transaction_timestamp"
          format: "%Y-%m-%d %H:%M:%S"

      # Conditional: Failed payments need error code
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Failed payments need error details"
        params:
          fields: ["error_code", "error_message"]
        condition: "status == 'FAILED'"

      # Fraud detection - outlier amounts
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        description: "Detect potentially fraudulent amounts"
        params:
          field: "amount"
          method: "zscore"
          threshold: 4.0
```

---

## Shipping and Fulfillment

### Scenario: Shipment Tracking

**Data:** Shipping and delivery data

### Configuration

```yaml
validation_job:
  name: "Shipment Validation"
  description: "Shipping and fulfillment validation"

files:
  - name: "shipments"
    path: "data/shipments.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "shipment_id"
            - "order_id"
            - "carrier"
            - "tracking_number"
            - "ship_date"
            - "shipment_status"

      # Shipment ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["shipment_id"]

      # Order reference
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "order_id"
          reference_file: "data/orders.parquet"
          reference_key: "order_id"
          reference_file_format: "parquet"

      # Carrier validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "carrier"
          valid_values:
            - "UPS"
            - "FEDEX"
            - "USPS"
            - "DHL"
            - "AMAZON"

      # Tracking number format (varies by carrier)
      # Simplified: just check not empty
      - type: "StringLengthCheck"
        severity: "ERROR"
        params:
          field: "tracking_number"
          min_length: 5
          max_length: 50

      # Shipment status
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "shipment_status"
          valid_values:
            - "LABEL_CREATED"
            - "PICKED_UP"
            - "IN_TRANSIT"
            - "OUT_FOR_DELIVERY"
            - "DELIVERED"
            - "EXCEPTION"
            - "RETURNED"

      # Ship date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "ship_date"
          format: "%Y-%m-%d"

      # Conditional: Delivered shipments need delivery date
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Delivered shipments need delivery date"
        params:
          fields: ["delivery_date", "delivery_signature"]
        condition: "shipment_status == 'DELIVERED'"

      # Delivery date must be after ship date
      - type: "CrossFieldComparisonCheck"
        severity: "ERROR"
        description: "Delivery date must be after ship date"
        params:
          field1: "ship_date"
          field2: "delivery_date"
          operator: "<="
```

---

## Best Practices

### 1. Real-Time Validation

**Use Parquet for large datasets:**

```yaml
files:
  - path: "orders.parquet"
    format: "parquet"
    columns:  # Only load needed columns
      - "order_id"
      - "customer_id"
      - "total"
```

**Optimize chunk size:**

```yaml
settings:
  chunk_size: 100000  # Larger for order/transaction data
```

### 2. Price Precision

**Always validate monetary precision:**

```yaml
- type: "NumericPrecisionCheck"
  severity: "ERROR"
  params:
    field: "price"
    max_decimal_places: 2
```

### 3. Cross-Validation

**Validate order totals match items:**

```yaml
# In cross-file validation
- type: "CrossFileComparisonCheck"
  severity: "ERROR"
  params:
    aggregation: "sum"
    column: "line_total"
    comparison: "=="
    reference_file: "data/orders.parquet"
    reference_column: "subtotal"
    tolerance_pct: 0.01  # Allow 0.01% for rounding
```

### 4. Inventory Consistency

**Multi-channel inventory sync:**

```yaml
# Validate inventory across warehouses
- type: "CrossFileDuplicateCheck"
  severity: "ERROR"
  description: "SKU should not appear in multiple warehouses"
  params:
    columns: ["sku"]
    reference_files:
      - "warehouse_A_inventory.csv"
      - "warehouse_B_inventory.csv"
```

### 5. Fraud Detection

**Combine multiple checks:**

```yaml
# Statistical outliers
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "total"
    method: "modified_zscore"
    threshold: 4.0

# Large orders flagging
- type: "MandatoryFieldCheck"
  severity: "WARNING"
  params:
    fields: ["fraud_review_completed"]
  condition: "total > 5000"
```

---

## Next Steps

1. **[Finance Examples](finance.md)** - Financial data validation
2. **[Healthcare Examples](healthcare.md)** - HIPAA compliance
3. **[Best Practices](../using-datak9/best-practices.md)** - Production patterns

---

**🐕 DataK9 for E-Commerce - Your K9 guardian for online retail data quality**
