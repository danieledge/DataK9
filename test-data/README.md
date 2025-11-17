# DataK9 Test Datasets

This directory contains organized test datasets for the DataK9 demo system, structured into 5 tiers from Tiny to Ultimate.

## Dataset Tiers

### Tier 1: Tiny (Included ✅)
**Location**: `tiny/`
**Size**: ~151 KB total
**Rows**: ~2,500 rows across 3 files
**Included in Git**: Yes ✅

Files:
- `customers.csv` (88K, ~1,000 customers)
- `accounts.csv` (23K, ~500 accounts)
- `transactions.csv` (40K, ~1,000 transactions)

**Purpose**: Quick testing, tutorials, getting started

---

### Tier 2: Small (Included ✅)
**Location**: `small/`
**Size**: 18 MB (CSV), 5.3 MB (Parquet)
**Rows**: 100,000 rows
**Included in Git**: Symlinks only

Files:
- `ecommerce_transactions.csv` (18 MB)
- `ecommerce_transactions.parquet` (5.3 MB)

**Purpose**: Medium-sized dataset testing, profiling demos, performance benchmarks

**Actual Data Location**: `../../test-data/` (parent directory)

---

### Tier 3-5: Medium, Large, Ultimate (Download Required ⬇️)

**These tiers contain IBM AML banking transaction datasets from Kaggle.**

| Tier | Size | Rows | Files |
|------|------|------|-------|
| Medium | ~500 MB | 5M | HI-Small_Trans, LI-Small_Trans (CSV + Parquet) |
| Large | ~3 GB | 31M | HI-Medium_Trans, LI-Medium_Trans (CSV + Parquet) |
| Ultimate | 16 GB (CSV), 5.1 GB (Parquet) | 179M | HI-Large_Trans, LI-Large_Trans (CSV + Parquet) |

## Downloading Large Datasets

### IBM AML Banking Transactions

**Source**: IBM Transactions for Anti Money Laundering (AML)
**Kaggle URL**: https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
**License**: Community Data License Agreement - Sharing - Version 1.0

#### Installation Steps:

1. **Install Kaggle CLI** (if not already installed):
   ```bash
   pip install kaggle
   ```

2. **Configure Kaggle API credentials**:
   - Go to https://www.kaggle.com/account
   - Scroll to "API" section
   - Click "Create New Token"
   - Save `kaggle.json` to `~/.kaggle/`
   - Set permissions: `chmod 600 ~/.kaggle/kaggle.json`

3. **Download the dataset**:
   ```bash
   # Create download directory
   cd ~/www/dqa
   mkdir -p test-data
   cd test-data

   # Download dataset (39 GB total)
   kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml

   # Unzip
   unzip ibm-transactions-for-anti-money-laundering-aml.zip
   ```

4. **Verify the download**:
   ```bash
   ls -lh ~/www/dqa/test-data/

   # You should see:
   # HI-Small_Trans.csv (454 MB)
   # LI-Small_Trans.csv (621 MB)
   # HI-Medium_Trans.csv (2.9 GB)
   # LI-Medium_Trans.csv (2.8 GB)
   # HI-Large_Trans.csv (16 GB)
   # LI-Large_Trans.csv (16 GB)
   # Plus account and pattern files
   ```

5. **The symlinks will automatically work** once files are in place:
   ```bash
   cd ~/www/dqa/data-validation-tool

   # Create symbolic links for Parquet files (if you convert CSV to Parquet)
   # This is optional but recommended for better performance

   # Medium tier should link to:
   # ../../../../test-data/HI-Small_Trans.*
   # ../../../../test-data/LI-Small_Trans.*

   # Large tier should link to:
   # ../../../../test-data/HI-Medium_Trans.*
   # ../../../../test-data/LI-Medium_Trans.*

   # Ultimate tier should link to:
   # ../../../../test-data/HI-Large_Trans.*
   # ../../../../test-data/LI-Large_Trans.*
   ```

## Dataset Details

### IBM AML Dataset Schema

**Common columns**:
- `Timestamp` - Transaction timestamp
- `From Bank` - Source bank identifier
- `Account` - Account number
- `To Bank` - Destination bank identifier
- `Account.1` - Destination account number
- `Amount Received` - Transaction amount
- `Receiving Currency` - Currency code
- `Amount Paid` - Amount paid (may differ from received)
- `Payment Currency` - Payment currency code
- `Payment Format` - Transaction type (WIRE, CREDIT, DEPOSIT, CHECK, etc.)
- `Is Laundering` - Ground truth label (0 or 1)

**HI vs LI**:
- **HI** (High Illicit): Higher percentage of money laundering transactions
- **LI** (Low Illicit): Lower percentage of money laundering transactions

**Small vs Medium vs Large**:
- **Small**: ~5M rows, ~500 MB
- **Medium**: ~31M rows, ~3 GB
- **Large**: ~179M rows, ~16 GB CSV

### E-Commerce Dataset Schema

**Columns**:
- `Transaction ID` - Unique identifier
- `Customer ID` - Customer identifier
- `Customer Name`, `Customer Email`, `Customer Age`
- `Customer City`, `Customer State`, `Customer Country`
- `Product ID`, `Product Name`, `Product Category`, `Product Brand`
- `Quantity`, `Unit Price`, `Total Amount`, `Discount Amount`
- `Payment Method` - Credit Card, PayPal, Bank Transfer, etc.
- `Transaction Date`, `Transaction Status`
- `Shipping Address`, `Billing Address`

## Using the Datasets

### With Demo Script

```bash
./demo.sh

# Choose operation (Validate or Profile)
# Select dataset tier (1-5)
# Select file
# Watch DataK9 work!
```

### Direct CLI Usage

**Profile a small dataset**:
```bash
python3 -m validation_framework.cli profile test-data/tiny/customers.csv -o profile.html
```

**Validate with config**:
```bash
python3 -m validation_framework.cli validate my_config.yaml
```

**Profile large dataset** (with sampling):
```bash
python3 -m validation_framework.cli profile test-data/ultimate/HI-Large_Trans.parquet -o profile.html
```

## Performance Notes

### Parquet vs CSV
- **Parquet**: 10x faster, 1/3 the size
- **CSV**: Universal compatibility

### Recommended Approach
1. **Tiny/Small**: Use CSV (fast enough)
2. **Medium**: Use Parquet for speed
3. **Large**: Strongly recommend Parquet
4. **Ultimate**: **Must use Parquet** (179M rows)

### Expected Processing Times

| Tier | Format | Rows | Expected Time |
|------|--------|------|---------------|
| Tiny | CSV | 2.5K | <1 second |
| Small | CSV | 100K | ~10 seconds |
| Small | Parquet | 100K | ~5 seconds |
| Medium | CSV | 5M | ~2 minutes |
| Medium | Parquet | 5M | ~30 seconds |
| Large | CSV | 31M | ~15 minutes |
| Large | Parquet | 31M | ~3 minutes |
| Ultimate | CSV | 179M | ~2 hours |
| Ultimate | Parquet | 179M | ~10 minutes |

**Note**: Times are approximate and depend on:
- Hardware (CPU, memory, disk speed)
- Operations performed (profiling vs validation)
- Validation complexity
- Chunk size settings

## Converting CSV to Parquet

For better performance with large datasets:

```python
import pandas as pd

# Read CSV
df = pd.read_csv('HI-Large_Trans.csv')

# Write Parquet
df.to_parquet('HI-Large_Trans.parquet',
              engine='pyarrow',
              compression='snappy',
              index=False)
```

Or use DataK9's built-in conversion (coming soon).

## Storage Requirements

| What | Size |
|------|------|
| Tiny (included in Git) | 151 KB |
| Small (symlink only) | 4 KB |
| Medium (download required) | ~1.1 GB |
| Large (download required) | ~5.7 GB |
| Ultimate (download required) | ~32 GB |
| **Total (all tiers)** | **~39 GB** |

## Troubleshooting

### Symlinks not working?
```bash
# Check if actual files exist
ls -lh ../../test-data/

# Recreate symlinks
cd data-validation-tool/test-data/small
rm -f *.csv *.parquet
ln -sf ../../../test-data/ecommerce_transactions.csv .
ln -sf ../../../test-data/ecommerce_transactions.parquet .
```

### Kaggle download fails?
```bash
# Verify credentials
cat ~/.kaggle/kaggle.json

# Check permissions
chmod 600 ~/.kaggle/kaggle.json

# Try manual download from web interface
# https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
```

### Out of memory?
```bash
# Increase chunk size (process more rows at once but use more memory)
# or decrease chunk size (process fewer rows but use less memory)

# Edit config:
processing:
  chunk_size: 10000  # Smaller = less memory, slower processing
```

## Example Configs

Pre-built validation configs are available in `test-data/configs/`:

- `comprehensive_test_config.yaml` - 25+ validations on e-commerce data
- `comprehensive_large_test_config.yaml` - 25+ validations on HI-Large (179M rows)
- `optimized_validations_test.yaml` - Performance-optimized validations
- `ultimate_validation_showcase.yaml` - All 33 validation types on 357M rows

## Documentation

- **User Guide**: `docs/USER_GUIDE.md`
- **Architecture Reference**: `ARCHITECTURE_REFERENCE.md`
- **Validation Catalog**: `docs/VALIDATION_CATALOG.md`
- **Demo Script**: `demo.sh`
- **Test Runner**: `scripts/run_tests.sh`

---

**Questions? Issues?**
Open an issue on GitHub or check the documentation.

🐕 **DataK9 - Your K9 Guardian for Data Quality** 🐕
