# Test Data Setup Guide

## Overview

DataK9 uses two test datasets for validation, performance testing, and demonstrations:

1. **E-Commerce Transactions** (17.5 MB) - Included in repository
2. **IBM AML Banking Transactions** (52 GB) - External download required

## Dataset 1: E-Commerce Transactions (Included)

**Location**: `examples/sample_data/ecommerce_transactions.csv`
**Size**: 17.5 MB (100,000 records, 22 columns)
**Status**: ✓ Included in repository (safe for GitHub)

This dataset is used for:
- Quick validation demos
- Documentation examples
- Basic testing
- Training purposes

No setup required - ready to use after cloning the repository.

## Dataset 2: IBM AML Banking Transactions (External Download)

**Size**: 52 GB total (13 Parquet files)
**Status**: ❌ NOT included in repository (exceeds GitHub limits)
**Source**: [IBM AMLSim on Kaggle](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml)
**License**: Community Data License Agreement - Sharing - Version 1.0

### Why External?

These files are too large for GitHub:
- Individual files: 144 MB to 5.1 GB each
- Total size: 52 GB
- GitHub limit: 100 MB per file

### File Breakdown:

**Transaction Files** (for validation testing):
```
HI-Large_Trans.parquet      5.1 GB   179M rows   High illicit activity
LI-Large_Trans.parquet      5.0 GB   177M rows   Low illicit activity
HI-Medium_Trans.parquet     932 MB   33M rows    Medium dataset
LI-Medium_Trans.parquet     909 MB   32M rows    Medium dataset
HI-Small_Trans.parquet      144 MB   5.1M rows   Quick tests
LI-Small_Trans.parquet      200 MB   7.0M rows   Quick tests
```

**Account Files** (for cross-file validation):
```
HI-Large_accounts.parquet    67 MB
LI-Large_accounts.parquet    65 MB
HI-Medium_accounts.parquet   66 MB
LI-Medium_accounts.parquet   64 MB
HI-Small_accounts.parquet    14 MB
LI-Small_accounts.parquet    20 MB
```

**Pattern Files** (money laundering patterns):
```
HI-Large_patterns.parquet
LI-Large_patterns.parquet
(etc.)
```

### Setup Instructions

#### Option 1: Kaggle CLI (Recommended)

1. **Install Kaggle CLI**:
   ```bash
   pip3 install kaggle
   ```

2. **Setup Kaggle credentials**:
   - Go to https://www.kaggle.com/settings
   - Click "Create New API Token"
   - Save `kaggle.json` to `~/.kaggle/kaggle.json`
   - Set permissions: `chmod 600 ~/.kaggle/kaggle.json`

3. **Download and extract**:
   ```bash
   # Navigate to test data directory
   cd /path/to/your/project/../test-data/

   # Download dataset (6.5 GB compressed, 52 GB uncompressed)
   kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml

   # Extract
   unzip ibm-transactions-for-anti-money-laundering-aml.zip

   # Clean up
   rm ibm-transactions-for-anti-money-laundering-aml.zip
   ```

4. **Verify installation**:
   ```bash
   ls -lh *.parquet | awk '{print $5, $9}'
   ```

   You should see 13 Parquet files totaling ~52 GB.

#### Option 2: Manual Download

1. Visit: https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
2. Click "Download" (requires Kaggle account)
3. Extract to `/path/to/project/../test-data/`

#### Option 3: Use Smaller Datasets Only

If you don't need large-scale testing, you can:
1. Download only the Small files (344 MB total)
2. Use the included e-commerce dataset (17.5 MB)
3. Skip ultimate stress tests

### Dataset Schema

**Transaction Files** (`*_Trans.parquet`):
- `Timestamp` - Transaction timestamp
- `From Bank` / `To Bank` - Bank identifiers
- `Account` / `Account.1` - From/To account IDs
- `Amount Received` / `Amount Paid` - Transaction amounts
- `Receiving Currency` / `Payment Currency` - Currency codes
- `Payment Format` - WIRE, CREDIT, DEPOSIT, CHECK, etc.
- `Is Laundering` - Boolean flag (ground truth for fraud detection)

**Account Files** (`*_accounts.parquet`):
- Account metadata and balance information

**Pattern Files** (`*_patterns.parquet`):
- Money laundering pattern descriptions

### Configuration Files

DataK9 includes pre-configured YAML files for testing with these datasets:

**For Small Datasets** (5-7 million rows):
- `test-data/configs/hi_small_test_config.yaml`
- `test-data/configs/li_small_test_config.yaml`

**For Medium Datasets** (32-33 million rows):
- `test-data/configs/hi_medium_test_config.yaml`

**For Large Datasets** (179 million rows):
- `test-data/configs/comprehensive_large_test_config.yaml`

**Ultimate Stress Test** (357 million rows, ALL validations):
- `test-data/configs/ultimate_validation_showcase.yaml`

### Quick Start Examples

**Profile a dataset**:
```bash
python3 -m validation_framework.cli profile \
  ../test-data/HI-Small_Trans.parquet \
  -o hi_small_profile.html
```

**Run validation**:
```bash
python3 -m validation_framework.cli validate \
  test-data/configs/hi_small_test_config.yaml \
  -o report.html
```

**Ultimate stress test** (requires Large files):
```bash
./ultimate_demo.sh
```

### Storage Requirements

| Dataset Size | Disk Space | RAM Required | Test Time |
|--------------|------------|--------------|-----------|
| Small        | 344 MB     | 500 MB       | 30 sec    |
| Medium       | 2 GB       | 700 MB       | 2 min     |
| Large        | 10 GB      | 900 MB       | 5 min     |
| Ultimate     | 52 GB      | 1 GB         | 15 min    |

### Performance Metrics

**Chunked Processing** (50,000 rows/chunk):
- **Small dataset**: ~170,000 rows/sec
- **Medium dataset**: ~400,000 rows/sec
- **Large dataset**: ~600,000 rows/sec

**Memory Efficiency**:
- Processing 179M rows: ~900 MB RAM
- Processing 357M rows: ~1 GB RAM

## Troubleshooting

**"File not found" errors**:
- Check that files are in `../test-data/` relative to project root
- Verify Parquet file extensions (not `.csv`)

**Out of memory errors**:
- Reduce `chunk_size` in config (default: 50000)
- Use smaller test files first
- Close other applications

**Slow performance**:
- Prefer Parquet over CSV (10x faster)
- Use SSD storage for large files
- Check disk I/O with `iostat -x 1`

**Kaggle API issues**:
- Verify `~/.kaggle/kaggle.json` exists
- Check permissions: `chmod 600 ~/.kaggle/kaggle.json`
- Ensure Kaggle account is verified

## Notes

- **Git Ignored**: The `../test-data/` directory is excluded from version control (`.gitignore`)
- **License**: IBM AML data is under Community Data License Agreement
- **Attribution**: Dataset created by IBM Research for anti-money laundering research
- **Updates**: Dataset structure may change - refer to Kaggle page for latest info

## Support

For issues with:
- **DataK9 framework**: Open issue at https://github.com/yourusername/data-validation-tool
- **Kaggle dataset**: Visit dataset page or contact dataset owner
- **Download issues**: Check Kaggle API documentation

---

**Last Updated**: 2025-11-17
**DataK9 Version**: 1.0
