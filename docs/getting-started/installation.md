# Installing DataK9

This guide covers installing **DataK9** - your K9 guardian for data quality.

---

## System Requirements

### Minimum Requirements
- **Python:** 3.8 or higher
- **RAM:** 2GB minimum (4GB+ recommended for large files)
- **Disk Space:** 500MB for installation
- **Operating System:** Linux, macOS, or Windows

### Recommended for Large Files (200GB+)
- **Python:** 3.10 or higher
- **RAM:** 8GB+ recommended
- **Disk Space:** Adequate space for data files and reports
- **Format:** Use Parquet for best performance

---

## Installation Methods

### Method 1: Install from Source (Recommended)

**Step 1:** Clone the repository

```bash
git clone https://github.com/danieledge/data-validation-tool.git
cd data-validation-tool
```

**Step 2:** Install dependencies

```bash
pip install -r requirements.txt
```

**Step 3:** Install DataK9

```bash
pip install -e .
```

**Step 4:** Verify installation

```bash
python3 -m validation_framework.cli --version
```

You should see:
```
DataK9 Data Quality Framework v2.3.0
```

---

### Method 2: Install in Virtual Environment (Recommended for Production)

**Step 1:** Create a virtual environment

```bash
python3 -m venv datak9-env
source datak9-env/bin/activate  # On Windows: datak9-env\Scripts\activate
```

**Step 2:** Clone and install

```bash
git clone https://github.com/danieledge/data-validation-tool.git
cd data-validation-tool
pip install -r requirements.txt
pip install -e .
```

**Step 3:** Verify installation

```bash
python3 -m validation_framework.cli --version
```

---

### Method 3: Docker Installation (Coming Soon)

Docker support is planned for future releases.

---

## Dependencies

DataK9 requires the following Python packages (automatically installed):

### Core Dependencies
- **pandas** (>= 1.3.0) - Data processing
- **pyyaml** (>= 5.4) - Configuration parsing
- **click** (>= 8.0) - CLI interface
- **jinja2** (>= 3.0) - HTML report generation

### Optional Dependencies
- **pyarrow** (>= 6.0) - Parquet file support (recommended)
- **openpyxl** (>= 3.0) - Excel file support
- **sqlalchemy** (>= 1.4) - Database validation support
- **psycopg2-binary** (>= 2.9) - PostgreSQL support
- **pymysql** (>= 1.0) - MySQL support

### Development Dependencies
- **pytest** (>= 7.0) - Testing framework
- **black** (>= 22.0) - Code formatting
- **flake8** (>= 4.0) - Linting

---

## Verifying Your Installation

### Check Version

```bash
python3 -m validation_framework.cli --version
```

### List Available Validations

```bash
python3 -m validation_framework.cli list-validations
```

You should see a list of 35+ validation types.

### Run Sample Validation

Create a test file `test.yaml`:

```yaml
validation_job:
  name: "Installation Test"

files:
  - name: "test"
    path: "test.csv"
    format: "csv"
    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"
```

Run it:

```bash
# Create a test CSV
echo "id,name\n1,test" > test.csv

# Run validation
python3 -m validation_framework.cli validate test.yaml
```

If you see ✓ PASS, DataK9 is installed correctly!

---

## Optional: Install Database Drivers

For database validations, install the appropriate driver:

### PostgreSQL

```bash
pip install psycopg2-binary
```

### MySQL

```bash
pip install pymysql
```

### SQL Server

```bash
pip install pyodbc
```

### Oracle

```bash
pip install cx_Oracle
```

---

## Optional: Install Parquet Support

For 10x faster processing of large files:

```bash
pip install pyarrow
```

**Highly recommended** for files over 1GB!

---

## Upgrading DataK9

To upgrade to the latest version:

```bash
cd data-validation-tool
git pull origin main
pip install -r requirements.txt --upgrade
```

---

## Troubleshooting Installation

### "pip: command not found"

Install pip:

```bash
# On Ubuntu/Debian
sudo apt-get install python3-pip

# On macOS
brew install python3

# On Windows
# Download from python.org
```

### "Permission denied" errors

Use `--user` flag:

```bash
pip install --user -r requirements.txt
```

Or use a virtual environment (recommended).

### "ModuleNotFoundError" after installation

Make sure you're in the correct directory and activated your virtual environment (if using one).

### Pandas installation fails

On some systems, you may need to install build dependencies:

```bash
# Ubuntu/Debian
sudo apt-get install python3-dev build-essential

# macOS
xcode-select --install
```

### PyArrow (Parquet) installation issues

Try installing a specific version:

```bash
pip install pyarrow==10.0.0
```

---

## Next Steps

Now that DataK9 is installed:

1. **[5-Minute Quickstart](quickstart-5min.md)** - Create your first validation
2. **[What is DataK9?](../using-datak9/what-is-datak9.md)** - Understand the capabilities
3. **[Configuration Guide](../using-datak9/configuration-guide.md)** - Learn YAML configuration
4. **[DataK9 Studio](../using-datak9/studio-guide.md)** - Use the visual IDE

---

## Uninstalling DataK9

If you need to uninstall:

```bash
# If installed with -e flag
cd data-validation-tool
pip uninstall validation-framework

# If using virtual environment
deactivate
rm -rf datak9-env
```

---

**🐕 DataK9 is ready to guard your data quality!**
