<div align="center">
  <img src="resources/images/datak9-web.png" alt="DataK9" width="180">

  # DataK9

  **Data validation for files and databases**

  [![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
  [![Status: Beta](https://img.shields.io/badge/status-beta-orange.svg)](#status)
</div>

Like a K9 unit sniffing out problems before they escalate, DataK9 catches data quality issues before they cause damage.

DataK9 validates CSV, Excel, Parquet, JSON files and PostgreSQL, MySQL, SQL Server, Oracle databases. 37 built-in validations catch schema issues, missing data, duplicates, referential integrity problems, and statistical anomalies.

## Install

```bash
git clone https://github.com/danieledge/datak9.git
cd datak9/data-validation-tool
pip install -r requirements.txt
```

## Quick Start

**Profile any data file:**
```bash
python3 -m validation_framework.cli profile data.csv -o report.html
```

**Run validations:**
```bash
python3 -m validation_framework.cli validate config.yaml
```

## Features

- **36 validations** - Schema, field, record, cross-file, database, statistical
- **200GB+ files** - Memory-efficient chunked processing
- **Visual IDE** - DataK9 Studio, no coding required
- **CI/CD ready** - Exit codes, JSON output, AutoSys support

## Documentation

- [5-Minute Quickstart](docs/getting-started/quickstart-5min.md)
- [Installation Guide](docs/getting-started/installation.md)
- [CLI Reference](docs/reference/cli-reference.md)
- [Validation Reference](docs/reference/validation-reference.md)
- [Full Documentation](docs/README.md)

## Status

Beta software. Test thoroughly before production use.
[Report issues](https://github.com/danieledge/datak9/issues)

## License

MIT - free for commercial use.
