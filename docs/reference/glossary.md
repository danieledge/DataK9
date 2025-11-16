# DataK9 Glossary

Definitions of terms used in **DataK9** - your K9 guardian for data quality.

---

## Core Concepts

### DataK9
The complete data quality framework. "DataK9" refers to the entire system including the CLI, Studio, Profiler, and all validation capabilities.

### K9 (Canine)
The metaphor behind DataK9. Like security K9 units, DataK9 "sniffs out" data quality problems before they escalate.

---

## Components

### DataK9 CLI
The command-line interface for running validations.
```bash
python3 -m validation_framework.cli validate config.yaml
```

### DataK9 Studio
The visual IDE for building validation configurations without coding. Browser-based application with drag-and-drop interface.

### DataK9 Profiler
The data analysis component that examines data files and auto-generates validation configurations.

---

## Configuration Terms

### YAML
"YAML Ain't Markup Language" - the configuration file format used by DataK9. Human-readable data serialization format.

### Validation Job
A complete validation workflow defined in a YAML file. Contains settings, files, and validation rules.

### Chunk
A portion of data loaded into memory at once for processing. Default size is 50,000 rows. Enables processing of files larger than available RAM.

### Chunk Size
Number of rows processed together in memory. Configurable via `chunk_size` setting. Larger chunks = faster processing but more memory usage.

---

## Validation Terms

### Validation Rule
A single check applied to data. Examples: MandatoryFieldCheck, RegexCheck, RangeCheck.

### Validation Type
The category or kind of validation rule. DataK9 has 35+ types organized in 10 categories.

### Severity
Importance level of a validation result. Two levels:
- **ERROR** - Critical issue that must be fixed
- **WARNING** - Issue that should be reviewed

### Status
Outcome of a validation execution. Three statuses:
- **PASSED** - Validation succeeded, no issues found
- **FAILED** - Validation found issues
- **WARNING** - Validation completed with warnings

---

## Data Terms

### Field
A single column in your data file. Also called "column" or "attribute".

### Record
A single row in your data file. Also called "row" or "observation".

### Schema
The structure of your data: column names, data types, and order.

### Null
Missing or empty value. In CSV files, often represented as blank cells or "NULL".

### Delimiter
Character that separates fields in CSV files. Common delimiters: comma (,), pipe (|), tab (\t).

---

## Validation Types (Categories)

### File-Level Validation
Checks properties of the entire file: row count, file size, emptiness.

Examples:
- EmptyFileCheck - Ensure file isn't empty
- RowCountRangeCheck - Verify expected row count
- FileSizeCheck - Check file size limits

### Schema Validation
Validates the structure and columns of the data.

Examples:
- SchemaMatchCheck - Verify column names and types match expected schema
- ColumnPresenceCheck - Ensure required columns exist

### Field-Level Validation
Checks individual column values.

Examples:
- MandatoryFieldCheck - Require non-null values
- RegexCheck - Pattern matching
- RangeCheck - Numeric or date ranges
- ValidValuesCheck - Whitelist/blacklist values

### Record-Level Validation
Examines entire rows or relationships between fields.

Examples:
- DuplicateRowCheck - Find duplicate records
- BlankRecordCheck - Detect empty rows
- UniqueKeyCheck - Ensure primary key uniqueness

### Advanced Validation
Sophisticated checks including statistical analysis.

Examples:
- StatisticalOutlierCheck - Detect anomalies
- CrossFieldComparisonCheck - Validate field relationships
- FreshnessCheck - Ensure data isn't stale
- CompletenessCheck - Percentage-based completeness

### Conditional Validation
Validations with if-then-else logic.

Example:
- ConditionalValidation - Apply rules based on conditions

### Cross-File Validation
Validates relationships between multiple files.

Examples:
- ReferentialIntegrityCheck - Verify foreign keys
- CrossFileComparisonCheck - Compare data across files
- CrossFileDuplicateCheck - Find duplicates across files

### Database Validation
Validates data in or against databases.

Examples:
- SQLCustomCheck - Run custom SQL queries
- DatabaseReferentialIntegrityCheck - Verify database foreign keys
- DatabaseConstraintCheck - Validate database constraints

### Temporal Validation
Time-based validation checks.

Examples:
- BaselineComparisonCheck - Compare against historical data
- TrendDetectionCheck - Identify trends and anomalies

### Statistical Validation
Advanced statistical analysis.

Examples:
- DistributionCheck - Verify data distribution
- CorrelationCheck - Check field correlations
- AdvancedAnomalyDetectionCheck - Sophisticated anomaly detection

---

## Results & Reporting

### Validation Result
The outcome of a single validation rule execution. Contains:
- Status (PASSED/FAILED/WARNING)
- Message
- Sample failures
- Statistics

### File Validation Report
Results of all validations for one data file.

### Validation Report
Complete report for an entire validation job (all files).

### Sample Failures
Example records that failed a validation. Limited by `max_sample_failures` setting (default: 100).

### HTML Report
Interactive, visual report generated by DataK9. Features dark theme, expandable sections, and charts.

### JSON Report
Machine-readable report for CI/CD integration and automated processing.

---

## Processing Terms

### Data Iterator
Python iterator that yields DataFrame chunks. Enables memory-efficient processing of large files.

### Memory-Efficient Processing
DataK9's approach to handling files larger than available RAM by processing in chunks.

### Streaming
Reading and processing data incrementally rather than loading entire file into memory.

---

## File Formats

### CSV (Comma-Separated Values)
Plain text file format where values are separated by delimiters (typically commas).

### Excel
Microsoft Excel file formats: XLS (legacy) and XLSX (modern).

### Parquet
Columnar storage file format. Highly compressed and optimized for analytics. **10x faster than CSV for large files.**

### JSON (JavaScript Object Notation)
Text-based data interchange format. DataK9 supports standard JSON arrays and JSON Lines (JSONL).

### JSON Lines (JSONL, NDJSON)
Newline-delimited JSON format where each line is a complete JSON object.

---

## Integration Terms

### Exit Code
Numeric code returned by DataK9 CLI indicating success or failure:
- `0` = All validations passed
- `1` = Validation errors found
- `2` = Configuration or runtime error

### AutoSys
CA Workload Automation tool for batch job orchestration. DataK9 integrates via proper exit codes.

### CI/CD
Continuous Integration / Continuous Deployment. DataK9 integrates via JSON output and exit codes.

### Airflow
Apache Airflow workflow orchestration platform. DataK9 runs in Airflow via BashOperator.

---

## Advanced Concepts

### Condition Expression
SQL-like syntax for conditional logic in validations.

Examples:
```
account_type == 'BUSINESS'
age >= 18 AND age <= 120
status IN ('ACTIVE', 'PENDING')
balance > 0
```

### Regex Pattern
Regular expression for pattern matching in RegexCheck validations.

Example:
```
^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$
```

### Z-Score
Statistical measure used in StatisticalOutlierCheck. Indicates how many standard deviations a value is from the mean.

### IQR (Interquartile Range)
Statistical measure used for outlier detection. Range between 25th and 75th percentiles.

### Statistical Outlier
Data point that deviates significantly from other observations. Detected using Z-score or IQR methods.

---

## Registry & Architecture

### Validation Registry
Central catalog of all available validation types. Uses Singleton pattern for global access.

### Factory Pattern
Design pattern used by LoaderFactory to create appropriate data loaders (CSV, Excel, Parquet, JSON).

### Plugin Architecture
DataK9's extensibility model allowing custom validations to be registered and used.

### Data Loader
Component that reads data files and yields chunks. Implements standard interface for different formats.

---

## Settings

### max_sample_failures
Maximum number of failing records to store per validation. Default: 100. Reduce to save memory.

### fail_on_error
Whether to return non-zero exit code when errors are found. Default: true.

### chunk_size
Number of rows to process at once. Default: 50,000. Increase for better performance (if enough RAM).

---

## Acronyms

| Acronym | Full Term |
|---------|-----------|
| CLI | Command-Line Interface |
| CSV | Comma-Separated Values |
| YAML | YAML Ain't Markup Language |
| JSON | JavaScript Object Notation |
| JSONL | JSON Lines |
| NDJSON | Newline-Delimited JSON |
| IDE | Integrated Development Environment |
| CI/CD | Continuous Integration / Continuous Deployment |
| IQR | Interquartile Range |
| API | Application Programming Interface |
| RAM | Random Access Memory |
| GB | Gigabyte |
| MB | Megabyte |

---

## Related Documentation

- **[Configuration Guide](../using-datak9/configuration-guide.md)** - YAML configuration reference
- **[Validation Catalog](../using-datak9/validation-catalog.md)** - All validation types
- **[FAQ](../using-datak9/faq.md)** - Frequently asked questions
- **[Architecture](../for-developers/architecture.md)** - Technical architecture

---

**🐕 Guard your data with DataK9 - your K9 guardian for data quality!**
