<div align="center">
  <img src="../resources/images/datak9-web.png" alt="DataK9 Logo" width="300">

  # DataK9 Documentation
  ## 🐕 Your K9 guardian for data quality
</div>

Welcome to **DataK9** - a data quality framework that guards your data pipelines with vigilance and precision. Like a K9 unit sniffing out problems before they escalate, DataK9 catches data quality issues before they cause problems.

---

## 📖 Documentation Index

### 🚀 Getting Started (Start Here!)

**New to DataK9?** Start with these essentials:

1. **[5-Minute Quickstart](getting-started/quickstart-5min.md)** - Get running in 5 minutes
2. **[Installation Guide](getting-started/installation.md)** - Complete setup instructions
3. **[What is DataK9?](using-datak9/what-is-datak9.md)** - Overview and key concepts

**Quick Reference Guides:**
- 📁 **[File Validation Quick Start](reference/quick-reference/FILE_QUICKSTART.md)** - CSV, Excel, JSON, Parquet
- 🗄️ **[Database Validation Quick Start](guides/database/DATABASE_QUICKSTART.md)** - PostgreSQL, MySQL, SQL Server, Oracle, SQLite

---

## 📚 Core Documentation

### Using DataK9

**Configuration & Setup:**
- [Configuration Guide](using-datak9/configuration-guide.md) - YAML syntax and options
- [DataK9 Studio Guide](using-datak9/studio-guide.md) - Visual IDE interface
- [Best Practices](using-datak9/best-practices.md) - Recommended patterns

**Data Analysis & Validation:**
- [Data Profiling](using-datak9/data-profiling.md) - Analyze data quality
- [Validation Catalog](using-datak9/validation-catalog.md) - All 35 validation types
- [Reading Reports](using-datak9/reading-reports.md) - Understand validation results

**Integration & Deployment:**
- [AutoSys Integration](using-datak9/autosys-integration.md) - Job scheduling
- [CI/CD Integration](using-datak9/cicd-integration.md) - GitHub Actions, GitLab, Jenkins
- [Large Files Guide](using-datak9/large-files.md) - Handle 200GB+ datasets

**Support:**
- [FAQ](using-datak9/faq.md) - Frequently asked questions
- [Troubleshooting](using-datak9/troubleshooting.md) - Common issues and solutions

→ **[Using DataK9 Guide](using-datak9/README.md)** (Complete index)

---

### 🎯 Specialized Guides

#### Performance Optimization
Maximize speed and minimize memory usage:

- **[Performance Optimization Guide](guides/performance/PERFORMANCE_OPTIMIZATION_GUIDE.md)** - 8-20x speedup strategies
- **[Polars Backend Guide](guides/performance/POLARS_BACKEND_GUIDE.md)** - High-performance backend ⚡
- **[Chunk Size Guide](guides/performance/CHUNK_SIZE_GUIDE.md)** - Memory-efficient processing
- **[Sampling Quick Reference](guides/performance/SAMPLING_QUICK_REFERENCE.md)** - Smart sampling for large datasets

#### Database Validation
Validate data directly from databases:

- **[Database Quick Start](guides/database/DATABASE_QUICKSTART.md)** - Get started in 3 minutes
- **[Database Validation Guide](guides/database/DATABASE_VALIDATION_GUIDE.md)** - Complete guide
- **[Database Safety](guides/database/DATABASE_SAFETY.md)** - Production safety features 🛡️
- **[Database Credentials Security](guides/database/DATABASE_CREDENTIALS_SECURITY.md)** - Secure credential management ⚠️

#### Advanced Features
Power user features and complex scenarios:

- **[Understanding CDAs](guides/advanced/UNDERSTANDING_CDAS.md)** - What are Critical Data Attributes and why track them?
- **[CDA Gap Analysis Guide](guides/advanced/CDA_GAP_ANALYSIS_GUIDE.md)** - Technical guide for CDA gap detection
- **[Cross-File Validation Quick Reference](guides/advanced/CROSS_FILE_VALIDATION_QUICK_REFERENCE.md)** - Validate across multiple files
- **[Profiler Enhanced Features](guides/advanced/PROFILER_ENHANCED_FEATURES.md)** - Advanced profiling capabilities

---

### 📖 Reference Documentation

#### Quick References
- **[File Quick Start](reference/quick-reference/FILE_QUICKSTART.md)** - CSV, Excel, JSON, Parquet validation
- **[Validation Compatibility Matrix](reference/VALIDATION_COMPATIBILITY.md)** - Which validations work where

#### Complete References
- **[CLI Reference](reference/cli-reference.md)** - All command-line options
- **[Validation Reference](reference/validation-reference.md)** - Complete validation catalog (35 types)
- **[YAML Reference](reference/yaml-reference.md)** - Configuration file syntax
- **[Error Codes](reference/error-codes.md)** - Error messages and solutions
- **[Glossary](reference/glossary.md)** - Terminology and definitions

---

### 💻 For Developers

**Architecture & Design:**
- **[Architecture Overview](for-developers/architecture.md)** - System design and patterns
- **[Design Patterns](for-developers/design-patterns.md)** - Factory, Registry, Strategy patterns
- **[API Reference](for-developers/api-reference.md)** - Python API documentation

**Extending DataK9:**
- **[Custom Validations](for-developers/custom-validations.md)** - Build your own validation types
- **[Custom Loaders](for-developers/custom-loaders.md)** - Add new data sources
- **[Custom Reporters](for-developers/custom-reporters.md)** - Create custom reports

**Contributing:**
- **[Testing Guide](for-developers/testing-guide.md)** - Write and run tests
- **[Contributing Guide](for-developers/contributing.md)** - Contribution guidelines

→ **[Developer Guide](for-developers/README.md)** (Complete index)

---

### 🏢 Industry Examples

Real-world validation configurations:

- **[Finance](examples/finance.md)** - Banking, trading, AML validation
- **[Healthcare](examples/healthcare.md)** - HIPAA-compliant patient data
- **[E-Commerce](examples/ecommerce.md)** - Customer, order, inventory validation

→ **[Examples Index](examples/README.md)**

---

## 🔍 Quick Navigation by Task

| I want to... | Go here |
|-------------|---------|
| **Get started** |
| Install DataK9 | [Installation Guide](getting-started/installation.md) |
| Learn the basics | [5-Minute Quickstart](getting-started/quickstart-5min.md) |
| Understand concepts | [What is DataK9?](using-datak9/what-is-datak9.md) |
| **Validate files** |
| Validate CSV/Excel/JSON/Parquet | [File Quick Start](reference/quick-reference/FILE_QUICKSTART.md) 📁 |
| Handle large files (200GB+) | [Large Files Guide](using-datak9/large-files.md) |
| Optimize performance | [Performance Guide](guides/performance/PERFORMANCE_OPTIMIZATION_GUIDE.md) |
| **Validate databases** |
| Quick start with databases | [Database Quick Start](guides/database/DATABASE_QUICKSTART.md) 🗄️ |
| Production database safety | [Database Safety](guides/database/DATABASE_SAFETY.md) 🛡️ |
| Secure credentials | [Credentials Security](guides/database/DATABASE_CREDENTIALS_SECURITY.md) |
| **Build validations** |
| Write YAML configs | [Configuration Guide](using-datak9/configuration-guide.md) |
| Use visual IDE | [DataK9 Studio Guide](using-datak9/studio-guide.md) |
| See all validation types | [Validation Reference](reference/validation-reference.md) |
| **Analyze data** |
| Profile data quality | [Data Profiling](using-datak9/data-profiling.md) |
| Auto-generate configs | [Data Profiling](using-datak9/data-profiling.md#auto-generate) |
| **Integration** |
| AutoSys jobs | [AutoSys Integration](using-datak9/autosys-integration.md) |
| CI/CD pipelines | [CI/CD Integration](using-datak9/cicd-integration.md) |
| **Troubleshooting** |
| Common issues | [Troubleshooting](using-datak9/troubleshooting.md) |
| Error messages | [Error Codes](reference/error-codes.md) |
| FAQ | [FAQ](using-datak9/faq.md) |
| **Advanced** |
| Understand Critical Data Attributes | [Understanding CDAs](guides/advanced/UNDERSTANDING_CDAS.md) |
| Track CDA validation coverage | [CDA Gap Analysis](guides/advanced/CDA_GAP_ANALYSIS_GUIDE.md) |
| Cross-file validation | [Cross-File Quick Reference](guides/advanced/CROSS_FILE_VALIDATION_QUICK_REFERENCE.md) |
| Custom validations | [Custom Validations](for-developers/custom-validations.md) |

---

## 🗺️ Documentation Structure

```
docs/
├── README.md                    ← You are here
│
├── getting-started/             ← Start here (New users)
│   ├── quickstart-5min.md
│   └── installation.md
│
├── using-datak9/                ← User guides (Most users)
│   ├── README.md
│   ├── what-is-datak9.md
│   ├── configuration-guide.md
│   ├── studio-guide.md
│   ├── data-profiling.md
│   ├── validation-catalog.md
│   ├── best-practices.md
│   ├── reading-reports.md
│   ├── large-files.md
│   ├── performance-tuning.md
│   ├── autosys-integration.md
│   ├── cicd-integration.md
│   ├── troubleshooting.md
│   └── faq.md
│
├── guides/                      ← Specialized guides
│   ├── performance/             ← Performance optimization
│   │   ├── PERFORMANCE_OPTIMIZATION_GUIDE.md
│   │   ├── POLARS_BACKEND_GUIDE.md
│   │   ├── CHUNK_SIZE_GUIDE.md
│   │   └── SAMPLING_QUICK_REFERENCE.md
│   │
│   ├── database/                ← Database validation
│   │   ├── DATABASE_QUICKSTART.md
│   │   ├── DATABASE_VALIDATION_GUIDE.md
│   │   ├── DATABASE_SAFETY.md
│   │   └── DATABASE_CREDENTIALS_SECURITY.md
│   │
│   └── advanced/                ← Advanced features
│       ├── CROSS_FILE_VALIDATION_QUICK_REFERENCE.md
│       ├── CDA_GAP_ANALYSIS_GUIDE.md
│       └── PROFILER_ENHANCED_FEATURES.md
│
├── reference/                   ← Reference documentation
│   ├── quick-reference/
│   │   └── FILE_QUICKSTART.md
│   ├── cli-reference.md
│   ├── validation-reference.md
│   ├── yaml-reference.md
│   ├── error-codes.md
│   ├── glossary.md
│   └── VALIDATION_COMPATIBILITY.md
│
├── for-developers/              ← Developer documentation
│   ├── README.md
│   ├── architecture.md
│   ├── design-patterns.md
│   ├── api-reference.md
│   ├── custom-validations.md
│   ├── custom-loaders.md
│   ├── custom-reporters.md
│   ├── testing-guide.md
│   └── contributing.md
│
└── examples/                    ← Industry examples
    ├── README.md
    ├── finance.md
    ├── healthcare.md
    └── ecommerce.md
```

---

## 🚦 Recommended Reading Paths

### Path 1: New User (Getting Started)
1. [5-Minute Quickstart](getting-started/quickstart-5min.md)
2. [File Quick Start](reference/quick-reference/FILE_QUICKSTART.md) or [Database Quick Start](guides/database/DATABASE_QUICKSTART.md)
3. [Configuration Guide](using-datak9/configuration-guide.md)
4. [Validation Reference](reference/validation-reference.md)

### Path 2: Performance Optimization
1. [Performance Optimization Guide](guides/performance/PERFORMANCE_OPTIMIZATION_GUIDE.md)
2. [Polars Backend Guide](guides/performance/POLARS_BACKEND_GUIDE.md)
3. [Chunk Size Guide](guides/performance/CHUNK_SIZE_GUIDE.md)
4. [Large Files Guide](using-datak9/large-files.md)

### Path 3: Database Validation
1. [Database Quick Start](guides/database/DATABASE_QUICKSTART.md)
2. [Database Safety](guides/database/DATABASE_SAFETY.md)
3. [Database Credentials Security](guides/database/DATABASE_CREDENTIALS_SECURITY.md)
4. [Validation Compatibility Matrix](reference/VALIDATION_COMPATIBILITY.md)

### Path 4: Developer/Contributor
1. [Architecture Overview](for-developers/architecture.md)
2. [Design Patterns](for-developers/design-patterns.md)
3. [Custom Validations](for-developers/custom-validations.md)
4. [Testing Guide](for-developers/testing-guide.md)
5. [Contributing Guide](for-developers/contributing.md)

---

## 💡 Quick Tips

**First time?** → Start with [5-Minute Quickstart](getting-started/quickstart-5min.md)

**Validating files?** → See [File Quick Start](reference/quick-reference/FILE_QUICKSTART.md)

**Validating databases?** → See [Database Quick Start](guides/database/DATABASE_QUICKSTART.md)

**Need speed?** → Read [Performance Optimization Guide](guides/performance/PERFORMANCE_OPTIMIZATION_GUIDE.md)

**Building custom validations?** → Check [Custom Validations](for-developers/custom-validations.md)

**Stuck?** → Try [Troubleshooting](using-datak9/troubleshooting.md) or [FAQ](using-datak9/faq.md)

---

## 🆘 Need Help?

- **[FAQ](using-datak9/faq.md)** - Frequently asked questions
- **[Troubleshooting](using-datak9/troubleshooting.md)** - Common issues and solutions
- **[Error Codes](reference/error-codes.md)** - Error messages explained
- **[Examples](examples/)** - Real-world configurations
- **[GitHub Issues](https://github.com/danieledge/data-validation-tool/issues)** - Report bugs or request features

---

## 🐕 What is DataK9?

DataK9 is a production-grade data quality framework that:

- ✅ **Validates both files and databases** - CSV, Excel, JSON, Parquet, PostgreSQL, MySQL, SQL Server, Oracle, SQLite
- ✅ **Handles massive datasets** - 200GB+ files with memory-efficient processing
- ✅ **35 built-in validations** - File, Schema, Field, Record, Advanced, Cross-File, Database, Temporal, Statistical
- ✅ **Visual IDE** - DataK9 Studio for point-and-click configuration
- ✅ **High performance** - Polars backend for 5-10x faster processing
- ✅ **Enterprise-ready** - AutoSys/CI/CD integration, proper exit codes, JSON output
- ✅ **Production tested** - 115+ tests, 48% coverage, validated on 357M row datasets

---

**🐕 Guard your data pipelines with DataK9 - Your K9 guardian for data quality**

---

**Copyright © 2025 Daniel Edge**
**License:** MIT
**Repository:** https://github.com/danieledge/data-validation-tool
