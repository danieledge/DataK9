# Profiler Documentation Index

All documentation related to the DataK9 Profiler.

---

## Getting Started

- [Data Profiling Guide](../using-datak9/data-profiling.md) - Main profiler user guide
- [5-Minute Quickstart](../getting-started/quickstart-5min.md) - Includes profiler basics

## Reference

- [CLI Reference - Profile Command](../reference/cli-reference.md) - All profiler CLI options
- [Profiler Internals](../reference/profiler-internals.md) - Technical architecture

## Advanced Features

- [ML Analysis Guide](../guides/advanced/PROFILER_ML_ANALYSIS.md) - Machine learning features
- [Enhanced Features](../guides/advanced/PROFILER_ENHANCED_FEATURES.md) - Advanced profiler capabilities
- [Intelligent Sampling](../guides/advanced/intelligent-sampling-statistical-basis.md) - Statistical sampling methods

## Performance

- [Profiler Memory Optimization](../guides/performance/profiler-memory-optimization.md) - Memory-efficient profiling
- [Sampling Quick Reference](../guides/performance/SAMPLING_QUICK_REFERENCE.md) - Sampling strategies
- [Large Files Guide](../using-datak9/large-files.md) - Profiling 200GB+ files

## Output & Reports

- [Reading Reports](../using-datak9/reading-reports.md) - Understanding profiler output
- [Sample Reports](../samples/) - Example profiler HTML reports

---

**Quick Command:**
```bash
python3 -m validation_framework.cli profile data.csv -o report.html --beta-ml
```
