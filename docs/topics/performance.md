# Performance Documentation Index

All documentation related to DataK9 performance optimization.

---

## Quick Start

- [Performance Tuning](../using-datak9/performance-tuning.md) - Essential performance tips
- [Large Files Guide](../using-datak9/large-files.md) - Handling 200GB+ files

## Detailed Guides

- [Performance Optimization Guide](../guides/performance/PERFORMANCE_OPTIMIZATION_GUIDE.md) - Comprehensive optimization strategies
- [Polars Backend Guide](../guides/performance/POLARS_BACKEND_GUIDE.md) - High-performance Polars engine
- [Chunk Size Guide](../guides/performance/CHUNK_SIZE_GUIDE.md) - Memory-efficient chunking

## Profiler Performance

- [Profiler Memory Optimization](../guides/performance/profiler-memory-optimization.md) - Memory-efficient profiling
- [Sampling Quick Reference](../guides/performance/SAMPLING_QUICK_REFERENCE.md) - Smart sampling strategies

## Reference

- [CLI Reference - Performance Options](../reference/cli-reference.md) - `--chunk-size`, `--backend` flags
- [Benchmarks](../benchmark.md) - Performance test results

---

## Performance Quick Tips

| Tip | Impact |
|-----|--------|
| Use Parquet format | 10x faster than CSV |
| Polars backend (default) | 5-10x faster than pandas |
| Increase chunk size | Faster processing (more memory) |
| Reduce `max_sample_failures` | Lower memory usage |

**Quick Command:**
```bash
python3 -m validation_framework.cli validate config.yaml --chunk-size 100000
```
