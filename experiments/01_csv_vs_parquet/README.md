# Iteration 1: CSV versus Parquet

**Status: planned; not implemented or measured.**

Compare CSV and Parquet using the same records and equivalent processing/query
work, with correctness checks and runtime/storage measurements. The baseline
already uses Parquet; this experiment will provide a CSV comparison to explain
that format choice. Detailed benchmark design comes with implementation.

## Learning sequence

- **00 — Baseline:** original untuned Delta/Parquet batch pipeline.
- **01 — CSV versus Parquet:** file-format comparison, planned.
- **02 — Compression:** completed Snappy-versus-ZSTD experiment within Parquet.

Compression was built first and renamed from iteration 1 to iteration 2. Its
[results](../../results/02_compression/README.md) remain valid; numbering reflects
the learning sequence, not the order of implementation.
