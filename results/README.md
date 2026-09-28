# Experiment results

This is the central comparison index. Each experiment gets one declared change,
a matching workload on both sides, correctness checks, repeated measurements,
and an explanation of what the evidence means. A change may help, hurt, or make
no measurable difference.

| Experiment | Change and workload | Status | Evidence |
| --- | --- | --- | --- |
| 00 — Baseline | Full three-file Bronze/Silver/Gold rebuild, default SQL settings | Complete: one recorded reference run; 33.769 s processing, 925.72 MB tables | [Baseline report](00_baseline/README.md) |
| Comparison runner control | Baseline versus itself, no optimization | Passed: 15 tests, four full-data runs, 24 exact table comparisons | [Control results](comparison_control/README.md) |
| 01 — CSV versus Parquet | Compare file formats with matching records and work | Planned; not implemented or measured | [Experiment](../experiments/01_csv_vs_parquet/README.md) |
| 02 — Compression | Snappy versus ZSTD; same full pipeline and logical model | Complete: 31.85% smaller tables; no clear runtime gain; 16 tests and 40 exact comparisons passed | [Results](02_compression/README.md) |
| 03 — Date partitioning | Partition Silver impressions by date; keep ZSTD; rebuild and filtered/full reads | Complete: faster measured date-filtered reads; 7.99% slower rebuild; 14.86% smaller tables | [Results](03_date_partitioning/README.md) |

## How to read comparisons

- **Correctness first:** compare actual table contents, schemas, duplicates, and
  nulls. Equal overall counts alone do not establish equal reporting results.
- **Main outcomes:** measured workload time and output storage. Pipeline totals,
  validation time, task input bytes, shuffle, spills, and file counts explain costs.
- **Variation matters:** use repeated samples, median, and range. A single run is
  a reference observation, not a reliable improvement claim.
- **Workloads must match:** an isolated query improvement is not automatically an
  improvement to the whole pipeline. The original single-run baseline is retained
  but new comparisons rerun both sides under the same measurement procedure.
- **Mechanism matters:** inspect plans/event logs to establish what Spark did;
  the shared runner does not invent an explanation from a faster number.

See [the comparison procedure](../docs/comparisons.md) for commands, methodology,
the experiment entrypoint contract, and what is automated.
