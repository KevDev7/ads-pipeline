# Experiment results

This is the central comparison index. Each experiment gets one declared change,
a matching workload on both sides, correctness checks, repeated measurements,
and an explanation of what the evidence means. A change may help, hurt, or make
no measurable difference.

| Experiment | Change and workload | Status | Evidence |
| --- | --- | --- | --- |
| 00 — Baseline | Full three-file Bronze/Silver/Gold rebuild, default SQL settings | Complete: one recorded reference run; 33.769 s processing, 925.72 MB tables | [Baseline report](00_baseline/README.md) |
| Comparison runner control | Baseline versus itself, no optimization | Passed: 15 tests, four full-data runs, 24 exact table comparisons | [Control results](comparison_control/README.md) |
| 01 — CSV versus Parquet | Identical typed impressions; uncompressed formats; three read workloads | Complete: 58.18% fewer data bytes; 3.89–8.44× faster median reads in tested workloads | [Results](01_csv_vs_parquet/README.md) |
| 02 — Compression | Snappy versus ZSTD; same full pipeline and logical model | Complete: 31.85% smaller tables; no clear runtime gain; 16 tests and 40 exact comparisons passed | [Results](02_compression/README.md) |
| 03 — Date partitioning | Partition Silver impressions by date; keep ZSTD; rebuild and filtered/full reads | Complete: faster measured date-filtered reads; 7.99% slower rebuild; 14.86% smaller tables | [Results](03_date_partitioning/README.md) |
| 04 — Ads join | Iteration 2 versus ads sort-merge join; profile join unchanged | Complete: Gold median 7.947 → 14.871 s, 5.02× shuffle writes; 24 exact comparisons passed; two repeats per side | [Results](04_ads_shuffle_join/README.md) |
| 05 — Profile join | Iteration 2 versus profile sort-merge join; ads join unchanged | Complete with failure history: Gold median 6.591 → 17.302 s; processing +33.05%; one earlier memory failure; fresh series passed 40 exact comparisons | [Results](05_profile_shuffle_join/README.md) |
| 06 — Shuffle partitions | Iteration 2 versus 32 initial SQL shuffle partitions; AQE unchanged | Complete: Gold median -6.71%; overall timing mixed; AQE readers 4 → 5; 40 exact comparisons passed | [Results](06_shuffle_partitions/README.md) |
| 07 — Cached enrichment | Iteration 2 versus persisting full enrichment for both reports | Complete: reuse verified, Gold +132.45%, processing +22.32%; keep iteration 2; 40 exact comparisons passed | [Results](07_cached_enrichment/README.md) |
| 08 — Shuffle coalescing | Iteration 2 versus disabling automatic shuffle-partition combining | Complete: Gold +27.96%, processing +8.90%; 4 → 200 readers/files per Gold report; 40 exact comparisons passed | [Results](08_no_shuffle_coalescing/README.md) |
| 09 — Python date UDF | Iteration 2 built-in date versus regular Python UDF | Complete: Silver +43.25%, processing +6.56%; same storage; 40 exact comparisons and 228 ZSTD footer checks passed | [Results](09_python_date_udf/README.md) |

## Reproduce an experiment

Run `bash scripts/test_iteration.sh 1` for CSV versus Parquet,
`bash scripts/test_iteration.sh 2` for compression, or
`bash scripts/test_iteration.sh 3` for date partitioning, or
`bash scripts/test_iteration.sh 4` for the ads join, or
`bash scripts/test_iteration.sh 5` for the profile join, or
`bash scripts/test_iteration.sh 6` for initial shuffle partitions, or
`bash scripts/test_iteration.sh 7` for cached enrichment, or
`bash scripts/test_iteration.sh 8` for shuffle coalescing, or
`bash scripts/test_iteration.sh 9` for the Python date UDF. Each creates a fresh
comparison and a `RESULTS.md` linking all applicable automated checks. See
[the command options and manual interpretation boundary](../docs/comparisons.md#one-command-per-implemented-iteration).

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

## Generated-table retention

Old benchmark table copies may be removed while their implementations and evidence
remain available. On 2026-09-29, generated tables from iterations 6–8 were removed,
reclaiming about 10.56 GiB; iteration 9 outputs and the original baseline were kept.
See the [retention history and exact cleanup manifest](retention/README.md).
Historical reports describe the outputs at measurement time; this retention log
records which generated tables remain on disk afterward.
