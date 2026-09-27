# Recorded baseline: September 27, 2026

The complete three-file pipeline succeeded with **26,557,961 impressions** and
**1,366,056 clicks**. Both Gold reports preserve those totals, including
**1,528,526 impressions without a matching user profile**. Overall CTR is
5.1437%; calculate it from total clicks divided by total impressions,
not by averaging the CTR column.

This is one untuned local reference run, not an optimization comparison.
The measured code commit is `62ff48f11c23b114adefc84f494931fcf0844497` with a clean working
tree. Later documentation/result commits do not change that measured code.

## Time and storage

| Operation | Wall time |
| --- | ---: |
| CSV → Bronze | 16.000 s |
| Bronze → Silver | 10.795 s |
| Silver → Gold | 6.974 s |
| **Pipeline processing subtotal** | **33.769 s** |
| Correctness validation | 5.537 s |
| **Total, including startup, source hashing, and shutdown** | **42.910 s** |

Source CSV files: **1.143 GB**.
All eight generated Delta tables together: **925.72 MB**.
Spark event logs: **19.93 MB**, additional to table storage.
Sizes use decimal MB/GB and count file lengths, not filesystem allocated blocks.
The source files remain on disk, so source and output storage are additive.

| Table | Rows | Total table size | Parquet files |
| --- | ---: | ---: | ---: |
| `bronze/ads` | 846,811 | 15.93 MB | 4 |
| `bronze/impressions` | 26,557,961 | 340.11 MB | 9 |
| `bronze/user_profiles` | 1,061,768 | 8.90 MB | 4 |
| `gold/audience_daily` | 3,738,362 | 28.65 MB | 4 |
| `gold/campaign_daily` | 1,819,995 | 23.36 MB | 4 |
| `silver/ads` | 846,811 | 14.22 MB | 4 |
| `silver/impressions` | 26,557,961 | 486.59 MB | 5 |
| `silver/user_profiles` | 1,061,768 | 7.98 MB | 4 |

## What the measurements mean

- Hardware: Apple Silicon Mac, 16 GiB RAM; local Spark uses four worker threads
  and a 2 GiB driver heap. Python 3.11.15, Spark 4.0.1,
  Delta 4.0.0, Java 21.0.12.1.
- SQL defaults remain in place: adaptive execution enabled, 200 configured shuffle
  partitions, 10 MiB automatic broadcast threshold, and Snappy Parquet compression.
  The number of actual tasks/files can differ from 200 because Spark adapts execution.
- Processing tasks wrote **137.75 MB of shuffle data**. Validation has separate
  counters in the JSON. There were **0 disk-spill bytes** and **0 failed task attempts**
  across the recorded run. This run therefore does not exercise disk-spill behavior.
- Source hashing reads the CSV files before ingestion and can warm the OS filesystem
  cache. This is **not a controlled cold-cache benchmark**. The JVM dependencies were
  already downloaded. Keep the same measurement procedure when comparing later runs.
- Checks also read tables before Gold runs. Validation is timed separately, but its
  cache effects are part of this procedure. No explicit Spark data caching is used.
- No cloud service was used and no cloud dollar cost is estimated.
- Plan text in the local run directory captures initial plans; final adaptive plan
  updates are retained in Spark event logs.

## Verification and reproduction

All seven automated tests passed. They cover known campaign/CTR answers, midnight
boundaries in Asia/Shanghai, literal `NULL` and empty attributes, price preservation,
missing profiles, duplicate dimension keys, invalid click flags, missing ads,
malformed numeric input, changed headers, two equivalent rebuilds, protection of
existing output directories, and persisted failure reports.

The full run additionally matched the earlier independent inspection's row counts,
click totals, and missing-profile count. All three source SHA-256 values matched
[the source manifest](../../docs/source_manifest.json), whose checksums were verified
against the download mirror during inspection. The mirror is not independent
proof of original-publisher authenticity.

Run from the repository root:

```bash
bash scripts/test_baseline.sh
bash scripts/run_baseline.sh
```

Each new run has a fresh output directory and consumes additional disk space.
The recorded run is locally under `outputs/00_baseline/baseline-full-20260927/`.
Its tables, logs, and initial plans are ignored by Git; the small
[measurement report](baseline-full-20260927.json) is committed here.
The baseline's code remains in `experiments/00_baseline/` and its release is tagged
`baseline-v0.1`.
