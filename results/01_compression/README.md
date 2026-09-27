# Iteration 1 results: Snappy versus ZSTD

**ZSTD made the output tables 31.85% smaller. These runs do not demonstrate a
processing-speed improvement.** All reporting results stayed identical.

This is a compression experiment. Both versions already use Delta tables backed
by Parquet; we did not switch the pipeline to CSV. The conceptual and logical data
models, transformations, and eight output tables remain the same.

## What changed

The only deliberate performance change is:

```python
.config('spark.sql.parquet.compression.codec', 'zstd')
```

The baseline uses Spark's default Snappy codec. The independently runnable
[iteration snapshot](../../experiments/01_compression/README.md) retains the same
transforms, validation, measurements, and resource budget. No compression level,
join hint, partitioning, sorting, caching, or shuffle setting was tuned.

For learning: a codec changes how data is compressed inside Parquet files. The
same rows can occupy fewer bytes. Reading and writing them still requires CPU
work, so smaller files do not guarantee a faster overall pipeline.

## Main measurements

Three full-data runs per version, in alternating order. MB means 1,000,000 bytes.
Times below are medians unless a range is explicitly shown.

| Measurement | Baseline: Snappy | Iteration 1: ZSTD | Interpretation |
| --- | ---: | ---: | --- |
| Eight output tables | 925.72 MB | 630.85 MB | **294.86 MB / 31.85% smaller** |
| Pipeline processing | 34.236 s | 33.934 s | 0.88% lower median; no clear speed gain |
| Processing range | 33.533–34.571 s | 33.220–34.783 s | Ranges overlap |
| Full run, including startup and validation | 43.071 s | 43.066 s | Essentially unchanged |
| Processing task input bytes | 1,828.14 MB | 1,594.41 MB | 12.79% fewer task-reported bytes |
| Parquet data files | 38 | 38 | Same count |
| Processing disk spill | 0 bytes | 0 bytes | No spill to reduce in this comparison |

Output size was identical across the three runs of each version. Table size
includes Parquet data and table metadata/local filesystem sidecar files, but
excludes raw CSV files, event logs, and reports. The shared source CSV files
remain **1.14 GB** and are unchanged. Keeping both implementations and all six
measured outputs consumes disk space; this experiment has not freed 294.86 MB
from the computer.

The 12.79% input-byte reduction is smaller than the storage reduction because
processing includes reading the unchanged CSV source and only some portions of
the resulting tables. These Spark counters are **not unique physical disk reads**.

Processing task CPU time had medians of 93.75 versus 94.90 CPU-seconds, with
variation across runs; this does not establish a CPU penalty. Task CPU-seconds
sum work across tasks and can exceed elapsed seconds. Shuffle writes were
137.75 versus 138.72 MB, a small increase of 0.71%; this was not a shuffle
optimization. The complete metric ranges are retained in the raw report.

## Correctness and mechanism checks

- **16 automated tests passed**, including the compression integration test.
- **Six full pipeline runs succeeded** with identical source hashes and reporting
  checks: 26,557,961 impressions, 1,366,056 clicks, and 1,528,526 impressions with
  missing user profiles retained in both Gold reports.
- **40 exact table comparisons passed:** each of the five later runs was compared
  with the first baseline run across all eight tables. Column names/types and
  actual values match; duplicate rows and nulls are preserved. This is stronger
  than checking row counts alone. Row order and nullable metadata flags are not
  equality requirements.
- **All 228 Parquet files were inspected**, across 48 table instances. Every
  column chunk uses Snappy on the baseline side and ZSTD on the iteration side.
  Footer row/file totals and file byte sizes agree with the measurement reports.
- The recorded SQL-setting difference is only the declared compression codec.
  All runs used the same clean code commit and the same runtime/resource settings.
- Latest recorded runtime plans for both Gold queries show two broadcast hash
  joins in every run. The codec change did not change those join strategies.
- No failed task attempts or disk spills were recorded for processing/validation.

Tests, exact cross-run comparisons, plan inspection, and codec inspection are
outside the timed pipeline work. Each pipeline's own validation is reported
separately and included in its full-run total. The exact cross-run comparison
phase took 709.495 seconds; that verification time is not a pipeline runtime.

## Where storage changed

These are complete table directory sizes, including metadata, in decimal MB.

| Table | Snappy MB | ZSTD MB | Reduction |
| --- | ---: | ---: | ---: |
| bronze/ads | 15.93 | 11.28 | 29.17% |
| bronze/impressions | 340.11 | 209.93 | 38.28% |
| bronze/user_profiles | 8.90 | 6.13 | 31.15% |
| gold/audience_daily | 28.65 | 19.82 | 30.82% |
| gold/campaign_daily | 23.36 | 15.76 | 32.53% |
| silver/ads | 14.22 | 10.42 | 26.73% |
| silver/impressions | 486.59 | 352.82 | 27.49% |
| silver/user_profiles | 7.98 | 4.70 | 41.05% |

## Method and limits

Measured on 2026-09-27 at commit
`ff949eb6c3c94ed5d418500970aeaa3102939e7b`, with a clean working tree for every
measured run. Python 3.11.15, Spark 4.0.1, Delta 4.0.0, Java 21.0.12.1,
macOS arm64, `local[4]`, and a 2 GiB driver heap. Spark's automatic optimizations
remain enabled on both sides. The original baseline code/tag are unchanged.

Every run rebuilds Bronze, Silver, and Gold from the full three source files in
a fresh process. Order was Snappy, ZSTD, ZSTD, Snappy, Snappy, ZSTD. We retained
all measurements, including the first runs, and used fresh baseline measurements
rather than comparing against the older single-run reference.

OS caches were not flushed, source hashing can warm caches, and background
machine activity was not controlled. Three runs per version describe the observed
variation; they do not establish statistical significance or predict cluster
performance. This is a full batch rebuild benchmark, not a separate interactive
query benchmark. We measured local seconds and bytes, not cloud dollar costs.

**Decision:** retain ZSTD as the starting point for the next experiment because
it delivered consistent storage savings without an observed substantial runtime
regression here. Future changes should be compared with this version as well as
keeping the original baseline available. No iteration 2 changes are included.

## Evidence and reproduction

- [Generated measurements and all six run times](measurements.md)
- [Raw comparison report](comparison.json): individual reports, source hashes,
  settings, task counters, summaries, and all exact equality results
- [Actual file codecs](codecs.json)
- [Gold join plan observations](join_plans.json)
- [Shared comparison procedure](../../docs/comparisons.md)

Run a new comparison after following the root README's setup instructions:

```bash
bash scripts/compare.sh --right experiments/01_compression/run.py \
  --comparison-id compression-01-rerun --repeats 3 \
  --change 'Parquet compression: Snappy to ZSTD across Bronze, Silver, and Gold' \
  --changed-setting spark.sql.parquet.compression.codec
```

Use a fresh comparison ID; existing results are protected. Verify its files after
the comparison succeeds. With `JAVA_HOME` exported (or configured in `.env.local`):

```bash
# Only needed if using .env.local instead of an exported JAVA_HOME:
set -a
source .env.local
set +a

export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS='--driver-memory 2g pyspark-shell'
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
uv run --frozen python -m benchmarks.storage_codecs \
  outputs/comparisons/compression-01-rerun
```

The published comparison ID is `compression-01-20260927`. Its tables, full event
logs, and test logs remain locally under the ignored `outputs/comparisons/`
directory. Only small result summaries are in Git. The completed experiment and
report are preserved with tag `iteration-01-compression`; the original baseline
remains `baseline-v0.1`. CSV-versus-Parquet remains an optional separate learning
lab and was not implemented in this iteration.
