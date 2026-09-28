# Iteration 1 results: CSV versus Parquet

**Uncompressed Parquet used 58.18% fewer data bytes and delivered substantially
faster reads for all three measured workloads.** The complete 26,557,961-record
representation and every query result matched.

This fills the format lesson that was missing when the original baseline began
with Parquet. It is a focused read-workload experiment, **not a claim that the
entire Bronze/Silver/Gold pipeline became 3.9–8.4 times faster**.

## What we compared

Use the same six typed fields from the full, original `raw_sample.csv` file:
`user`, `time_stamp`, `adgroup_id`, `pid`, `nonclk`, and `clk`. CSV has an explicit
schema and strict parsing; there is no schema-inference penalty in its timings.
We interpret the CSV once to write an equivalent Parquet copy, then execute the
same queries against both formats. Lookup tables, joins, derived timestamps,
and Gold reports are outside this experiment.

Both representations are **uncompressed**. The Parquet writer explicitly uses
`.option('compression', 'uncompressed')`. The session's recorded default remains
Snappy, but the writer option overrides it; all nine files' column-chunk footers
confirm `UNCOMPRESSED`. This is not another Snappy/ZSTD comparison.

There is no added partitioning, sorting, caching, or manual repartitioning. The
original CSV remains one file, which Spark splits into tasks; naturally written
Parquet has nine files. File layout and resulting execution partitions are part
of this practical conversion. We did not manufacture identical file layouts or
isolate each internal encoding/reader feature in a separate microbenchmark.

## Storage and conversion

Decimal MB = 1,000,000 bytes.

| Measurement | Result |
| --- | ---: |
| Original CSV data file | 1,088.06 MB |
| Uncompressed Parquet data files | 455.07 MB |
| Data-file reduction | **632.99 MB / 58.18%** |
| Parquet directory including local sidecars | 458.62 MB |
| CSV → Parquet conversion action | **7.335 seconds**, one observation |
| Original records | 26,557,961 |
| Parquet files / row groups / column chunks | 9 / 9 / 54 |

Conversion was performed once, before read measurements, and its cost is excluded
from query times. It includes source parsing and writing but excludes Spark
startup and source hashing. It is one observation, not a repeated conversion
benchmark. Reusing the files is what allows repeated reads to recover that cost;
converting solely for a one-off query is a different cost calculation.

We retained the original CSV and the Parquet copy for reproducibility. The storage
reduction is a comparison of representations, not newly freed disk space.

“Uncompressed” does not mean text representation or absence of format encodings.
Parquet has binary physical types and encoding mechanisms, separate from an
additional codec such as Snappy or ZSTD. We did not isolate their individual
contributions to this dataset's reduction. [Parquet encoding specification](https://parquet.apache.org/docs/file-format/data-pages/encodings/)

## Query results

Three runs per format. Times are median [minimum–maximum] seconds. The ratio is
CSV median divided by Parquet median; all Parquet runs were faster than their
paired CSV runs for all three workloads.

| Workload | CSV | Parquet | Median ratio |
| --- | ---: | ---: | ---: |
| Placement/count/clicks (2 columns) | 3.173 [2.951–4.341] | 0.816 [0.392–1.074] | 3.89× |
| All six columns | 4.100 [4.050–4.747] | 0.859 [0.755–1.342] | 4.77× |
| One-day placement/count/clicks (3 columns) | 2.850 [2.769–3.585] | 0.338 [0.301–1.324] | 8.44× |

- **Narrow:** group by placement, count impressions, and sum clicks. Only `pid`
  and `clk` are required from storage.
- **Wide:** group by placement, count rows, and sum every other field. This
  deliberately forces all six columns to participate. Sums of identifiers and
  timestamps are diagnostic checksums, not advertising business metrics.
- **One day:** the narrow query filtered to May 8, 2017 in Asia/Shanghai with a
  half-open epoch-second interval. It requires `time_stamp`, `pid`, and `clk`.

All queries include sums requiring real data access; none is a metadata-only
`COUNT(*)` comparison. Full-period results contain 26,557,961 impressions and
1,366,056 clicks; the one-day result contains 3,354,523 impressions and 174,394
clicks in both formats.

## Evidence for the mechanism

Spark's executed plans show `Batched: true` for Parquet and `Batched: false` for
CSV. The narrow query's `ReadSchema` contains only placement and clicks on both
sides. Yet their task input bytes differ dramatically:

| Workload | CSV task input MB | Parquet task input MB | Task CPU seconds: CSV → Parquet |
| --- | ---: | ---: | ---: |
| narrow | 1088.59 | 7.14 | 11.69 → 1.61 |
| wide | 1088.59 | 458.62 | 14.92 → 2.25 |
| one_day | 1088.59 | 221.11 | 10.29 → 0.77 |

These are medians from task counters, not unique physical disk reads. CPU seconds
sum work across tasks, so they can exceed wall-clock seconds. All measured query
runs had zero disk spill and no failed task attempts.

CSV can avoid converting unnecessary fields, but it still scans the text stream
containing all columns. Parquet can read the relevant column data. In this run,
the narrow Parquet workload reported about 7.14 MB of task input versus 1,088.59 MB
for CSV. The all-column Parquet query reported about 458.62 MB instead: requesting
more columns changes the amount of data read.

For the one-day query, both plans contain pushed date-range filters. CSV still
reports the same input bytes as its other queries. All nine Parquet files are
selected, and neither format has partition filters. Therefore **this is not the
date-partition pruning from iteration 3**. A pushed predicate does not by itself
prove whole files were skipped. We retain the plans and scan counters rather
than attributing the entire speedup to a single mechanism.

See the [narrow CSV plan](query-plans/01-csv-narrow.txt),
[narrow Parquet plan](query-plans/01-parquet-narrow.txt), and
[one-day Parquet plan](query-plans/01-parquet-one_day.txt).
[Spark's Parquet documentation](https://spark.apache.org/docs/4.0.1/sql-data-sources-parquet.html)
and [CSV options](https://spark.apache.org/docs/4.0.1/sql-data-sources-csv.html)
provide the reader/format configuration context.

## All timing samples

Order rotates so every workload is first once per format. No first measurement
is silently discarded. Query action timing includes planning and collecting
results, but excludes process/Spark startup, relation/schema preparation, plan
inspection, and correctness verification.

| Run | Order | Narrow (s) | Wide (s) | One day (s) |
| --- | --- | ---: | ---: | ---: |
| 01-csv | narrow, wide, one_day | 4.341 | 4.100 | 2.850 |
| 01-parquet | narrow, wide, one_day | 1.074 | 0.755 | 0.301 |
| 02-parquet | wide, one_day, narrow | 0.392 | 1.342 | 0.338 |
| 02-csv | wide, one_day, narrow | 2.951 | 4.747 | 2.769 |
| 03-csv | one_day, narrow, wide | 3.173 | 4.050 | 3.585 |
| 03-parquet | one_day, narrow, wide | 0.816 | 0.859 | 1.324 |

## Correctness and limitations

- **21 automated tests passed.** The new format fixture checks typed nulls,
  quoted strings, duplicate records, and inclusion/exclusion at day boundaries.
- **Complete exact record equality passed**, using bidirectional `EXCEPT ALL`
  with matching types, duplicate multiplicities, and nulls. This compares all
  26,557,961 records, not just totals or sampled rows.
- **All 18 measured query executions matched**, giving 15 comparisons against
  the first query run's three results. Read schemas also match.
- **Every one of the nine Parquet file footers was checked**: all 54 column chunks
  use `UNCOMPRESSED`, and footer rows/bytes agree with the data and storage report.
- The source SHA-256 before preparation and after verification matches the
  original dataset fingerprint recorded in earlier experiments.
- Preparation and all query processes used the same clean measured commit
  `535df77c5482d060cd96c64c4c675bb97d187300`, runtime, and recorded SQL settings.

Measured on 2026-09-27 with Python 3.11.15, Spark 4.0.1, Java 21.0.12.1, macOS
arm64, four local worker threads, and a 2 GiB driver heap. The shared session loads
Delta 4.0.0, but this experiment reads ordinary CSV/Parquet, not Delta tables.

The physical file count differs (one CSV versus nine Parquet files), and formats
can lead Spark to divide work differently. No result should be described as the
isolated causal effect of column pruning alone. The comparison measures the
complete format/read path with its native representation and default execution.

Each query run starts a fresh process, but the three workloads within it share a
session. Metadata, JIT, and first-query effects remain visible despite rotating
order. OS caches are not flushed; conversion may warm both source/output caches.
No data-query warm-up or explicit cache is used. Background activity is not
controlled. Three local samples describe observations, not guarantees for a
cluster or remote object store. No cloud-dollar savings were measured.

## Conclusion for this project

This is strong evidence for keeping Parquet as the project's analytical storage
foundation: smaller representation, much less input for narrow queries, and
faster reads here. It also explains why the untuned baseline could already use
Parquet: “baseline” meant no manual tuning, not deliberately avoiding useful
standard formats. Compression and date partitioning remain separate lessons.

The up-front conversion cost still matters. Preserve raw CSV as the source and
reuse the analytical representation when repeated access justifies it. These
results do not prove every one-off workload benefits from conversion or measure
the full lakehouse pipeline's end-to-end improvement.

## Reproduce and inspect

```bash
bash scripts/test_iteration.sh 1
```

This runs the test suite, conversion, repeated reads, exact record verification,
and codec inspection, then produces a `RESULTS.md` entry point. Use a fresh
`--comparison-id` if specifying one. A single-repeat tiny-fixture run is a smoke
check, not a performance result.

- [Experiment design and options](../../experiments/01_csv_vs_parquet/README.md)
- [Generated measurements](measurements.md)
- [Every query sample, result, metric, and verification](format-comparison.json)
- [Preparation details](preparation.json)
- [Exact equality and codec proof](verification.json)
- [Central experiment index](../README.md)

Large files and event/test logs remain locally under the ignored comparison
`outputs/comparisons/formats-01-20260927/`. Only small reports and sanitized plans
are published. The experiment and results are tagged `iteration-01-csv-vs-parquet`.
Existing baseline, compression, and partitioning implementations are preserved.
