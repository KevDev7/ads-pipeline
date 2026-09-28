# Iteration 3 results: date partitioning

**Date-filtered reads improved in these runs, but full rebuilds became slower.**
The one-day query's median fell from 0.565 to 0.261 seconds; pipeline processing
rose from 36.503 to 39.420 seconds. All logical tables and query results match.
This is a workload tradeoff, not a universal upgrade.

## What changed

The control is **iteration 2**, with ZSTD and no table partitioning. Iteration 3
adds one deliberate layout change when writing `silver/impressions`:

```python
writer = writer.partitionBy('reporting_date')
```

Bronze, lookup tables, and Gold remain unpartitioned. Both sides use the same
schemas, transforms, source files, compression, SQL settings, and resource budget.
The conceptual/logical model is unchanged. Each version still rebuilds all layers
from all three CSV files; this is not incremental processing.

Storage partitions persist on disk. They are distinct from Spark's execution and
shuffle partitions, which we did not tune. Spark's existing automatic behavior
remains enabled; downstream changes in file counts/encoding are consequences of
the new layout, not additional manual optimizations.

## Rebuild and storage results

Three full-data runs per side. Times are medians; MB means 1,000,000 bytes.

| Measurement | Iteration 2: unpartitioned | Iteration 3: date-partitioned | Change |
| --- | ---: | ---: | --- |
| Pipeline processing | 36.503 s | 39.420 s | **7.99% slower** |
| Processing range | 35.784–36.722 s | 38.561–40.265 s | Partitioned was slower in every pair |
| Full run, including startup/validation | 46.370 s | 48.593 s | 4.79% longer |
| Silver impressions write | 8.856 s | 12.488 s | 41.01% longer |
| All output tables | 630.85 MB | 537.13 MB | **14.86% smaller** |
| Silver impressions table | 352.82 MB | 260.55 MB | 26.15% smaller |
| All Parquet files | 38 | 74 | 36 more files |
| Silver impressions files | 5 | 40 | Eight date partitions, five files each |
| Median fact-file size | 86.19 MB | 7.83 MB | Much smaller individual files |
| Processing disk spill | 0 MB | 463.62 MB | Temporary I/O during the fact-table write |

Output bytes were identical across repeats of each version. Table sizes include
Parquet and table metadata/local sidecars; they exclude source CSVs, event logs,
and reports. The 1.14 GB source files are unchanged. Keeping experiment outputs
consumes space; these smaller table sizes are not a claim that disk was freed.

The main measured write cost is in Silver impressions. Every partitioned run
reported 463,619,559 disk-spill bytes there, versus zero in the control, under the
fixed 2 GiB driver heap. The corresponding memory-spill counter was 2,415,916,800
bytes: it measures the pre-serialization representation of spilled data, not
peak RAM usage. Processing task CPU medians also rose by 10.55%.

Gold remains logically and physically unpartitioned, but its output layout is
not byte-identical: campaign output increases from four files to five. Its
recorded join operators remain two broadcast hash joins per Gold query on both
sides. Different upstream row/file organization can change downstream automatic
execution and encoding without changing the business result.

## Read-query results

All queries group Silver impressions by placement and calculate impression
counts and clicks. `SUM(clicked)` forces data reads; this is not a metadata-only
row-count benchmark. Date ranges were selected before measurement. All 18 query
executions succeeded, with exactly matching output rows for each workload.

Seconds are median [minimum–maximum]. Negative changes mean shorter times.

| Query | Iteration 2 | Iteration 3 | Median change |
| --- | ---: | ---: | ---: |
| One day: May 8 | 0.565 [0.331–0.958] | 0.261 [0.214–0.641] | -53.74% |
| Three days: May 8–10 | 0.475 [0.413–1.053] | 0.348 [0.330–0.777] | -26.72% |
| Full period: May 6–13 | 0.533 [0.462–0.898] | 0.485 [0.455–1.114] | -9.05% |

Both date-filtered queries were faster in every paired repetition. These are
observations from three local samples per side, not a promise of the same
percentage on a cluster. The full-period query is mixed: iteration 3 was slightly
faster in two pairs and slower in the third. Its 9.05% lower median is not enough
to claim a reliable full-period speedup.

### What Spark actually read

The arrows below mean iteration 2 → iteration 3. Byte values are medians in MB.

| Query | Files selected | Complete selected-file bytes | Task input bytes |
| --- | ---: | ---: | ---: |
| One day: May 8 | 5 → 5 | 350.07 → 32.87 | 16.673 → 0.940 |
| Three days: May 8–10 | 5 → 15 | 350.07 → 96.71 | 16.671 → 2.784 |
| Full period: May 6–13 | 5 → 40 | 350.07 → 258.45 | 5.815 → 7.441 |

These are different measurements:

- **Files selected** and **complete selected-file bytes** come from the actual
  executed file scan's `numFiles` and `filesSize` metrics. They describe the
  selected files, not the bytes physically read from storage.
- **Task input bytes** come from Spark task metrics. Column pruning, encodings,
  read ranges, and file overhead affect these counters. They are not unique
  physical disk reads, and OS caches can satisfy reads.

The one-day query selects one partition and five files; three days select three
partitions and 15 files; the full period selects all eight partitions and 40
files. The control selects its five large files for every query. In particular,
**five files versus five files does not mean equal work**: one version's files
contain all eight days and the other's contain only the selected day.

All five control fact files have date min/max statistics spanning May 6–13.
Its one-day executed plan has a pushed Parquet date filter, but no partition
filter. The partitioned plan explicitly contains
`PartitionFilters: ... reporting_date = 2017-05-08`; it reads placement and clicked
columns from Parquet and obtains the date from partition metadata. See the
[control plan](query-plans/01-left-one_day.txt) and
[partitioned plan](query-plans/01-right-one_day.txt).

The observed one-day task input reduction is **94.36%**; the three-day reduction
is **83.30%**. Full-period task input instead **increases by 27.97%**, despite the
smaller total file bytes. The file layout and encoding have changed, and scanning
40 small files has different overhead from scanning five larger files. These
measurements do not isolate each contributor to that increase.

### Every query timing sample

Workload names map to the date ranges above. Order rotates so each workload is
first once per side. No first run is silently discarded.

| Run | Query order | One day (s) | Three days (s) | Full period (s) |
| --- | --- | ---: | ---: | ---: |
| 01-left | one_day, three_days, full_period | 0.958 | 0.475 | 0.462 |
| 01-right | one_day, three_days, full_period | 0.641 | 0.348 | 0.455 |
| 02-right | three_days, full_period, one_day | 0.214 | 0.777 | 0.485 |
| 02-left | three_days, full_period, one_day | 0.331 | 1.053 | 0.533 |
| 03-left | full_period, one_day, three_days | 0.565 | 0.413 | 0.898 |
| 03-right | full_period, one_day, three_days | 0.261 | 0.330 | 1.114 |

## Correctness and method

- **17 automated tests passed**, including a hand-checkable partition/pruning
  fixture with midnight boundaries and a missing user profile.
- **Six full rebuilds passed their validations**, each with 26,557,961 impressions,
  1,366,056 clicks, and 1,528,526 missing-profile impressions retained.
- **40 exact table comparisons passed:** all eight tables in each later run
  matched the first control, including types, values, duplicates, and nulls.
  Physical column order and nullable metadata flags are not equality requirements.
- **18 measured query executions matched**, giving 15 cross-run result comparisons
  against the first query run, in addition to complete table equality.
- **336 Parquet files were inspected:** every column chunk remains ZSTD. Footer
  row/file totals and byte sizes agree with the reports.
- Delta metadata confirms only the right-hand Silver impressions table has a
  `reporting_date` partition specification. Executed scan metrics confirm the
  expected one, three, or eight selected partitions.
- All measured processes used clean commit
  `dc9bebf20e35e6ac58be34f78cae9db7d6f49154`, identical source hashes, and identical
  recorded runtime/SQL settings. No pipeline or query task failures were recorded.

Measured on 2026-09-27: Python 3.11.15, Spark 4.0.1, Delta 4.0.0, Java 21.0.12.1,
macOS arm64, `local[4]`, and a 2 GiB driver heap. Pipeline order was left, right,
right, left, left, right. We reran the control; the original historical baseline
and compression timings are not the denominator of this experiment.

Each rebuild uses a fresh process. Query benchmarks then use one fresh process
per output, with Delta metadata warmed outside timing and no explicit data cache
or data-query warm-up. Query timing includes planning and collecting results,
excludes Spark startup/metadata warm-up/plan inspection, and is **not** an
end-to-end job startup measurement. First-query/JIT effects remain visible in the
samples even with rotating order. OS caches were not flushed, and prior rebuilds
and equality checks may warm them; background activity is uncontrolled.

The 769.243-second exact-table verification phase is excluded from pipeline
processing and query time. Each pipeline's own validation is included in its
full-run time and separately recorded. We measure local seconds and bytes,
not cloud dollar costs.

## What to learn and what to keep

**Partitioning helped the measured date-filtered reads, at a write and file-count
cost.** We should keep both implementations available and choose based on the
workload, rather than treating iteration 3 as an unconditional replacement.
Iteration 2 remains the simpler reference for full rebuilds; iteration 3 is useful
for learning selective access and measuring the consequences of small files.

These partitions are only about 31–34 MB of Parquet each, with individual files
as small as 0.10 MB. That is far below Delta's roughly 1 GB-per-partition guideline.
The local result demonstrates pruning; it does not establish this as an ideal
production layout. [Delta's partitioning guidance](https://docs.delta.io/best-practices/#choose-the-right-partition-column)

The conceptual/logical models have stayed the same. The physical model changed,
which changed what Spark could skip. Partition pruning is the benefit enabled by
this storage change; it is not a second separately switched-on optimization.

## Evidence and reproduction

- [Experiment and commands](../../experiments/03_date_partitioning/README.md)
- [Generated rebuild measurements](rebuild-measurements.md)
- [Raw rebuild/equality report](comparison.json)
- [Raw query measurements and results](query-comparison.json)
- [Layout metadata and file-size distribution](layout.json)
- [Actual codecs](codecs.json)
- [Gold join observations](join_plans.json)
- [Central experiment index](../README.md)

The comparison ID is `partitioning-03-20260927`. Large tables and event/test logs
remain local under the ignored `outputs/comparisons/` directory. The published
query plans replace the workspace's absolute path with `<repo>`; raw local event
logs retain it. The completed implementation and evidence are preserved with tag
`iteration-03-date-partitioning`. Earlier implementations/tags remain unchanged.
CSV versus Parquet was not part of this experiment; its separately completed
[iteration 1 results](../01_csv_vs_parquet/README.md) are now available.
