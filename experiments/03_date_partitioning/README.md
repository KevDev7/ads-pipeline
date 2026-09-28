# Iteration 3: date partitioning

**One deliberate change from iteration 2:** write `silver/impressions` with
`partitionBy('reporting_date')`. ZSTD, schemas, transforms, SQL settings, and the
resource budget stay the same. Bronze, dimension tables, and Gold remain
unpartitioned. The largest fact table is the subject of this experiment; small
Gold tables are not partitioned just because they also contain a date.

This is persistent table partitioning, not manually tuning Spark execution or
shuffle partitions. The date already exists in the logical schema. A partitioned
Delta table stores its date in partition metadata/directories; Delta readers
reconstruct the same logical column. Writer sorting, file counts, and consequent
automatic plan changes are effects to measure, not separately tuned changes.

**Completed:** [results and evidence](../../results/03_date_partitioning/README.md)
show faster measured date-filtered reads, but a 7.99% slower full rebuild and
more small files. All logical results match. Tag: `iteration-03-date-partitioning`.

## Hypothesis

A date filter may read fewer files and bytes, but extra files and write work may
hurt full-period reads or rebuilds. The source contains about eight days, and its
daily partitions are much smaller than Delta's approximate 1 GB guideline. This
is a measured learning experiment, not a promise that partitioning is advisable
at this scale. See [Delta's partition guidance](https://docs.delta.io/best-practices/#choose-the-right-partition-column).

## Workloads and checks

1. Three complete rebuilds per version, alternating order, with the shared runner.
   All eight tables in every later run are compared exactly to the first control.
2. Read Silver impressions and group by placement to compute impressions and
   clicks for May 8, May 8–10 inclusive, and the full period. These date choices
   are fixed before running; all three queries force data reads with `SUM(clicked)`.
3. A fresh query process per pipeline output; warm Delta metadata outside timing,
   but no data warm-up/caching. Rotate query order across repetitions. Capture
   action time, task input bytes, and actual executed scan file/byte metrics.
   All query results must match across versions and repeats.
4. Inspect Delta partition metadata, files, and query plans. Record rebuild time,
   table bytes, file counts/sizes, and query tradeoffs separately.

OS caches remain uncontrolled. Query startup/metadata warm-up and correctness
verification are outside measured query action time. Pipeline validation remains
in full-run totals but outside pipeline processing time. This is a local test
with three observations per side, not a cold-storage or cluster benchmark.

## Run

Run all applicable checks with one command:

```bash
bash scripts/test_iteration.sh 3
```

Open the generated `RESULTS.md` for pipeline, query (when applicable), and codec
evidence. See [the comparison procedure](../../docs/comparisons.md). The individual
commands below remain available.

```bash
bash scripts/run_partitioning.sh
bash scripts/compare.sh --left experiments/02_compression/run.py \
  --right experiments/03_date_partitioning/run.py \
  --comparison-id partitioning-03 --repeats 3 \
  --change 'Partition Silver impressions by reporting_date; keep ZSTD'
```

After the comparison succeeds:

```bash
bash scripts/compare_partition_queries.sh \
  --comparison-dir outputs/comparisons/partitioning-03
```

Use fresh IDs. Large outputs stay local. See the [results index](../../results/README.md).
Iteration 1 (CSV versus Parquet) is still planned; this experiment proceeds from
the completed iteration 2 as requested.
