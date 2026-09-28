# Iteration 8 results: automatic shuffle-partition combining

**Keep automatic combining enabled for this workload.** Disabling only
`spark.sql.adaptive.coalescePartitions.enabled` increased median combined Gold
time from **7.024 to 8.988 seconds (+27.96%)** and median pipeline processing from
**35.428 to 38.580 seconds (+8.90%)**. Gold used 200 reader tasks and wrote 200
files per report, compared with four of each in iteration 2.

This is a controlled learning comparison: iteration 2 already benefits from
this automatic optimization. Iteration 8 reveals its contribution by disabling
it. It does not replace iteration 2 as the preferred reference.

## One deliberate change

Iteration 8 copies iteration 2 and changes one session-wide setting:

```python
.config('spark.sql.adaptive.coalescePartitions.enabled', 'false')
```

AQE itself remains enabled. Initial shuffle partitions remain 200; report order,
transformations, automatic joins, unpartitioned ZSTD tables, local[4], and the
2 GiB heap remain unchanged. No cache, manual repartition, join hint, or resource
increase was introduced. Iterations 0–7 are preserved.

The observation launcher reads effective settings from each live session,
extending the environment report without changing the older pipeline. It
records advisory size, parallelism-first, initial partition override, minimum
partition size, local shuffle reading, and skew-join settings as context. Only
the coalescing flag differs between sides.

Spark combines contiguous shuffle partitions using runtime statistics when AQE
and coalescing are enabled. The final task count can differ from the configured
initial count. Four readers is the observed result here, not a rule that every
job should use four. [Spark documentation](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html#coalescing-post-shuffle-partitions)

[Runnable version](../../experiments/08_no_shuffle_coalescing/README.md)

## Every full-data sample

Seconds. Each sample used a fresh process. Actual order was 01-left, 01-right,
02-right, 02-left, 03-left, 03-right. Left is iteration 2; right is iteration 8.

| Run | Campaign | Audience | Combined Gold | Pipeline processing | Validation | Total run |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 01-left | 2.983 | 4.114 | 7.097 | 34.984 | 5.898 | 44.153 |
| 01-right | 4.080 | 5.168 | 9.248 | 39.149 | 7.156 | 49.616 |
| 02-right | 3.887 | 4.789 | 8.676 | 38.580 | 6.815 | 48.674 |
| 02-left | 2.943 | 4.081 | 7.024 | 35.428 | 6.074 | 44.653 |
| 03-left | 3.010 | 3.774 | 6.784 | 36.037 | 5.815 | 45.049 |
| 03-right | 3.874 | 5.114 | 8.988 | 37.633 | 6.983 | 47.644 |

Gold ranges were **6.784–7.097 s** versus **8.676–9.248 s**. Processing ranges
were **34.984–36.037 s** versus **37.633–39.149 s**. All paired variant runs
were slower on both measures. Median validation also increased from **5.898 to
6.983 s (+18.40%)**; the flag applies to the whole session. Median total run time
rose from **44.653 to 48.674 s (+9.00%)**. Summing processing and validation within
each run gives medians of **41.502 versus 45.395 s (+9.38%)**.

Processing includes Bronze, Silver, and both Gold writes, including their Delta
work. Validation is timed separately. Total also includes fingerprints, startup,
shutdown, and reporting overhead. Cross-run exact comparisons and footer checks
happen afterward and are excluded. OS caches were not flushed. These three local
repetitions support this workload's observed outcome, not a universal speedup.

[All samples and exact comparisons](comparison.json) ·
[Generated measurements](measurements.md) · [Gold cost breakdown](gold-summary.json)

## What Spark actually did

The following behavior was consistent across all three repetitions per side:

| Observation | Iteration 2 | Iteration 8 |
| --- | ---: | ---: |
| AQE enabled | true | true |
| Coalescing enabled | true | false |
| Initial Gold shuffle partitions | 200 | 200 |
| Actual reader tasks, per Gold report | 4 | 200 |
| Coalesced reader in final plan | Yes | No |
| Ads and profile joins | Broadcast hash left outer | Broadcast hash left outer |
| Joined Gold SQL tasks, both reports | 34 | 426 |
| Other Gold-stage tasks, both reports | 702 | 600 |
| All Gold task attempts | 736 | 1,026 |
| Gold output files, per report | 4 | 200 |
| All pipeline Parquet files | 38 | 430 |

Execution evidence matches successful SQL executions to their completed stages
and task events. It verifies effective AQE settings and initial partition counts,
observes final readers and join choices, and rejects contradictory evidence.
Other Gold stages, primarily Delta metadata work, are reported separately.
A plan name or a setting alone is not sufficient evidence.

More reader tasks do not create more compute: both versions have four local
task slots. Each smaller task finishes sooner, but many more must run and write
files. For campaign readers, per-run median task durations were **519–711 ms**
with combining and **33–35 ms** without it. Audience medians were **842–870.5 ms**
and **39–40 ms** respectively. Despite those shorter individual tasks, total
Gold wall time increased. Every duration sample and nearest-rank p95 is retained
in the execution evidence; task duration is not a direct scheduler-overhead metric.

[Executed evidence and file distributions](coalescing-evidence.json) ·
[Control campaign plan](coalescing-plans/01-left/gold.campaign_daily.txt) ·
[Variant campaign plan](coalescing-plans/01-right/gold.campaign_daily.txt)

## Compute, metadata, and storage costs

| Gold metric, median across runs unless stated | Iteration 2 | Iteration 8 |
| --- | ---: | ---: |
| All Gold task CPU | 18.017 s | 20.006 s |
| Joined Gold SQL task CPU | 17.124 s | 19.873 s |
| Other Gold-stage task CPU | 0.864 s | 0.133 s |
| Shuffle writes | 138.692 MB | 138.687 MB |
| Disk / memory shuffle spill | 0 / 0 | 0 / 0 |
| Failed Gold task attempts | 0 | 0 |
| Persistent table bytes, each run | 630.854 MB | 631.938 MB |

CPU is cumulative across tasks, not elapsed wall time. Group medians are
computed separately and need not add to the total median. The reduction in
other Gold tasks and CPU shows why we should not attribute every effect of the
session-wide setting solely to aggregation scheduling. For each report, a one-task Delta-log JSON scan ran inside the control's Gold
job group and inside the variant's validation group, consistently across all
three runs. Controls also performed an additional 50-task Delta table-state
scan within each Gold group. Both sides have their own state scans during
validation, so the Gold task reduction is not entirely movement between groups.
[Metadata inspection and example plans](metadata-plans/inspection.json) record
the execution IDs, groups, counts, and plans. The internal optimizer cause of
these differences was not isolated. The higher combined processing-plus-
validation and total times confirm the slowdown even after accounting for it.

Shuffle volume was practically unchanged and neither side spilled. The larger
number of smaller tasks/files, increased joined-query CPU, and higher Gold wall
time are consistent with task and write overhead outweighing any benefit of
smaller work units here. We did not independently isolate scheduler, encoding,
and file-creation overhead, so this is an interpretation of the evidence rather
than a measured attribution to one component.

File distributions were identical across repetitions within each side. Decimal
MB and kB are used below. Each table has 4 files on the left and 200 on the right.

| Gold table and side | Min kB | Median kB | p95 kB | Max kB | Total MB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Campaign, iteration 2 | 3,821.580 | 3,906.785 | 3,992.142 | 3,992.142 | 15.627 |
| Campaign, iteration 8 | 78.554 | 80.818 | 82.229 | 83.403 | 16.157 |
| Audience, iteration 2 | 4,830.300 | 4,913.726 | 4,998.955 | 4,998.955 | 19.657 |
| Audience, iteration 8 | 97.628 | 99.629 | 100.985 | 101.656 | 19.930 |

Total persistent storage increased only **0.17%**, while Gold file counts rose
50-fold. Storage bytes alone therefore miss an important physical difference.
This experiment measured full rebuilds; it did not benchmark later read queries
or compaction. Per-table distributions for Bronze and Silver are also retained.

## Correctness and reproducibility

The completed suite passed **32 regression tests**, six full-data rebuilds,
**40 exact table comparisons** (eight tables for each of the five later runs
against the first control), and **1,404 ZSTD Parquet footer checks**. All source
fingerprints and recorded settings match except the declared coalescing flag.

Both Gold reports preserve **26,557,961 impressions**, **1,366,056 clicks**, and
**1,528,526 missing-profile impressions**. Campaign and audience tables have
1,819,995 and 3,738,362 rows respectively. Equality checks cover schemas, full
records, nulls, and duplicate multiplicities; counts alone are not the proof.

Fixture tests cover missing profiles and null values, the observation launcher's
neutrality, and rejection of missing or contradictory settings, plans, stages,
and failed SQL execution. Both real entrypoints and the standalone wrapper were
exercised. The same transformation and validation code as iteration 2 is retained.

[Codec evidence](codecs.json) · [Completed suite record](suite.json)

Run the full experiment with:

```bash
bash scripts/test_iteration.sh 8
```

Run just the variant with:

```bash
bash scripts/run_no_shuffle_coalescing.sh
```

Ensure at least 7 GiB free before the full comparison. The generated `RESULTS.md`
links the pipeline, coalescing, and codec checks. Full local outputs, plans,
events, logs, and verification records remain under
`outputs/comparisons/coalescing-08-20260928/`. Git retains compact evidence and
executed plans rather than the raw data and generated tables.

Earlier fixture evidence remains in `fixture-*.json`. Its five impressions and
one repetition per side establish correctness, not performance; those runs
occurred during development with a dirty worktree. The full-data evidence is
separate and uses clean commit `ccd8b21ea82a1bc95fb6186f04bd6e538995f1e8`.

## Storage and retained history

The user approved removing only the generated Bronze/Silver/Gold tables from
`partitioning-03-20260927`: **18 directories, 3,503,948,949 bytes (3.263 GiB)**.
Reports, logs, plans, query evidence, source data, every project version, and the
original baseline were retained. The cleanup left **7.095 GiB free** before
starting this full benchmark. No additional datasets were deleted.

[Cleanup manifest](cleanup.json) · [Preflight history](storage-preflight.json)

No resource limits were raised. All six timed full-data builds succeeded; no
failed full-data rebuild was omitted. The separate exact-verification process logged GC/page-allocation retries while
checking `03-left: silver/impressions`; that comparison and the remaining
checks passed. Those warnings occurred outside the timed rebuilds. About
**3.5 GiB remained after the suite**.
The completed findings are tagged `iteration-08-no-shuffle-coalescing`.

## Recommendation

Keep iteration 2 as the reference and retain automatic combining for this
workload. Iteration 6 changed the starting shuffle count; iteration 8 isolates
Spark's runtime combining of those partitions. Together they demonstrate why
configured partitions, actual task counts, and output files should be measured
separately. The preferred numbers remain workload- and resource-dependent.
