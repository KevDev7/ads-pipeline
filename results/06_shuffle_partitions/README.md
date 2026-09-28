# Iteration 6 results: initial shuffle partitions, 200 versus 32

**A modest observed Gold improvement, not a clear whole-pipeline upgrade.**
Changing only `spark.sql.shuffle.partitions` from 200 to 32 reduced the Gold
median from **7.516 to 7.012 seconds** in three local runs per side. Whole
processing medians were 36.183 versus 34.978 seconds, but ranges overlap and the
variant was slower in the first pair. All logical tables match.

The main learning result: **200 initial partitions became four Gold reader tasks;
32 initial partitions became five.** AQE remained enabled. The configured count
is not the final task count, and reducing it need not reduce final tasks.

## One change, chosen before measurement

Iteration 6 starts from **iteration 2**, retaining unpartitioned ZSTD tables,
automatic broadcast joins, `local[4]`, a 2 GiB heap, and the same transformations.
The only tuning change is:

```python
.config('spark.sql.shuffle.partitions', '32')
```

No join hint, manual repartition/coalesce call, cache, layout change, or AQE
setting change was added. Iterations 4 and 5 are not carried forward.

Before choosing 32, we inspected a previous iteration-2 control's events. Its
Gold exchanges had 200 initial hash partitions, already coalesced by AQE into
four reader tasks, with about 47 MB of campaign and 92 MB of audience shuffle
writes. [Pre-measurement inspection](preinspection.json)

We chose 32 to reduce the number of initial shuffle buckets while keeping more
than four available for adaptation. It is eight initial partitions per local
worker thread, a test value rather than an optimum. Because AQE already reduced
the task count, a large scheduling benefit was not expected. No other candidate
counts were benchmarked. The historical control motivated the choice; all
performance comparisons below use newly rerun controls.

## Initial partitions, actual tasks, and files

The latest runtime plans and stage/task events confirmed the following in every
run of the completed comparison:

| Measurement | Iteration 2 | Iteration 6 |
| --- | ---: | ---: |
| Initial hash partitions in each Gold aggregation exchange | **200** | **32** |
| AQE | Enabled | Enabled |
| Gold shuffle read | Coalesced | Coalesced |
| Actual reader tasks for campaign aggregation | **4** | **5** |
| Actual reader tasks for audience aggregation | **4** | **5** |
| Ads/profile joins | Both broadcast hash | Both broadcast hash |
| Gold files per table | 4 | 5 |
| All Parquet files | 38 | 40 |

Both joins remain left outer, preserving impressions without a profile. See the
[control campaign plan](shuffle-plans/01-left/gold.campaign_daily.txt),
[variant campaign plan](shuffle-plans/01-right/gold.campaign_daily.txt), and
[per-run execution evidence](shuffle-partitions.json).

AQE combines contiguous initial partitions using runtime size information.
Changing the initial partition boundaries changes what can be combined. Here,
that produced five reading tasks instead of four. This is compatible with AQE,
not evidence that the setting was ignored. We did not isolate the contribution
of every internal grouping decision. [Spark AQE coalescing](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html#coalescing-post-shuffle-partitions)

The observer matches stage **SQL execution IDs** to the two joined Gold report
executions. It excludes Delta metadata SQL executions sharing the job group,
some of which use 50 tasks unrelated to the report's aggregation partition count.
Whole Gold job-group task attempts are 736 versus 738; those counts include
metadata work and are not the four-versus-five reader-task measurement.

## Runtime and storage

Three runs per side. Values are medians unless labeled otherwise. MB means
1,000,000 bytes. Gold job time includes joins, aggregation, Delta writes, and
associated metadata work; it is not an isolated shuffle-operator timer.

| Measurement | Control: 200 | Variant: 32 | Observation |
| --- | ---: | ---: | --- |
| Two Gold jobs combined | **7.516 s** | **7.012 s** | 6.71% lower median |
| Gold range | 7.309–7.917 s | 6.774–7.253 s | Lower combined Gold time in each pair |
| Campaign Gold median | 3.237 s | 2.949 s | 8.90% lower |
| Audience Gold median | 4.279 s | 4.063 s | 5.05% lower; mixed pairwise results |
| Whole pipeline processing | **36.183 s** | **34.978 s** | 3.33% lower median; ranges overlap |
| Processing range | 34.629–36.208 s | 33.940–35.598 s | First pair favors control |
| Full run including startup/validation | 45.573 s | 44.030 s | 3.39% lower median |
| Gold shuffle writes | 138.69 MB | 135.19 MB | 2.52% fewer bytes |
| Gold cumulative task CPU | 19.57 CPU-seconds | 18.10 CPU-seconds | 7.54% lower median |
| Gold disk and memory spill | 0 | 0 | No spill on either side |
| All table bytes | 630.85 MB | 629.78 MB | Only 0.17% smaller |

The lower initial count did not reduce Gold shuffle bytes in proportion to
200 → 32. The same logical aggregates still have to be computed and exchanged.
Physical organization and compression can affect bytes. We observed a small
reduction, not a 6.25-fold reduction in work.

All three runs on each side had identical table byte sizes. Bronze and Silver
sizes are unchanged. The roughly 1.07 MB difference lies in the Gold files;
physical organization changed despite identical logical output. Two additional
Gold files are an observed consequence, not a separately applied file-sizing
optimization. Table sizes include metadata and local sidecars, but exclude
source CSVs, reports, and event logs.

### Every timed sample, in execution order

| Run | Campaign (s) | Audience (s) | Gold combined (s) | Whole processing (s) |
| --- | ---: | ---: | ---: | ---: |
| 01-left: 200 | 3.171 | 4.138 | 7.309 | 34.629 |
| 01-right: 32 | 2.994 | 4.259 | 7.253 | 34.978 |
| 02-right: 32 | 2.901 | 3.873 | 6.774 | 33.940 |
| 02-left: 200 | 3.237 | 4.279 | 7.516 | 36.183 |
| 03-left: 200 | 3.577 | 4.340 | 7.917 | 36.208 |
| 03-right: 32 | 2.949 | 4.063 | 7.012 | 35.598 |

The combined Gold gap in the first pair is only 0.056 seconds. Campaign time is
lower in each pair, while audience time is higher in the first and lower in the
other two. The whole-pipeline medians move favorably, but overlap and background
variation limit the claim. Three local samples do not establish a universal
speedup or an ideal partition count.

## What to keep and what to learn

**Treat 32 as a modestly promising local setting, not a necessary replacement
for iteration 2.** Keep iteration 2 as the general reference for future isolated
experiments. Keep iteration 6 available when the workload calls for further
partition tuning. We do not need to find a "perfect middle number" or change
another setting to make this iteration look better.

The useful distinctions are now visible in real evidence:

- Initial shuffle partitions split map output into key-based buckets.
- AQE can combine those buckets into a different number of reading tasks.
- Storage partitioning organizes persistent tables; it was not changed here.
- Output file count can change as a consequence of execution, even with the same
  logical model and no manual storage-layout change.

This experiment tests one setting **with AQE active**. It does not measure the
same setting with AQE disabled, and it does not establish what happens on a
larger cluster. Local task bytes are not unique disk reads or network latency;
cumulative task CPU is not wall time; no cloud-dollar saving is claimed.

## Verification and provenance

- **27 automated tests passed**, including real entrypoints for 200 versus 32,
  exact fixture-table equality, runtime exchange inspection, and rejection of
  missing evidence or a failed SQL execution.
- **Six full rebuilds passed validation**, each retaining 26,557,961 impressions,
  1,366,056 clicks, and 1,528,526 impressions without a matching profile.
- **40 exact table comparisons passed:** every table in each later run matches
  the first control through bidirectional `EXCEPT ALL`, including logical types,
  values, duplicate multiplicities, and nulls. Physical row/column order and
  nullable metadata flags are not equality requirements.
- **12 successful Gold SQL executions** passed exchange and unchanged-join
  checks, with actual completed stage/task evidence.
- **234 Parquet files** passed ZSTD codec inspection; footer row/file totals
  agree with the run reports.
- Source fingerprints and recorded runtime settings match except the declared
  `spark.sql.shuffle.partitions` change. Every measured run used clean commit
  `1329cb63ae2a2a3889324a66c588c5032ab4ea55`. No failed task attempts were recorded.

Measured September 27, 2026 local time (September 28 UTC): Python 3.11.15,
Spark 4.0.1, Delta 4.0.0, Java 21.0.12.1, macOS arm64, `local[4]`, 2 GiB heap.
Fresh sequential processes alternate left/right, right/left, left/right. No
sample is discarded. OS caches are not flushed, and background activity and
JIT effects remain. Tests, cross-run equality, and plan/codec checks are outside
processing time. In-pipeline validation is separate and included in full-run time.

With user approval, only the generated tables in the old baseline-versus-itself
control comparison were removed to make space. Its evidence/logs, source data,
original baseline run, and all project versions remain. See the
[retention note](../comparison_control/README.md#local-output-retention-update).

## Evidence and reproduction

```bash
bash scripts/test_iteration.sh 6
```

This runs tests, three full rebuilds per side, exact table comparisons, actual
shuffle/AQE/join evidence, and file-codec checks. It creates a fresh comparison
ID; open its `RESULTS.md` for the outcome and links. No automatic winner is chosen.

- [Runnable experiment](../../experiments/06_shuffle_partitions/README.md)
- [Generated measurements](measurements.md)
- [All run reports and exact equality results](comparison.json)
- [Gold totals and distributions](gold-summary.json)
- [Initial partition counts, AQE reads, actual stages/tasks, and joins](shuffle-partitions.json)
- [Actual codecs](codecs.json)
- [Pre-measurement inspection](preinspection.json)
- [Central results index](../README.md)

Comparison ID: `shuffle-06-20260927`. Large tables and event/test logs remain in
the ignored local `outputs/comparisons/` directory. Published plans replace the
workspace path with `<repo>`. Tag: `iteration-06-shuffle-partitions`.
