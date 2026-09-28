# Iteration 5 results: profile broadcast versus sort-merge join

**Keep automatic broadcasting for this workload.** Forcing the user-profile join
to sort-merge made median Gold processing **6.591 → 17.302 seconds** and full
pipeline processing **32.609 → 43.387 seconds** in the completed comparison.
It also caused a memory-allocation failure in an earlier attempt under the same
2 GiB heap. The successful retry does not erase that failure.

## The one change

Start from **iteration 2: unpartitioned ZSTD** and add `.hint('merge')` to the
projected user-profile relation in `enrich()`. The ads join receives no hint.
Iteration 4's ads change and iteration 3's date partitioning are not included.
Schemas, business logic, left-join semantics, compression, layout settings,
resource budget, broadcast threshold, AQE, and shuffle settings stay unchanged.

The latest executed plans in the completed comparison confirm:

| Join | Control: iteration 2 | Iteration 5 |
| --- | --- | --- |
| Ads, `adgroup_id` | BroadcastHashJoin | BroadcastHashJoin |
| Profiles, `user_id` | BroadcastHashJoin | SortMergeJoin |

These checks cover both Gold reports in all six runs. See the
[control audience plan](join-plans/01-left/gold.audience_daily.txt),
[variant audience plan](join-plans/01-right/gold.audience_daily.txt), and
[all executed join checks](join-strategies.json). Both joins remain left outer.

## The memory failure is part of the result

The first comparison, `profile-join-05-20260927`, passed all 24 automated tests.
Its control completed, then the variant failed while sorting during the audience
Gold job. Spark reported `SparkOutOfMemoryError`, condition
`UNABLE_TO_ACQUIRE_MEMORY`, when a sorter requested another 65,536 bytes and
received zero. This is an execution-memory allocation failure, not evidence
that the source file must fit in RAM or that disk capacity was exhausted.

The variant had completed Bronze, Silver, and the campaign report. Its audience
report was incomplete. The suite correctly stopped: its status remains **failed**,
and its full-table equality and codec steps did not run. Failed-job elapsed time
is not a valid full-pipeline runtime or a faster audience-query sample.

We then ran **one diagnostic repeat with identical settings**, which succeeded:
44.864 seconds of processing, including 17.919 seconds in Gold. We next ran **one
fresh three-pair series**, `profile-join-05-retry-20260927`. All six runs in that
series completed, and its exact equality, executed-join, and codec checks passed.
The following table summarizes the entire attempt history, not only successes:

| Attempt | Control | Profile sort-merge variant |
| --- | --- | --- |
| Initial comparison | Succeeded | Failed in audience Gold sorting |
| Standalone diagnostic repeat | No new control | Succeeded with the same 2 GiB heap |
| Fresh comparison: three pairs | All three succeeded | All three succeeded |

That is one failure among five variant attempts, with four successful control
attempts. This small, deliberately investigated sequence does **not** establish
a production failure rate. It does establish an observed intermittent failure
under this resource budget. We have not isolated the exact cause of the
run-to-run memory difference; GC timing, concurrent task memory, and adaptive
execution are possible contributors, not proven diagnoses.

All prior reports and failed execution evidence are preserved under
[prior-attempts/](prior-attempts/). The fresh-series medians below are explicitly
**conditional on completed runs**, and are not pooled with the initial control
or diagnostic repeat. Do not present this as a clean success-only performance
experiment without the failure history.

## Completed comparison measurements

Three runs per side, on all 26,557,961 impressions. Times and counters below are
medians; MB means 1,000,000 bytes. Gold time sums the two report jobs and includes
joins, aggregation, and Delta writes, not just the join operator.

| Measurement | Automatic broadcast control | Profile sort-merge |
| --- | ---: | ---: |
| Two Gold jobs combined | **6.591 s** | **17.302 s** |
| Gold range | 6.417–6.805 s | 17.203–17.863 s |
| Gold shuffle writes | **138.69 MB** | **1,021.54 MB** |
| Gold cumulative task CPU | 17.15 CPU-seconds | 48.07 CPU-seconds |
| Gold disk spill | **0 MB** | **564.37 MB** |
| Whole pipeline processing | **32.609 s** | **43.387 s** |
| Processing range | 32.145–32.713 s | 43.062–43.699 s |
| Full run, including startup and validation | 40.985 s | 52.071 s |
| All output tables | 630.85 MB | 630.76 MB |
| Parquet file count | 38 | 40 |

Within this completed series, Gold processing took **2.63 times** as long,
shuffle writes were **7.37 times** as large, and whole processing was **33.05%
longer**. Every paired variant was slower. These local observations do not
predict the same percentages on a distributed cluster.

The first variant's Gold spill was 640.15 MB; the other two spilled 564.37 MB.
Shuffle writes ranged from 1,013.74 to 1,021.54 MB. The median memory-spill counter
was 2,973,759,248 bytes; this is the pre-serialization representation of spilled
data, not peak RAM use. CPU time is cumulative across tasks, not wall-clock time.

Storage is effectively unchanged at the median (about 0.015% smaller). The first
variant occupies 628.57 MB; the other two occupy 630.76 MB. Each variant writes
five files per Gold table versus four in the control. Bronze and Silver byte sizes
are identical. The difference lies in Gold physical files/encoding; exact logical
results still match. AQE remains enabled; we did not manually tune file counts
or layout. Sizes include table metadata/local sidecars but exclude source CSVs,
event logs, and reports.

### Every timed sample in the completed series

| Run, in execution order | Campaign (s) | Audience (s) | Gold combined (s) | Whole processing (s) |
| --- | ---: | ---: | ---: | ---: |
| 01-left: control | 2.925 | 3.666 | 6.591 | 32.145 |
| 01-right: profile sort-merge | 8.546 | 8.657 | 17.203 | 43.387 |
| 02-right: profile sort-merge | 9.269 | 8.594 | 17.863 | 43.699 |
| 02-left: control | 2.787 | 3.630 | 6.417 | 32.609 |
| 03-left: control | 2.956 | 3.849 | 6.805 | 32.713 |
| 03-right: profile sort-merge | 8.559 | 8.743 | 17.302 | 43.062 |

## What to learn

Broadcasting lets Spark look up the projected profile relation while processing
the impressions. The variant's plan instead exchanges data by `user_id` and sorts
it before the profile join. This experiment shows the additional shuffle, CPU,
and spill that can result. Gold aggregation already shuffles in the control,
which is why the control has nonzero shuffle writes.

A shuffle join can work well when broadcasting is unsuitable, but it still needs
execution memory. Spark uses that memory for sorting, joining, shuffling, and
aggregation. Spilling does not mean a job can always finish with arbitrarily
little memory. [Spark memory management](https://spark.apache.org/docs/4.0.1/tuning.html#memory-management-overview)

**Small-data correctness, full-data correctness, speed, and reliability are
separate questions.** Our fixture checks passed, successful full-data outputs
match exactly, the variant is slower here, and one full-data attempt failed.
That combination is a useful experiment result, not a reason to hide the failure
or quietly raise the memory budget. A memory/partition tuning experiment could
be a separate future iteration; it is not mixed into this one.

Keep iteration 2 as the preferred reference. Keep iteration 5 runnable for the
learning record. Iterations 4 and 5 independently explore two join locations;
we do not compare their historical seconds as if measured head-to-head. Neither
implies that every shuffle-join strategy is worse on every dataset. This local
JVM does not reproduce network latency between machines, and no cloud-dollar
savings or cluster-wide broadcast memory estimate is claimed.

## Correctness and method

- **24 automated tests passed** before the initial series and again before the
  fresh series. These are the same 24 tests, including expanded fixture coverage
  of the control and both join iterations, exact rows, and actual join strategies.
- **Six full rebuilds passed validation** in the completed series, retaining
  26,557,961 impressions, 1,366,056 clicks, and 1,528,526 impressions without a
  matching user profile.
- **40 exact table comparisons passed** in that series: all eight tables from
  each later run match its first control using bidirectional `EXCEPT ALL`.
  Values, logical types, nulls, and duplicate multiplicities match. Physical
  row/column order and nullable metadata flags are not equality requirements.
- **12 completed Gold executions** passed the expected join checks.
- **234 Parquet files** passed actual ZSTD codec inspection; footer rows, file
  counts, and bytes agree with the reports.
- Recorded source fingerprints and runtime/SQL settings match. All timed attempts
  used clean commit `71ca428e42978ff4597fd5156bd5f72ba9dff32c`.
  No failed task attempts occurred in the completed fresh series; the earlier
  failed attempt is recorded separately.

Measured September 27, 2026 local time (September 28 UTC), using Python 3.11.15,
Spark 4.0.1, Delta 4.0.0, Java 21.0.12.1, macOS arm64, `local[4]`, and a 2 GiB
heap. Fresh sequential processes alternate left/right, right/left, left/right.
OS caches are not flushed, and background activity and JIT effects remain.
Tests, cross-run equality, and plan/codec inspection are outside pipeline timing.
In-pipeline validation is separate from processing and included in full-run time.

## Evidence and reproduction

```bash
bash scripts/test_iteration.sh 5
```

This creates a fresh comparison ID, runs the tests and three pairs, then checks
all tables, executed joins, and codecs. It stops and marks the suite failed if
a run fails. Given the observed memory failure, successful completion under this
resource budget is not guaranteed. The failed report and logs are retained.

The standalone repeat used the existing wrapper, without changing its memory:

```bash
bash scripts/run_profile_shuffle_join.sh --run-id my-profile-join-check
```

- [Runnable experiment](../../experiments/05_profile_shuffle_join/README.md)
- [Generated fresh-series measurements](measurements.md)
- [All fresh-series samples and exact comparisons](comparison.json)
- [Derived Gold totals and distributions](gold-summary.json)
- [Executed join checks](join-strategies.json)
- [Actual codec checks](codecs.json)
- [Earlier attempt reports, including the failure](prior-attempts/README.md)
- [Central results index](../README.md)

Large outputs and event/test logs remain local under ignored `outputs/`. Published
plans use `<repo>` in place of the workspace path. Tag:
`iteration-05-profile-shuffle-join`. All earlier implementations remain available.
