# Iteration 4 results: ads broadcast versus sort-merge join

**Automatic broadcasting is the better choice for this measured workload.**
Forcing the ads join to sort-merge made the two Gold jobs take a median of
14.871 seconds instead of 7.947 seconds, with about five times the shuffle writes
and 440 MB of disk spill. The logical output is unchanged.

## The one change

Start from **iteration 2: unpartitioned ZSTD**, and add `.hint('merge')` to the
projected ads relation in `enrich()`. This affects the ads join in both Gold
reports. The user-profile join stays automatic; no global broadcast threshold,
AQE, shuffle partition count, resource budget, schema, or compression setting
changes. Iteration 3's date partitioning is not included.

The latest runtime plans for all eight completed Gold SQL executions confirmed:

| Join | Iteration 2 control | Iteration 4 |
| --- | --- | --- |
| Ads, using `adgroup_id` | BroadcastHashJoin | SortMergeJoin |
| Profiles, using `user_id` | BroadcastHashJoin | BroadcastHashJoin |

Both joins remain left outer joins. This is evidence from execution, not an
assumption based on a hint. See the [control campaign plan](join-plans/01-left/gold.campaign_daily.txt),
[sort-merge campaign plan](join-plans/01-right/gold.campaign_daily.txt), and
[checks for every run](join-strategies.json).

## Measurements

Two full rebuilds per version, on all 26,557,961 impressions. Values below are
medians; MB means 1,000,000 bytes. Gold time is the sum of the campaign and audience
jobs, including their joins, aggregations, and Delta writes. It is not the time
of the join operator alone.

| Measurement | Iteration 2: automatic broadcast | Iteration 4: ads sort-merge |
| --- | ---: | ---: |
| Two Gold jobs combined | **7.947 s** | **14.871 s** |
| Gold time range | 7.112–8.782 s | 14.480–15.262 s |
| Campaign Gold job | 3.414 s | 7.064 s |
| Audience Gold job | 4.533 s | 7.808 s |
| Gold shuffle writes | **138.69 MB** | **695.63 MB** |
| Gold cumulative task CPU | 20.80 CPU-seconds | 41.15 CPU-seconds |
| Gold disk spill | **0 MB** | **440.18 MB** |
| Whole pipeline processing | 38.993 s | 43.091 s |
| Processing range | 34.577–43.409 s | 42.303–43.878 s |
| Full run, including startup and validation | 48.819 s | 53.083 s |
| All output tables | 630.85 MB | 630.78 MB |
| Parquet file count | 38 | 38 |

The Gold median was **87.13% longer**, shuffle writes were **5.02 times** as large,
and cumulative task CPU nearly doubled. Both variant runs spilled exactly
440,180,476 bytes to disk during Gold processing; neither control did. Their
memory-spill counter was 2,281,699,520 bytes. That counter describes the
pre-serialization representation of spilled data, not peak memory use.

Storage is effectively unchanged: the variant is only 77,828 bytes smaller
(about 0.012%). Bronze and Silver table sizes are identical; small changes in
Gold physical files account for the difference. Output sizes include table
metadata and local sidecars, but exclude source CSVs, reports, and event logs.
Every Parquet column chunk still uses ZSTD.

### Every timed run, in execution order

| Run | Version | Campaign (s) | Audience (s) | Gold combined (s) | Whole processing (s) |
| --- | --- | ---: | ---: | ---: | ---: |
| 01-left | Control | 3.052 | 4.060 | 7.112 | 34.577 |
| 01-right | Ads sort-merge | 7.715 | 7.547 | 15.262 | 43.878 |
| 02-right | Ads sort-merge | 6.412 | 8.068 | 14.480 | 42.303 |
| 02-left | Control | 3.776 | 5.006 | 8.782 | 43.409 |

**Whole-pipeline timing is less conclusive than Gold timing.** Its median is
10.51% higher for the variant, but ranges overlap, and the second control is
slower overall than its paired variant. That control also slowed in unchanged
Bronze and Silver jobs. We retain it, rather than discard an inconvenient sample.
Background activity, OS caching, and runtime variation were not controlled enough
to attribute that change. Do not read 10.51% as a reliable overall slowdown.
The Gold jobs, shuffle counters, and spill tell a more consistent story here.

## Why this happened and what to learn

With broadcasting, Spark builds the smaller ads side for lookup while processing
impressions. With sort-merge, the executed plan redistributes rows by `adgroup_id`
and sorts both inputs before joining. In this experiment, that extra work appears
as substantially more shuffle, CPU work, and temporary disk spill. Existing Gold
aggregations still shuffle in the control, which is why its shuffle count is not
zero. The saved plans show the exchanges and sorts around the changed join.

A sort-merge join is one type of shuffle join. This result does not mean shuffle
joins are generally bad: the suitable strategy depends on relation sizes,
existing layout, memory, and the cluster. Broadcasting also has memory and
transfer costs; these counters do not fully measure broadcast memory. This is
one local Spark JVM, so shuffle bytes do not measure cross-machine network cost.
[Spark's join strategy documentation](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html#join-strategy-hints)

Keep iteration 2's automatic broadcast behavior as the preferred reference for
this workload. Keep iteration 4 as a runnable learning experiment that makes the
cost of an unsuitable join strategy visible. The conceptual and logical models
stay the same; execution changes. Iteration 5 is not part of this work.

## Correctness and measurement limits

- **24 automated tests passed**, including real-entrypoint fixture checks for
  exact table equality and the actual ads/profile join strategies. Negative tests
  reject missing evidence and an unintended profile-join change.
- **Four full rebuilds passed validation**, retaining 26,557,961 impressions,
  1,366,056 clicks, and 1,528,526 impressions with a missing user profile.
- **24 full-data table comparisons passed:** all eight tables in each later run
  exactly match the first control using bidirectional `EXCEPT ALL`. Values,
  types, duplicates, and nulls are checked; row order and nullable metadata flags
  are not equality requirements.
- **Eight completed Gold executions** passed the runtime join-strategy checks.
- **152 Parquet files** passed actual codec inspection, with footer row/file
  totals and byte sizes matching the run reports.
- Source hashes and recorded runtime/SQL settings match across all four runs.
  All used clean commit `326dcb6864cc5f6cffdfb8ec86e90baf5bc02b5e`.
  No failed task attempts were recorded in the measured pipeline operations.

Measured on September 27, 2026 local time (September 28 UTC): Python 3.11.15,
Spark 4.0.1, Delta 4.0.0, Java 21.0.12.1, macOS arm64, `local[4]`, 2 GiB heap.
Each timed rebuild used a fresh process. No sample was discarded or labeled a
warm-up. OS caches were not flushed. The newly rerun control, not a historical
iteration-2 timing, is the comparison denominator.

We used **two paired repeats because local free disk space was limited**. Earlier
outputs were preserved. The command still defaults to three repeats per version;
two samples give less confidence about timing variability. Tests, cross-run table
equality, and plan/codec checks are outside pipeline processing time. Each
pipeline's own validation is measured separately and included in full-run time.
We measure local seconds and bytes, not cloud-dollar savings.

## Evidence and reproduction

Run the complete automated procedure:

```bash
bash scripts/test_iteration.sh 4
```

To use the same sample count as this recorded comparison:

```bash
bash scripts/test_iteration.sh 4 --repeats 2
```

Each invocation creates a new comparison ID and preserves its outputs. Open its
`RESULTS.md` for the suite status and linked evidence. It runs tests, repeated
pipelines, exact table equality, executed join checks, and actual codec checks.
It does not automatically decide which version is better.

- [Runnable experiment](../../experiments/04_ads_shuffle_join/README.md)
- [Generated pipeline measurements](measurements.md)
- [Raw measurements and exact equality results](comparison.json)
- [Gold totals and distributions derived from those measurements](gold-summary.json)
- [Executed join checks](join-strategies.json)
- [Actual codec checks](codecs.json)
- [Central results index](../README.md)

Comparison ID: `ads-join-04-20260927`. Large tables and event/test logs remain in
the ignored local `outputs/comparisons/` directory. Published plans replace the
workspace path with `<repo>`. The implementation and results are preserved by
tag `iteration-04-ads-shuffle-join`; earlier versions remain available.
