# Iteration 5: profile broadcast versus sort-merge join

Start from **iteration 2: unpartitioned ZSTD**. The one deliberate change is
`.hint('merge')` on the user-profile relation in `enrich()`. The ads join remains
automatic. Iteration 4's ads hint and iteration 3's date partitioning are not
included. This is one runnable pipeline version.

## Learning target

Iteration 4 changed the ads join; this experiment separately changes the profile
join. It lets us examine the same strategy choice at a different point in the
pipeline, with a different join key and projected relation. Do not assume the
cost will match iteration 4 or compare its historical timings directly.

The hypothesis is that redistributing and sorting the impressions for the profile
join adds work when automatic broadcasting already suits the lookup. We will
measure Gold jobs, total processing, shuffle, task CPU, spill, and storage to see
whether that holds. A sort-merge join is one specific shuffle-join strategy;
broadcasting also has memory and transfer costs. Neither is universally best.

## Reproduce

```bash
bash scripts/test_iteration.sh 5
```

This runs automated tests, three full rebuilds per side in alternating order,
exact comparisons of all eight tables, executed join-strategy checks, and codec
inspection. Each measured run uses a fresh process. Open the generated
`outputs/comparisons/<id>/RESULTS.md` for the evidence. To run only this version:

```bash
bash scripts/run_profile_shuffle_join.sh
```

The runtime checks require the following latest plans for completed Gold SQL
executions, identified by join key:

| Join | Control: iteration 2 | Iteration 5 |
| --- | --- | --- |
| Ads, `adgroup_id` | BroadcastHashJoin | BroadcastHashJoin |
| Profiles, `user_id` | BroadcastHashJoin | SortMergeJoin |

Both remain left outer joins, preserving impressions without a profile. The
profile join changes in both Gold reports, since each independently enriches
Silver. Missing or unexpected join evidence fails the check. A hint alone does
not establish the executed strategy. [Spark join hints](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html#join-strategy-hints)

Schemas, logical output, ZSTD, layout, 2 GiB heap, `local[4]`, automatic broadcast
threshold, AQE, and shuffle settings stay unchanged. Existing automatic Spark
optimizations remain enabled. All earlier versions stay runnable.

## Measurement boundaries

Gold job times include their joins, aggregations, and Delta writes. Task CPU is
cumulative across tasks; spill counters and peak task execution memory are not
process RAM. Local shuffle bytes do not measure cross-machine network latency.
No cloud-dollar cost is measured. Physical file sizes can change even when all
logical rows are identical.

Tests, exact cross-run equality, and plan/codec checks are outside measured
processing. Each pipeline's validation is reported separately and included in
full-run time. No measured sample is discarded. OS caches are not flushed;
background activity and startup/JIT effects remain sources of variation. The
comparison reruns iteration 2 with matching source hashes and recorded runtime
settings, rather than reusing a historical timing.

## Recorded result

[The full-data findings](../../results/05_profile_shuffle_join/README.md) include
an initial failed comparison, one successful diagnostic repeat, and a completed
three-pair series. The variant was slower and one attempt failed to acquire
execution memory while sorting, under the same 2 GiB heap. The successful series
passed all 40 exact table comparisons and actual join/codec checks. This is not
an upgrade for the measured workload; earlier automatic broadcasting remains
the preferred reference. Tag: `iteration-05-profile-shuffle-join`.
