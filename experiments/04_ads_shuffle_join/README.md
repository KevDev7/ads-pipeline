# Iteration 4: ads broadcast versus sort-merge join

Start from **iteration 2: unpartitioned ZSTD**, not the date-partitioned version.
One deliberate change: add `.hint('merge')` to the projected ads relation in
`enrich()`. This requests a **sort-merge join**, one specific shuffle-join strategy.
The shared enrichment is used independently by both Gold reports, so the target
ads join changes in both reports. The user-profile join receives no hint.

## Learning target and hypothesis

The control previously used automatic broadcast hash joins for both dimensions.
Broadcasting the smaller ads relation avoids redistributing the large impressions
input for that join. Sort-merge instead normally exchanges rows by the ad key and
sorts them before matching. It may add shuffle, CPU, or spill here; this experiment
is not a promise of improvement. Broadcasting has its own memory/transfer costs,
and a different relation size, cluster, or memory budget could favor another
strategy. Local shuffle counters do not reproduce multi-machine network latency.

ZSTD, schemas, transformations, left-join semantics, partition layout, memory,
worker threads, broadcast threshold, and AQE stay unchanged. In particular, we
do not globally disable broadcasting: doing so would also affect the profile
join and confound this experiment. Spark can reject a hint or adapt its plan,
so the hint alone is not evidence. [Spark's join-hint documentation](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html#join-strategy-hints)

## Run all checks

```bash
bash scripts/test_iteration.sh 4
```

A single standalone iteration-4 rebuild is also available:

```bash
bash scripts/run_ads_shuffle_join.sh
```

The comparison command runs the test suite, then three full rebuilds per side in
alternating order and exact comparisons of all eight tables. It verifies the
latest structured runtime plan for completed Gold SQL executions, using join
keys to distinguish the ads join from the user-profile join:

| Join | Control (iteration 2) | Iteration 4 |
| --- | --- | --- |
| `adgroup_id` → ads | BroadcastHashJoin | SortMergeJoin |
| `user_id` → profiles | BroadcastHashJoin | BroadcastHashJoin |

Both remain left outer joins. Missing, ambiguous, or unexpected join evidence
fails the suite. Runtime plans are saved under `join-plans/`, and
`join-strategies.json` records the checks. Codec inspection verifies every output
file still uses ZSTD. Open `RESULTS.md` for the linked evidence.

## Measurements and limits

Compare total pipeline processing and each Gold job separately, along with
shuffle read/write bytes, task CPU time, spill, peak task execution memory, file
counts, and output bytes. Peak task execution memory is not total process RAM.
Writing different physical files can follow from a join-plan change even when
logical rows remain identical. Any such changes are consequences to document,
not additional tuning.

Each measured run starts a fresh process with the same source hashes and runtime.
OS caches are not flushed; repeated samples describe observed variation. Tests,
exact cross-run verification, and plan/codec inspection occur outside pipeline
processing timing. Ordinary pipeline validation is included in full-run totals
and reported separately. No cloud-dollar cost is measured.

Earlier iterations remain runnable. Iteration 5, if pursued later, will separately
change the profile join starting from iteration 2; it is not included here.

## Recorded result

[The completed full-data comparison](../../results/04_ads_shuffle_join/README.md)
used two repeats per side because local free disk space was limited. Gold jobs
were slower with the forced sort-merge ads join, with about five times the shuffle
writes and new disk spill. All table contents matched. The default command uses
three repeats; `--repeats 2` reproduces the recorded sample count. The implementation
and evidence are preserved as `iteration-04-ads-shuffle-join`.
