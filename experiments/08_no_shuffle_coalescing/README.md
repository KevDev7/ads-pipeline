# Iteration 8: automatic shuffle-partition combining

This runnable version starts from iteration 2 and changes only
`spark.sql.adaptive.coalescePartitions.enabled` to `false` for the Spark session.
AQE stays enabled; initial shuffle partitions stay at 200. There is no cache,
join hint, manual repartition, layout change, or resource increase. The original
transformations, ZSTD, local[4], and 2 GiB heap are retained.

```bash
bash scripts/run_no_shuffle_coalescing.sh
bash scripts/test_iteration.sh 8
```

The first command builds this version once. The second runs the regression
suite, three fresh processes per side in alternating order, 40 exact table
comparisons, executed coalescing evidence, and ZSTD footer checks. Ensure at least
7 GiB is free before starting the full-data comparison. Existing runs are never
overwritten. Arguments such as `--source-dir`, `--comparison-id`, and `--repeats`
follow the existing command contract.

The observation launcher reads effective settings from each live Spark session
without changing them or editing iteration 2. Standalone iteration 8 records
the same settings. The only declared setting difference is coalescing enabled.
Other AQE settings, including advisory size and parallelism-first behavior, are
recorded as context rather than tuned. An unset initialPartitionNum is reported
as null; the Gold execution plans establish the actual initial partition count.

`coalescing-evidence.json`, linked from the generated `RESULTS.md`, combines
settings, final SQL plans, successful stages, actual task counts/durations, joins,
and per-table file-size distributions. The observer separates joined Gold SQL
from other Gold work (primarily Delta metadata). It does not prescribe final
reader or file counts and does not force joins. Task-duration p95 and file-size
p95 use nearest rank. Task duration is not a direct scheduler-overhead metric.

Because the switch applies session-wide, it can affect processing, validation,
and metadata work. Compare both Gold reports together, overall processing, and
validation separately. Report table bytes, file counts, CPU, shuffle, and spill
alongside timing variation. A speedup is not required for a useful lesson.

[Central results index](../../results/README.md) ·
[Spark coalescing documentation](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html#coalescing-post-shuffle-partitions)

Implementation and the full fixture workflow passed. Full-data benchmarking is
paused pending storage; no performance conclusion is available yet.
See [the status and fixture evidence](../../results/08_no_shuffle_coalescing/README.md).
