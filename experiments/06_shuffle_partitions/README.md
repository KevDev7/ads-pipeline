# Iteration 6: initial shuffle partitions, 200 versus 32

Start from **iteration 2: unpartitioned ZSTD with automatic joins**. There is
exactly one deliberate tuning change:

```python
.config('spark.sql.shuffle.partitions', '32')
```

The reference uses 200. AQE, broadcast behavior, compression, table layout,
transformations, `local[4]`, and the 2 GiB heap stay unchanged. No repartition or
coalesce call, join hint, cache, or storage-partition change is added. This is one
version with one alternative count, not a sweep through multiple settings.

## Why 32, chosen before measurement

Inspection of the previous iteration-2 control's event logs showed that both
Gold aggregation exchanges start at 200 partitions, but AQE coalesces the reads
into **four actual tasks**. The campaign shuffle wrote about 47 MB; the audience
shuffle about 92 MB. These numbers describe that prior full-data run, not a
hardcoded requirement for future runs.

We therefore are not fixing 200 actual reducer tasks competing for four cores:
AQE already avoids that. The hypothesis is narrower: starting at 32 reduces
shuffle fan-out while leaving eight initial partitions per local worker thread
for AQE to combine as needed. It is a reasonable test value for this small local
workload, not a calculated optimum or a general production recommendation. A
small or neutral improvement is plausible because AQE already coalesces tasks.

The initial number influences how map output is split. The eventual reading
task count may differ. [Spark's shuffle and AQE documentation](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html#coalescing-post-shuffle-partitions)

This global SQL setting can affect multiple operations; effects on timings,
physical output files, or automatic task choices are consequences of the one
setting, not additional manual optimizations. Delta metadata jobs can have their
own partition counts and must not be mistaken for the Gold aggregation.

## Run the complete experiment

```bash
bash scripts/test_iteration.sh 6
```

This runs the test suite, three full rebuilds per side in fresh alternating
processes, exact comparisons of all eight tables, actual shuffle/plan checks,
and every output file's codec check. Open the generated
`outputs/comparisons/<id>/RESULTS.md` for the evidence.

To run this version alone:

```bash
bash scripts/run_shuffle_partitions.sh
```

## What we verify and measure

`shuffle-partitions.json` checks that the actual Gold exchange specifications
use 200 versus 32, AQE remains enabled, and both ads/profile joins remain broadcast
left outer joins. It records `AQEShuffleRead` nodes and actual shuffle-reader
stages/tasks from event logs. Those tasks are observed, not required to equal 32
or four. Gold SQL execution IDs distinguish report processing from Delta metadata
SQL sharing the same job group. Failed or incomplete evidence fails the check.
Sanitized runtime plans are saved under `shuffle-plans/`.

Compare whole-pipeline and Gold-job time, shuffle, task CPU, spill, actual reader
tasks, output bytes, and file counts. Gold time includes joins, aggregation, and
Delta writes; it is not isolated shuffle time. Task bytes are not unique physical
disk reads. Storage partitions, initial shuffle partitions, AQE reading tasks,
and output file counts are different measurements.

Tests, exact cross-run equality, and plan/codec inspection are outside processing
time. Each pipeline's validation is measured separately and included in full-run
time. All samples are retained; OS caches are not flushed. Local seconds/bytes
are not cloud costs. The reference is rerun; historical measurements are used
only to motivate the candidate, not as the performance denominator.
