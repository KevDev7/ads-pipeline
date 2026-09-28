# Iteration 9 results: built-in date calculation versus a Python UDF

**Keep iteration 2's built-in expression for this calculation.** Replacing only
its reporting-date expression with a regular Python UDF increased median Silver
impressions build time from **8.967 to 12.845 seconds (+43.25%)**. Median pipeline
processing increased from **35.436 to 37.762 seconds (+6.56%)**, and total run time
from **44.407 to 46.766 seconds (+5.31%)**.

The Silver ranges do not overlap in these three samples per side. Whole-pipeline
ranges do overlap, so the smaller overall difference should not be treated as a
precise universal penalty. This measures the complete Silver write, including
reading, other transformations and Delta writing; it does not isolate the pure
date function's runtime or serialization cost alone.

## What this teaches

Both versions are written in Python. Calling Spark's built-in `to_date` constructs
an expression that Spark executes in its engine. A regular Python UDF adds Python
worker execution and serialized data exchange. The function runs once per row,
but rows travel in batches; Spark does not launch a new Python process per row.
[Spark 4.0.1 UDF documentation](https://spark.apache.org/docs/4.0.1/api/python/user_guide/udfandudtf.html)

Here, Spark already had an appropriate built-in expression. The experiment makes
its benefit visible rather than proposing the Python version as the new reference.
Use a Python UDF when the required logic needs it; this result supports retaining
the built-in expression for this particular date conversion.

## One deliberate change

Iteration 9 is based on iteration 2. Only Silver impressions' `reporting_date`
changes from `F.to_date('event_timestamp')` to a scalar Python UDF on
`timestamp_seconds`, returning `DateType` with `useArrow=False`. The function uses
`datetime.fromtimestamp(seconds, ZoneInfo('Asia/Shanghai')).date()` and returns
`None` for null input. The timezone object is constructed outside the per-row call.
The existing event timestamp and all other logical transformations are preserved.

There are **no declared or observed SQL-setting differences**. Both sides retain
unpartitioned ZSTD Delta tables, automatic joins, AQE, 200 initial shuffle
partitions, four local threads, and a 2 GiB driver heap. No caching, hints, warm-up
actions, Pandas UDFs or Arrow optimization were added. Iterations 0–8 are unchanged.
The observation launcher reads the effective Python Arrow setting without setting
it; standalone iteration 9 records the same metadata.

All six measured runs used clean implementation commit
`7e0c344d601b8d4b1771e6b2519fd49eb9c025a4`, Spark 4.0.1 and Delta 4.0.0.
[Runnable version](../../experiments/09_python_date_udf/README.md)

## Every full-data timing sample

Seconds. Left is iteration 2; right is iteration 9. Each run used a fresh process;
the table preserves the actual alternating execution order. No samples were omitted.

| Run | Silver impressions | Campaign | Audience | Combined Gold | Processing | Validation | Total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 01-left | 8.967 | 2.770 | 3.747 | 6.517 | 34.096 | 5.672 | 42.527 |
| 01-right | 11.827 | 2.737 | 3.892 | 6.629 | 36.184 | 5.277 | 44.611 |
| 02-right | 12.845 | 2.894 | 4.109 | 7.003 | 37.762 | 5.459 | 46.766 |
| 02-left | 8.777 | 3.020 | 4.344 | 7.364 | 35.436 | 5.866 | 44.407 |
| 03-left | 9.852 | 3.141 | 4.101 | 7.242 | 37.582 | 6.240 | 47.137 |
| 03-right | 13.121 | 2.874 | 4.039 | 6.913 | 38.727 | 5.574 | 48.478 |

Silver ranges: **8.777–9.852 s** versus **11.827–13.121 s**. Pipeline processing
ranges: **34.096–37.582 s** versus **36.184–38.727 s**. Gold medians were
**7.242 versus 6.913 s**, with overlapping ranges. Gold does not execute this UDF;
its lower sample median is not evidence that Python makes the Gold calculation
faster. Validation timing is shown separately and is not included in processing.
OS caches were not flushed; source fingerprinting and validation may warm them.
Three repeats describe observed variability, not a statistical guarantee.

[All reports and exact checks](comparison.json) · [Generated measurements](measurements.md) ·
[Detailed timing and task summary](analysis.json)

## What Spark actually executed

All three variant runs executed one regular `BatchEvalPython` operator in the
Silver data-producing SQL execution. Completed stages and task accumulator updates
prove evaluation, rather than relying on an operator name alone. Each run recorded:

- **26,557,961 Python output rows**, matching Silver impressions exactly.
- **161,738,032 bytes sent** to Python workers and **83,923,211 bytes returned**.
- **Five Silver data-write tasks**, the same as the control. Four local task slots
  do not imply exactly four tasks or files.
- No Python evaluation in either Gold execution; both read the stored Silver date.
- Broadcast hash left outer joins for both ads and profiles in both Gold reports,
  unchanged on both sides. No joins were forced to improve the result.

The native Silver plan keeps its projections within one code-generated section.
The variant has a `BatchEvalPython` boundary between two such sections; Spark also
places the independent event-timestamp calculation in the final projection.
These are automatic physical-plan effects of the single logical replacement.
The experiment does not separately attribute the slowdown to each component.

The median of each run's median Silver data-task duration rose from **7,985 to
11,489 ms**. Median cumulative JVM CPU for that data execution rose from
**26.929 to 39.422 seconds**. These execution-scoped figures exclude unrelated
Delta metadata queries in the Silver timing group; individual task durations and
group counters are retained in the linked evidence.

The Python transfer bytes represent local process communication here, not Spark
shuffle traffic or unique physical disk reads. All timed processing runs had
**zero recorded memory/disk shuffle spill** and the same **1,375 task attempts**.
Median processing JVM task CPU was **98.132 versus 108.173 seconds**, cumulative
across tasks. JVM counters exclude complete Python worker CPU and memory usage;
we cannot use them as total process CPU/RSS measurements.

| Variant run | Worker startup ms | Worker initialization ms | Worker run metric ms |
| --- | ---: | ---: | ---: |
| 01-right | 1,111 | 1,023 | 38,965 |
| 02-right | 1,101 | 1,653 | 41,771 |
| 03-right | 1,148 | 1,914 | 41,812 |

These are task-accumulated worker timing metrics, not elapsed pipeline time or
Python CPU time. They can overlap and must not be added as independent costs.
Startup and transfer costs are already included in the timed Silver write.

[Runtime execution evidence](python-udf-evidence.json) ·
[Control Silver plan](python-udf-plans/01-left/silver.impressions.txt) ·
[Python Silver plan](python-udf-plans/01-right/silver.impressions.txt)

## Storage, correctness, and reproducibility

Every run stored **630,854,482 table bytes** in **38 Parquet data files**. Silver
impressions used five files and 350,074,879 Parquet bytes in each run; its total
table bytes including metadata were 352,821,843. Per-table sizes, file counts and
row counts are retained in every run report. The UDF supplied no storage benefit.

Acceptance passed: **34 regression tests**, **six full-data rebuilds**,
**40 exact table comparisons**, and **228 ZSTD footer checks**. Equality
compares column names/types and full row values with duplicate multiplicities and nulls,
not just totals. Source fingerprints and recorded settings match across all runs.
Both Gold reports preserve 26,557,961 impressions, 1,366,056 clicks and 1,528,526
missing-profile impressions. Campaign and audience outputs contain 1,819,995 and
3,738,362 rows respectively.

Fixture tests also cover missing profiles, null values, Shanghai midnight
boundaries, a leap day, pre-epoch dates, host-timezone independence, the explicit
non-Arrow UDF type and the observation launcher's neutrality. Sixteen fixture table
comparisons passed. Evidence tests reject missing or contradictory settings,
plans, task/metric evidence and unsuccessful SQL executions. Python datetime has
a narrower domain than Spark timestamps; equivalence is established for this
dataset and tested cases, not every timestamp Spark can represent.

[Completed suite](suite.json) · [Codec evidence](codecs.json) ·
[Verification record](verification.json) · [Full regression log](full-regression.log) ·
[Original fixture evidence](fixture-evidence.json)

Reproduce the full experiment:

```bash
bash scripts/test_iteration.sh 9
```

Run only the variant:

```bash
bash scripts/run_python_date_udf.sh --run-id my-python-date-run
```

The suite's local `RESULTS.md` links pipeline, Python execution and codec checks
under `outputs/comparisons/python-udf-09-20260928/`. Full outputs, events and logs
remain there. Git stores compact reports and plans, not the datasets.

## Retained history and resource limits

The user granted standing permission to remove old generated tables when space is
needed. For this run, only Bronze/Silver/Gold directories from the old successful
`ads-join-04-20260927` and `profile-join-05-retry-20260927` comparisons were removed:
**30 directories, 6,305,918,377 bytes (5.873 GiB)**. Reports, logs, plans, correctness
evidence, source data, all implementations, failed runs and the original baseline
were preserved. The cleanup left **9.041 GiB free**, passing the 7 GiB preflight.
No other old tables were removed for this iteration.

[Cleanup manifest](cleanup.json) · [Storage preflight](storage-preflight.json) ·
[Standing retention policy](../../docs/comparisons.md#standing-permission-for-generated-table-cleanup)

The initial development fixture exposed a Python worker import failure; sending
the function as a serializable closure fixed it without changing the calculation
or resources. The [development failure log](development-failure.log) and
[successful targeted rerun](targeted-tests.log) remain available. No timed full-data
run failed or was discarded, and no resource limit was raised.
After the suite, **5.226 GiB remained free**. No task-loss, allocation-failure or out-of-memory warnings were found in the timed-build and exact-verification logs.

The tested implementation was pushed first; the completed findings are the second
milestone, tagged `iteration-09-python-date-udf`. Keep iteration 2 as the preferred
reference for future experiments.
