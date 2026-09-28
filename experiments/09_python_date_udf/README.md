# Iteration 9 — built-in date calculation versus a Python UDF

Based on iteration 2, with one deliberate change: calculate Silver impressions'
`reporting_date` with a regular scalar Python UDF (`DateType`, `useArrow=False`)
instead of Spark's built-in date expression. The function consumes epoch seconds,
uses `Asia/Shanghai` explicitly, and preserves nulls. The existing
`event_timestamp` calculation is unchanged.

Run `bash scripts/run_python_date_udf.sh --run-id my-python-date-run`, or run the
comparison with `bash scripts/test_iteration.sh 9`. The comparison rebuilds both
versions three times in fresh processes, alternating order, checks all eight
tables against the first control (40 exact comparisons), verifies execution
evidence, and inspects every generated Parquet footer for ZSTD.

No SQL settings are deliberately changed. Automatic joins, AQE/coalescing,
200 initial shuffle partitions, local[4], 2 GiB driver heap, unpartitioned ZSTD
Delta tables, validations, and report order remain as in iteration 2. No cache,
warm-up action, Arrow optimization, or Pandas UDF is added.

The Silver write materializes the date and includes Python worker startup,
serialization, calculation, and return conversion. Gold reads the persisted
Silver date. The evidence checker verifies executed Python output rows and bytes,
not just a plan label. JVM task CPU/memory do not capture complete Python worker
resource usage, and cumulative worker times are not wall-clock duration.

This is an educational contrast against an already efficient built-in expression,
not a promised improvement. Python datetime does not cover Spark's full timestamp
domain; equivalence is tested for this dataset and documented boundary cases.

See [central findings](../../results/09_python_date_udf/README.md).
