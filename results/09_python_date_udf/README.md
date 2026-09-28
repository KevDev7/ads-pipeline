# Iteration 9 — Python date calculation

Status: **implemented and tested; full-data benchmark paused for storage approval**.
All 34 regression tests passed. Full-data results are not yet available; no
performance recommendation is inferred from fixture timings.

The reference is iteration 2. Only Silver impressions' reporting-date calculation
changes to a regular non-Arrow Python UDF using epoch seconds and Asia/Shanghai.
All data models, resources, other transformations, and SQL settings remain fixed.

Reproduce with `bash scripts/test_iteration.sh 9`. Run the variant alone with
`bash scripts/run_python_date_udf.sh --run-id my-python-date-run`.

## Storage gate

After tests, 3.418 GiB is free. Full-data benchmarking
requires 7 GiB. Approval is pending for removal of only generated Bronze/Silver/Gold
tables from `ads-join-04-20260927` (2,523,262,272 bytes) and
`profile-join-05-retry-20260927` (3,782,656,105 bytes).
No cleanup has been performed for iteration 9. Reports, logs, plans, failed runs,
source data, all versions, and the original baseline are preserved.

## Interpretation boundaries

The primary measurement is Silver impressions write time, including Python startup
and data transfer. Pipeline, Gold, validation, total time, task metrics and storage
provide context. Gold reads the already stored date and should not run this UDF.
JVM executor CPU and task memory omit complete Python worker CPU/RSS. Worker timing
accumulators represent cumulative work rather than elapsed pipeline time.
Python datetime has a narrower date range than Spark; this comparison establishes
equivalence for the actual dataset and tested null, midnight, leap-day and pre-epoch
cases, not for all possible Spark timestamps.

## Completed verification

- Full regression: **34 tests passed**, including previous iterations and the new
  launcher behavior. [Regression log](regression.log).
- Both real entrypoints ran on the five-impression fixture, including missing
  profiles and null dimension values. **16 exact table comparisons** passed:
  direct control versus observed control, then direct control versus variant.
- Recorded settings match exactly after accounting for observation-only metadata;
  no setting changes are declared. Earlier experiment implementations remain unchanged.
- Null dates, Shanghai midnight boundaries, a leap day, pre-epoch dates and host
  timezone independence passed. The UDF's evaluation type is regular, non-Arrow.
- Silver runtime evidence: **5 Python output rows, 43 bytes sent, 31 bytes returned**.
  Both Gold reports have no Python evaluation operator. Available worker timings
  are preserved in [fixture evidence](fixture-evidence.json), not used as speed claims.
- Tests reject missing settings/plans/metrics/tasks, contradictory Python counts,
  Arrow execution in place of the regular UDF, and unsuccessful SQL execution.
- Both shell entrypoints passed their help-command checks.

An initial development fixture failed because workers could not import the
experiment-local `tables` module. Packaging the function as a serializable closure
resolved it without changing the calculation or Spark resources. The
[failure log](development-failure.log) and [successful targeted rerun](targeted-tests.log)
are retained. No full-data run has failed or been discarded: none has started.

## Pending full-data work

The six full-data rebuilds, 40 exact comparisons, full-data Python evidence and
ZSTD footer inspection remain pending. The storage gate was respected and no old
tables were removed. Once cleanup is explicitly approved and at least 7 GiB is
free, run `bash scripts/test_iteration.sh 9` and document every sample and outcome.
The completed-findings push and `iteration-09-python-date-udf` tag are deferred
until that outcome is documented. See [machine-readable verification](verification.json).
