# Iteration 8: implementation verified; full-data benchmark pending

**The implementation and fixture workflow pass. Performance findings are not
available yet.** Full-data benchmarking is paused because the latest preflight
found **3.85 GiB free**, below the agreed **7 GiB minimum**. No cleanup has been
performed for this iteration.

## The experiment

Iteration 8 starts from iteration 2 and changes only
`spark.sql.adaptive.coalescePartitions.enabled` from true to false. AQE remains
enabled, initial shuffle partitions remain 200, and ZSTD, local[4], the 2 GiB
heap, automatic joins, logical models, and report order remain unchanged.
There is no iteration-7 cache. The switch applies session-wide, so validation
and Delta metadata work must be reported separately from pipeline processing.

[Runnable version](../../experiments/08_no_shuffle_coalescing/README.md)

## Verified on the fixture

- **32 regression tests passed.** New tests run the original control, observed
  control, and observed variant on the fixture containing missing profiles and
  null values. Exact comparisons cover all eight tables and verify that the
  settings observer preserves the control's results and existing settings.
- Negative tests reject missing settings, a coalesced plan contradicting the
  disabled setting, missing stage/task evidence, and unsuccessful SQL executions.
- The standalone iteration-8 shell entrypoint passed on the fixture.
- The complete `test_iteration.sh 8` workflow passed with one fixture repetition
  per side: **eight exact table comparisons**, executed coalescing evidence,
  actual file-size observations, and **23 ZSTD Parquet footer checks**.
- Iterations 0–7 were left unchanged. Iteration 8's transformation and validation
  modules are byte-for-byte copies of iteration 2's modules.

The fixture contains **five impressions**; it is a correctness and wiring check,
not a performance benchmark. Its recorded runs occurred during development
(`git_dirty: true`). The planned full-data runs will use a clean committed version.

| Fixture observation | Iteration 2 | Iteration 8 |
| --- | ---: | ---: |
| AQE enabled | true | true |
| Coalescing enabled | true | false |
| Initial shuffle partitions | 200 | 200 |
| Gold campaign reader tasks | 1 | 200 |
| Gold audience reader tasks | 1 | 200 |
| Campaign output files | 1 | 4 |
| Audience output files | 1 | 5 |

Task counts and output file counts are measured separately. Empty partitions in
this tiny dataset make file counts especially unsuitable as a proxy for task
counts. The full-data outcome may differ.

[Fixture comparison](fixture-comparison.json) ·
[Fixture execution and file evidence](fixture-coalescing-evidence.json) ·
[Fixture codec checks](fixture-codecs.json) · [Fixture suite record](fixture-suite.json)

Local logs, events, tables, and generated `RESULTS.md` remain under
`work/coalescing-comparisons/fixture-smoke/`. The standalone fixture run is under
`work/coalescing-fixture/standalone/`.

## Remaining acceptance work

After the storage prerequisite is satisfied, run:

```bash
bash scripts/test_iteration.sh 8 --comparison-id coalescing-08-full
```

This will run three fresh full-data processes per side in alternating order,
40 exact table comparisons, executed coalescing checks, and every Parquet footer
check. Compare combined Gold time, processing, validation, total time, CPU,
shuffle/spill, actual task counts/durations, and file sizes. Inspect automatic
join choices without forcing them. Preserve failed runs and keep resources fixed.

The benchmark launcher reads extra settings from the live session without
modifying older experiment files. The only permitted setting difference is the
coalescing flag. Final reader counts and file counts are observed rather than
required to equal a predetermined number. Gold execution-specific evidence
excludes other Gold stages, primarily Delta metadata work, which are retained
separately for interpretation.

## Pending storage decision

The proposed cleanup is limited to the generated Bronze/Silver/Gold tables in
`outputs/comparisons/partitioning-03-20260927`: **18 directories, approximately
3.26 GiB**. Approval remains pending. Its reports, logs, plans, source data,
every experiment version, and the original baseline must remain intact.

[Recorded preflight](storage-preflight.json) records current free bytes, the
candidate size, and that nothing was deleted. If approved, perform only that
cleanup, record a manifest, and recheck for at least 7 GiB free. If still below
that threshold, pause without deleting anything additional.

The tested implementation is the first Git milestone. The findings milestone
and `iteration-08-no-shuffle-coalescing` outcome tag are reserved for the
subsequent documented full-data outcome; this fixture success is not completion
of the performance experiment.
