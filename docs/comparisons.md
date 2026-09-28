# Comparing experiments

The baseline stays frozen. The shared runner lives in `benchmarks/` and invokes
each version as a separate Python process. It runs our test suite first and stops
on test failures, failed pipeline runs, incompatible inputs/settings, or unequal
outputs. It does not change the implementation being measured.

## One command per implemented iteration

```bash
# Same typed impressions: CSV versus uncompressed Parquet
bash scripts/test_iteration.sh 1

# Compression: iteration 0 versus iteration 2
bash scripts/test_iteration.sh 2

# Date partitioning: iteration 2 versus iteration 3
bash scripts/test_iteration.sh 3

# Ads join: iteration 2 versus iteration 4
bash scripts/test_iteration.sh 4
```

Each command uses a fresh timestamped comparison ID and three runs per side.
All commands run the automated test suite first. Iterations 2, 3, and 4 run full
rebuilds and exact table comparisons, followed by date-query benchmarks for
iteration 3, executed join-strategy checks for iteration 4, and actual file-codec checks. Iteration 1 instead prepares one
Parquet copy and compares repeated reads with CSV, then verifies records/codecs. Steps
run sequentially; codec inspection follows timed queries. Pipeline/query timing
methods and implementations are unchanged.

Open `outputs/comparisons/<id>/RESULTS.md` after completion. It links the pipeline
report, raw samples, query measurements/plans where applicable, and codec checks.
`suite.json` records overall and per-step status. Suite logs live beside that
folder in `<id>-suite-logs/`; detailed comparison logs remain inside it. Failures
stop subsequent steps and leave a clearly marked incomplete report. An existing
ID is never overwritten; rerun with a fresh ID. There is no automatic resume.

Optional arguments:

```bash
bash scripts/test_iteration.sh 3 --comparison-id my-partition-check \
  --source-dir data/raw/taobao --repeats 3
```

For a lightweight smoke check, provide a tiny valid three-file source fixture
and `--repeats 1`. For iteration 3, include dates in its fixed May 8–10 query
window. This tests the workflow but does not establish performance.
Full-data commands retain multiple complete outputs and can take several GB;
`--output-dir` can direct them to another local drive. No data is downloaded or
removed, and nothing is committed or published automatically.

Iteration 1 uses a dedicated format comparison rather than full rebuilds: one
conversion, repeated matching queries, complete record equality, and codec checks.
It writes `format-comparison.json` and its own generated `README.md`. Adding a future
experiment means declaring its reference, change, and applicable checks. The
lower-level commands below remain available for custom investigations.

**What remains manual:** deciding whether tradeoffs suit the workload, explaining
unexpected behavior, and deeper inspection such as the historical layout and
Gold join-plan analyses. The command collects reproducible evidence; it does not
claim to detect every difference or automatically choose a winning version.

## Run a control comparison

```bash
bash scripts/compare.sh --comparison-id baseline-control \
  --change 'No optimization: baseline compared with itself'
```

Both sides default to `experiments/00_baseline/run.py`. Three repetitions per side
are the default: left/right, right/left, left/right. Runs execute sequentially,
never in parallel. Each starts a fresh Python/JVM process. Source files are shared;
output directories are new. An existing comparison ID is rejected.

The comparison writes `comparison.json`, a generated `README.md`, per-run pipeline
reports, output tables, process logs, plans, and event logs under the ignored
`outputs/comparisons/<comparison-id>/` directory. A full three-pair baseline control
adds about 5.6 GB of tables, plus logs and temporary verification shuffle. All
generated runs are retained; it does not delete any earlier baseline or outputs.
For testing the runner, use `--source-dir` with a tiny three-file fixture.

## Compare a future iteration

Specify `--left` and `--right` with the two experiment entrypoints, `--change`
with the one deliberate difference, and `--workload` with the work being measured.
Default repetitions are three; `--repeats` changes that explicitly. A one-pair
run is useful as a smoke check, not evidence of stable timings.

An intentional Spark SQL setting change must be declared with
`--changed-setting <key>`. This prevents unrelated configuration differences from
being mistaken for the effect of the intended change. Repeat runs on the same
side must have identical recorded settings. The declaration is not proof that
Spark used the requested strategy; inspect plans and events before explaining it.

## Correctness and fairness

- All runs must use the same source file hashes/lengths and recorded runtime
  versions, resource budget, machine/platform, and repository state.
- Every run's complete table set is compared to the first left-hand run.
  Comparison aligns columns by name and requires identical logical data types.
  Physical column order, file layout, field metadata, and top-level nullable flags
  are not compared. Row values and duplicate multiplicities must match exactly.
- Bidirectional `EXCEPT ALL` compares actual rows, including nulls; this is not a
  probabilistic hash check or just a comparison of totals. Floating-point values
  are exact for this contract. A future approximate metric needs an explicit,
  reviewed tolerance; the runner never silently adds one.
- Comparisons run after all measured pipeline processes finish. Their runtime,
  Spark jobs, and disk shuffle are not counted as pipeline processing. Ordinary
  in-pipeline validation remains measured separately just as in the baseline.
- The runner saves all samples, median, minimum, maximum, sample standard deviation,
  and the percentage change between medians. A zero reference gives no percentage.
  It never infers statistical significance or automatically declares a winner.
- OS caches are not flushed. Hashing reads source files before ingestion; earlier
  runs and checks may warm caches. Alternating order helps expose order effects
  but does not eliminate them. Keep this procedure consistent between versions.
- `pipeline_writes` is the full pipeline's measured work, while `total` includes
  its startup/validation overhead. The harness's test/equality time is separate.
  Individual phase times and task counters are retained. Physical file bytes may
  vary slightly between identical runs because execution and metadata can vary.

## Entrypoint contract

Each experiment entrypoint accepts `--source-dir`, `--output-dir`, and `--run-id`.
It writes `<output-dir>/<run-id>/report.json` following the baseline's report
structure and materializes the reported tables as Delta directories at their
reported relative names. The shared runner reads those tables independently.
Source fingerprints, runtime/settings, status, phase seconds, table file/row
statistics, storage, and Spark task metrics are required. Writes must be complete
before returning success. Extra timing phases are preserved in the raw reports;
only metrics present in every run are summarized together.

Iteration 1 has a dedicated adapter for CSV/Parquet read workloads instead of the
full-pipeline contract above. It stores the small query results in JSON outside
timed actions, checks every result and the complete underlying typed records,
and reports preparation separately. No CSV read is compared with a lakehouse
rebuild. See [its explicit method](../experiments/01_csv_vs_parquet/README.md).

## Documentation and Git

[The central results page](../results/README.md) links each experiment and records
its change, workload, findings, and limitations. After successful comparisons,
copy only the small generated JSON/Markdown summaries to a named folder under
`results/`; retain all per-run observations inside the JSON. Add a human explanation
of the evidence. Do not publish partial/failed runs as performance improvements.
Raw data, generated tables, process logs, and Spark event logs stay local.

There is no automatic GitHub CI or automatic commit from the runner. We run local
checks, review the evidence, then commit and push coherent milestones.

## Date-partitioning read workloads

Iteration 3 also uses `scripts/compare_partition_queries.sh --comparison-dir
outputs/comparisons/<id>` after a successful pipeline comparison. This reads the
existing outputs without rebuilding them. It measures one-day, three-day, and
full-period placement reports in fresh processes, rotates workload order across
repetitions, checks result rows, and captures executed scan metrics plus task
input bytes. Query timing and rebuild timing remain separate. See the
[experiment method](../experiments/03_date_partitioning/README.md) and
[measured results](../results/03_date_partitioning/README.md) for dates, warm-up
policy, limitations, and all timing samples.

## Executed join evidence

Iteration 4 automatically runs `benchmarks.join_strategies` after exact equality
checks. It reads the latest structured runtime plan for completed Gold SQL
executions from event logs and identifies joins by key. Both controls must use
broadcast hash joins; the variant must use sort-merge for ads while retaining
broadcast hash for profiles. Missing evidence or an unintended join change fails
the suite. This is stronger than inspecting the hint or an initial `explain()`.
The result is `join-strategies.json`, plus sanitized runtime plans.
