# Comparison runner control

**Passed on September 27, 2026. No optimization was applied.** Both sides ran
`experiments/00_baseline/run.py`, unchanged from the preserved `baseline-v0.1`.
This verifies our comparison process before iteration 1.

## What was tested

- All **15 automated tests** passed before measurements began. The added tests
  catch altered duplicate counts, null handling, schema changes, and reporting
  groups with wrong click counts despite matching grand totals. They also check
  runtime/source compatibility, sample summaries, run order, and failure reporting.
- **Four full-data pipeline runs** completed: two per side in left/right then
  right/left order. Each used a fresh process and the same local resource budget.
- Every run processed **26,557,961 impressions and 1,366,056 clicks**, preserving
  **1,528,526 impressions without a user profile**.
- **24 exact table comparisons passed**: all eight tables from each of the three
  later runs matched the first run in values, data types, and duplicate counts.
- Inputs, runtime versions, machine/resources, Spark settings, and recorded Git
  state matched. Each run recorded clean code commit
  `48b80524b8383a2d6c18643af7cb82bbf7285ed1`.

## What we learned

Identical code produced pipeline times from **31.989 to 33.413
seconds**. This is direct evidence that a timing difference alone does not prove
an optimization worked.

| Run, in execution order | Pipeline processing | Pipeline total |
| --- | ---: | ---: |
| 01-left | 33.198 s | 41.893 s |
| 01-right | 32.262 s | 41.840 s |
| 02-right | 31.989 s | 41.038 s |
| 02-left | 33.413 s | 42.542 s |

The left median was **33.3055 seconds** and the right median
was **32.1255 seconds**. Although that is a
**3.54% lower right-hand median**, both sides
executed the same implementation. It is not a code improvement. OS caches,
run order, and background machine activity are not fully controlled; two samples
per side are only a control demonstration, not a stable performance estimate.

All four runs produced **925,718,749 bytes** of Delta tables each. The new runs
retain about **3.70 GB** of tables locally, plus logs. They do not overwrite the
original baseline. New comparisons default to three repetitions per side; this
control explicitly used two to cover both run orders while limiting stored output.

The exact comparison phase took **367.637 seconds**;
that expensive verification is outside pipeline performance measurements. Overall
runner time, including tests, four pipeline processes, and equality checks, was
**563.397 seconds**.

## Evidence and reproduction

- [Generated measurements](measurements.md): medians, ranges, storage, reads,
  shuffle, spills, and the individual timing observations.
- [Complete small report](comparison.json): per-run reports, fingerprints,
  runtime settings, counters, all samples, and exact equality outcomes.
- Local large artifacts: `outputs/comparisons/baseline-control-20260927/`.
  Tables, test/process logs, plans, and event logs stay out of Git.

From the repository root (choose a fresh comparison ID):

```bash
bash scripts/compare.sh --comparison-id another-baseline-control --repeats 2 \
  --change 'No optimization: unchanged baseline versus itself to verify the comparison runner'
```

See the [comparison procedure](../../docs/comparisons.md) for the experiment
interface and measurement limits. This control does not implement CSV-versus-Parquet
or any other optimization iteration.
