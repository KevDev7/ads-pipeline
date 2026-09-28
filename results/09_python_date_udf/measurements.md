# python-udf-09-20260928

Status: **succeeded**.

Declared change: Compute only Silver reporting_date with a non-Arrow Python UDF in Asia/Shanghai

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/02_compression/run.py`. Right: `experiments/09_python_date_udf/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 35.436 [34.096–37.582] | 37.762 [36.184–38.727] | +6.56% |
| seconds.total | 44.407 [42.527–47.137] | 46.766 [44.611–48.478] | +5.31% |
| seconds.validation | 5.866 [5.672–6.240] | 5.459 [5.277–5.574] | -6.94% |
| storage.table_bytes | 630,854,482.000 [630,854,482.000–630,854,482.000] | 630,854,482.000 [630,854,482.000–630,854,482.000] | +0.00% |
| storage.parquet_files | 38.000 [38.000–38.000] | 38.000 [38.000–38.000] | +0.00% |
| processing.input_bytes | 1,594,407,923.000 [1,594,404,465.000–1,594,408,621.000] | 1,594,406,785.000 [1,594,406,781.000–1,594,407,589.000] | -0.00% |
| processing.shuffle_write_bytes | 138,725,225.000 [138,724,010.000–138,725,400.000] | 138,724,764.000 [138,724,742.000–138,725,222.000] | -0.00% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 34.096 | 42.527 |
| 01-right | right | 36.184 | 44.611 |
| 02-right | right | 37.762 | 46.766 |
| 02-left | left | 35.436 | 44.407 |
| 03-left | left | 37.582 | 47.137 |
| 03-right | right | 38.727 | 48.478 |

## Correctness and interpretation

- 40 exact table comparisons recorded.
- Each measured run is checked against the first left-hand run, not only against totals.
- Exact schema types and row values are compared with duplicates and nulls preserved.
- Tests, full-table equality checks, and their Spark jobs are outside measured pipeline time.
- Fresh sequential processes; order alternates left/right then right/left.
- All measured runs are retained, including the first. No warm-up runs are silently discarded.
- OS caches are not flushed. Source hashing and validations may warm caches.
- A few repetitions describe variability; percentage differences are not proof of a speedup.
- Spark task bytes are not unique physical disk reads. Peak task memory is not process RAM.
- Per-run Spark settings, timing phases, counters, and source hashes are in comparison.json.
- Plans and event logs remain under the ignored local comparison directory. Inspect them to explain a mechanism.
- Human interpretation belongs in the central results page after reviewing this evidence.
