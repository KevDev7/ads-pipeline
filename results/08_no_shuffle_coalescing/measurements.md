# coalescing-08-20260928

Status: **succeeded**.

Declared change: Disable only spark.sql.adaptive.coalescePartitions.enabled; keep AQE and 200 initial partitions

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/02_compression/run.py`. Right: `experiments/08_no_shuffle_coalescing/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 35.428 [34.984–36.037] | 38.580 [37.633–39.149] | +8.90% |
| seconds.total | 44.653 [44.153–45.049] | 48.674 [47.644–49.616] | +9.00% |
| seconds.validation | 5.898 [5.815–6.074] | 6.983 [6.815–7.156] | +18.40% |
| storage.table_bytes | 630,854,482.000 [630,854,482.000–630,854,482.000] | 631,937,964.000 [631,937,964.000–631,937,964.000] | +0.17% |
| storage.parquet_files | 38.000 [38.000–38.000] | 430.000 [430.000–430.000] | +1031.58% |
| processing.input_bytes | 1,594,405,157.000 [1,594,404,747.000–1,594,407,791.000] | 1,594,396,747.000 [1,594,395,767.000–1,594,399,697.000] | -0.00% |
| processing.shuffle_write_bytes | 138,724,645.000 [138,723,562.000–138,724,923.000] | 138,718,894.000 [138,718,793.000–138,720,035.000] | -0.00% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 34.984 | 44.153 |
| 01-right | right | 39.149 | 49.616 |
| 02-right | right | 38.58 | 48.674 |
| 02-left | left | 35.428 | 44.653 |
| 03-left | left | 36.037 | 45.049 |
| 03-right | right | 37.633 | 47.644 |

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
