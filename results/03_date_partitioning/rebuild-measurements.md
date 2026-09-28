# partitioning-03-20260927

Status: **succeeded**.

Declared change: Partition Silver impressions by reporting_date; keep ZSTD

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/02_compression/run.py`. Right: `experiments/03_date_partitioning/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 36.503 [35.784–36.722] | 39.420 [38.561–40.265] | +7.99% |
| seconds.total | 46.370 [45.100–46.930] | 48.593 [47.549–49.773] | +4.79% |
| seconds.validation | 6.192 [6.127–6.354] | 6.129 [6.126–6.322] | -1.02% |
| storage.table_bytes | 630,854,482.000 [630,854,482.000–630,854,482.000] | 537,128,501.000 [537,128,501.000–537,128,501.000] | -14.86% |
| storage.parquet_files | 38.000 [38.000–38.000] | 74.000 [74.000–74.000] | +94.74% |
| processing.input_bytes | 1,594,407,893.000 [1,594,407,035.000–1,594,408,601.000] | 1,616,271,988.000 [1,616,271,746.000–1,616,283,500.000] | +1.37% |
| processing.shuffle_write_bytes | 138,725,087.000 [138,724,856.000–138,725,192.000] | 110,007,143.000 [110,006,645.000–110,008,974.000] | -20.70% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 463,619,559.000 [463,619,559.000–463,619,559.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 36.722 | 46.93 |
| 01-right | right | 39.42 | 48.593 |
| 02-right | right | 40.265 | 49.773 |
| 02-left | left | 36.503 | 46.37 |
| 03-left | left | 35.784 | 45.1 |
| 03-right | right | 38.561 | 47.549 |

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
