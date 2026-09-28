# shuffle-06-20260927

Status: **succeeded**.

Declared change: Set spark.sql.shuffle.partitions from 200 to 32; keep AQE, automatic joins and ZSTD unchanged

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/02_compression/run.py`. Right: `experiments/06_shuffle_partitions/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 36.183 [34.629–36.208] | 34.978 [33.940–35.598] | -3.33% |
| seconds.total | 45.573 [43.541–45.865] | 44.030 [42.545–44.662] | -3.39% |
| seconds.validation | 6.169 [5.870–6.503] | 6.035 [5.620–6.147] | -2.17% |
| storage.table_bytes | 630,854,482.000 [630,854,482.000–630,854,482.000] | 629,784,539.000 [629,784,539.000–629,784,539.000] | -0.17% |
| storage.parquet_files | 38.000 [38.000–38.000] | 40.000 [40.000–40.000] | +5.26% |
| processing.input_bytes | 1,594,407,963.000 [1,594,407,013.000–1,594,408,297.000] | 1,594,409,334.000 [1,594,407,202.000–1,594,410,030.000] | +0.00% |
| processing.shuffle_write_bytes | 138,725,293.000 [138,724,834.000–138,725,306.000] | 135,223,828.000 [135,222,916.000–135,224,131.000] | -2.52% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 34.629 | 43.541 |
| 01-right | right | 34.978 | 44.03 |
| 02-right | right | 33.94 | 42.545 |
| 02-left | left | 36.183 | 45.573 |
| 03-left | left | 36.208 | 45.865 |
| 03-right | right | 35.598 | 44.662 |

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
