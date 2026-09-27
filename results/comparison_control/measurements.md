# baseline-control-20260927

Status: **succeeded**.

Declared change: No optimization: unchanged baseline versus itself to verify the comparison runner

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/00_baseline/run.py`. Right: `experiments/00_baseline/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 33.305 [33.198–33.413] | 32.126 [31.989–32.262] | -3.54% |
| seconds.total | 42.218 [41.893–42.542] | 41.439 [41.038–41.840] | -1.84% |
| seconds.validation | 6.100 [5.908–6.292] | 6.072 [5.812–6.333] | -0.45% |
| storage.table_bytes | 925,718,749.000 [925,718,749.000–925,718,749.000] | 925,718,749.000 [925,718,749.000–925,718,749.000] | +0.00% |
| storage.parquet_files | 38.000 [38.000–38.000] | 38.000 [38.000–38.000] | +0.00% |
| processing.input_bytes | 1,828,135,025.000 [1,828,134,817.000–1,828,135,233.000] | 1,828,134,947.000 [1,828,133,633.000–1,828,136,261.000] | -0.00% |
| processing.shuffle_write_bytes | 137,747,133.500 [137,747,047.000–137,747,220.000] | 137,747,045.000 [137,746,544.000–137,747,546.000] | -0.00% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 33.198 | 41.893 |
| 01-right | right | 32.262 | 41.84 |
| 02-right | right | 31.989 | 41.038 |
| 02-left | left | 33.413 | 42.542 |

## Correctness and interpretation

- 24 exact table comparisons recorded.
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
- **A/A control:** both sides execute the same code. Timing differences are variation, not an optimization.
