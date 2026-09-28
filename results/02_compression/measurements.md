# compression-01-20260927

Status: **succeeded**.

Declared change: Parquet compression: Snappy to ZSTD across Bronze, Silver, and Gold

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/00_baseline/run.py`. Right: `experiments/01_compression/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 34.236 [33.533–34.571] | 33.934 [33.220–34.783] | -0.88% |
| seconds.total | 43.071 [42.512–44.605] | 43.066 [42.294–44.108] | -0.01% |
| seconds.validation | 5.857 [5.814–6.806] | 6.246 [5.693–6.254] | +6.64% |
| storage.table_bytes | 925,718,749.000 [925,718,749.000–925,718,749.000] | 630,854,482.000 [630,854,482.000–630,854,482.000] | -31.85% |
| storage.parquet_files | 38.000 [38.000–38.000] | 38.000 [38.000–38.000] | +0.00% |
| processing.input_bytes | 1,828,135,993.000 [1,828,133,157.000–1,828,136,197.000] | 1,594,406,777.000 [1,594,405,973.000–1,594,407,077.000] | -12.79% |
| processing.shuffle_write_bytes | 137,747,399.000 [137,746,769.000–137,747,561.000] | 138,724,497.000 [138,724,409.000–138,724,829.000] | +0.71% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 0.000 [0.000–0.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 33.533 | 42.512 |
| 01-right | right | 34.783 | 44.108 |
| 02-right | right | 33.22 | 42.294 |
| 02-left | left | 34.571 | 44.605 |
| 03-left | left | 34.236 | 43.071 |
| 03-right | right | 33.934 | 43.066 |

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
