# ads-join-04-20260927

Status: **succeeded**.

Declared change: Request MERGE only for the ads join; keep user-profile join automatic and ZSTD

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/02_compression/run.py`. Right: `experiments/04_ads_shuffle_join/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 38.993 [34.577–43.409] | 43.090 [42.303–43.878] | +10.51% |
| seconds.total | 48.819 [43.548–54.090] | 53.083 [51.823–54.343] | +8.73% |
| seconds.validation | 6.552 [5.783–7.320] | 6.627 [6.235–7.019] | +1.15% |
| storage.table_bytes | 630,854,482.000 [630,854,482.000–630,854,482.000] | 630,776,654.000 [630,776,654.000–630,776,654.000] | -0.01% |
| storage.parquet_files | 38.000 [38.000–38.000] | 38.000 [38.000–38.000] | +0.00% |
| processing.input_bytes | 1,594,407,433.000 [1,594,407,037.000–1,594,407,829.000] | 1,594,408,074.000 [1,594,407,539.000–1,594,408,609.000] | +0.00% |
| processing.shuffle_write_bytes | 138,725,014.500 [138,724,846.000–138,725,183.000] | 695,663,081.000 [695,662,962.000–695,663,200.000] | +401.47% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 440,180,476.000 [440,180,476.000–440,180,476.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 34.577 | 43.548 |
| 01-right | right | 43.878 | 54.343 |
| 02-right | right | 42.303 | 51.823 |
| 02-left | left | 43.409 | 54.09 |

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
