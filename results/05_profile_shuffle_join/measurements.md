# profile-join-05-retry-20260927

Status: **succeeded**.

Declared change: Request MERGE only for the user-profile join; keep ads join automatic and ZSTD

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/02_compression/run.py`. Right: `experiments/05_profile_shuffle_join/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 32.609 [32.145–32.713] | 43.387 [43.062–43.699] | +33.05% |
| seconds.total | 40.985 [40.480–41.034] | 52.071 [51.538–52.395] | +27.05% |
| seconds.validation | 5.327 [5.287–5.499] | 5.651 [5.482–5.786] | +6.08% |
| storage.table_bytes | 630,854,482.000 [630,854,482.000–630,854,482.000] | 630,759,165.000 [628,574,329.000–630,759,165.000] | -0.02% |
| storage.parquet_files | 38.000 [38.000–38.000] | 40.000 [40.000–40.000] | +5.26% |
| processing.input_bytes | 1,594,407,855.000 [1,594,406,847.000–1,594,408,297.000] | 1,594,408,206.000 [1,594,406,810.000–1,594,409,238.000] | +0.00% |
| processing.shuffle_write_bytes | 138,725,093.000 [138,724,792.000–138,725,201.000] | 1,021,568,444.000 [1,013,777,148.000–1,021,569,009.000] | +636.40% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 564,373,600.000 [564,373,600.000–640,147,565.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 32.145 | 40.48 |
| 01-right | right | 43.387 | 52.395 |
| 02-right | right | 43.699 | 52.071 |
| 02-left | left | 32.609 | 40.985 |
| 03-left | left | 32.713 | 41.034 |
| 03-right | right | 43.062 | 51.538 |

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
