# cache-07-20260928

Status: **succeeded**.

Declared change: Persist full enrichment for both Gold reports; blocking scoped release

Workload: Full three-source Bronze/Silver/Gold rebuild

Left: `experiments/02_compression/run.py`. Right: `experiments/07_cached_enrichment/run.py`.

## Measurements

Median [minimum–maximum]. Positive change means the right-hand value is larger.

| Measurement | Left | Right | Change |
| --- | ---: | ---: | ---: |
| seconds.pipeline_writes | 36.412 [33.899–37.325] | 44.540 [43.294–44.752] | +22.32% |
| seconds.total | 45.921 [43.175–47.492] | 53.560 [52.072–54.009] | +16.64% |
| seconds.validation | 6.643 [6.034–6.956] | 6.053 [5.839–6.203] | -8.88% |
| storage.table_bytes | 630,854,482.000 [630,854,482.000–630,854,482.000] | 626,073,326.000 [626,073,326.000–626,073,326.000] | -0.76% |
| storage.parquet_files | 38.000 [38.000–38.000] | 38.000 [38.000–38.000] | +0.00% |
| processing.input_bytes | 1,594,406,097.000 [1,594,405,211.000–1,594,407,823.000] | 3,301,168,670.000 [3,301,168,537.000–3,301,169,898.000] | +107.05% |
| processing.shuffle_write_bytes | 138,724,566.000 [138,724,048.000–138,725,044.000] | 123,292,694.000 [123,292,509.000–123,292,710.000] | -11.12% |
| processing.disk_spill_bytes | 0.000 [0.000–0.000] | 89,302,474.000 [88,081,410.000–89,421,192.000] | n/a (left is zero) |

## Individual runs

| Run | Side | Measured work (s) | Pipeline total (s) |
| --- | --- | ---: | ---: |
| 01-left | left | 37.325 | 47.492 |
| 01-right | right | 44.54 | 53.56 |
| 02-right | right | 44.752 | 54.009 |
| 02-left | left | 36.412 | 45.921 |
| 03-left | left | 33.899 | 43.175 |
| 03-right | right | 43.294 | 52.072 |

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
