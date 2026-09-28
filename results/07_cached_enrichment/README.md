# Iteration 7 results: reuse cached enrichment

**The cache worked, but it made this two-report workload slower.** Median combined
Gold time increased from **7.390 to 17.178 seconds (+132.45%)**, including cache
registration, materialization, and blocking release. Median pipeline processing
increased from **36.412 to 44.540 seconds (+22.32%)**. Keep iteration 2 as the
working reference; preserve iteration 7 as the caching experiment.

## One deliberate change

Iteration 7 starts from iteration 2 and persists the existing, full enriched
impressions DataFrame with `StorageLevel.MEMORY_AND_DISK_DESER`. The campaign
write fills the cache; the audience write reuses the same DataFrame. There is no
warm-up count, new projection/filter, join hint, repartition, or SQL-setting
change. The transformations, report order, unpartitioned ZSTD tables, AQE,
200 initial shuffle partitions, local[4], and 2 GiB heap are unchanged.

The full enrichment has **14 columns**. Caching retains columns individual
reports would otherwise not need. For example, the control campaign scan reads
four impression columns and only the profile key; building the cache reads nine
impression columns and the profile age as well. This is an automatic consequence
of caching the existing output, not a second deliberate optimization.

[Runnable version](../../experiments/07_cached_enrichment/README.md) ·
[Spark persistence API](https://spark.apache.org/docs/4.0.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html)

## Every timed sample

Seconds, fresh process per sample. The actual order was 01-left, 01-right,
02-right, 02-left, 03-left, 03-right. Left is iteration 2; right is iteration 7.

| Run | Campaign | Audience | Release | Combined Gold | Pipeline processing | Total run |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 01-left | 3.651 | 4.201 | — | 7.852 | 37.325 | 47.492 |
| 01-right | 12.638 | 4.519 | 0.021 | 17.178 | 44.540 | 53.560 |
| 02-right | 12.309 | 5.054 | 0.022 | 17.385 | 44.752 | 54.009 |
| 02-left | 3.455 | 3.935 | — | 7.390 | 36.412 | 45.921 |
| 03-left | 2.924 | 3.853 | — | 6.777 | 33.899 | 43.175 |
| 03-right | 12.272 | 4.124 | 0.022 | 16.418 | 43.294 | 52.072 |

The control Gold range was **6.777–7.852 s**; cached Gold was **16.418–17.385 s**.
Pipeline processing ranges were **33.899–37.325 s** and **43.294–44.752 s**.
The cached audience report was slower in every paired repetition too: median
**3.935 → 4.519 s**. Blocking release itself was small (0.021–0.022 s).

Processing includes Bronze, Silver, both Gold writes, and cache release. Total
run also includes source fingerprinting, Spark startup/shutdown, validation,
and measurement overhead. The 40 cross-run exact comparisons happen afterward
and are excluded from these times. OS caches were not flushed. Three local
repetitions establish the observed outcome, not universal performance bounds.

[All pipeline samples and comparisons](comparison.json) ·
[Generated measurements](measurements.md) · [Combined Gold metrics](gold-summary.json)

## Proof that the cache was used

All three variants produced the same lifecycle observations. RDD identity was
340 within each independent process; IDs are only meaningful within a run.
All five partitions were present after campaign, before audience, and after
audience. Blocking release removed the cache registration and storage blocks.

| Snapshot | Cached / total partitions | Memory bytes | Cache disk bytes |
| --- | ---: | ---: | ---: |
| After campaign | 5 / 5 | 772,217,368 | 6,860,306 |
| Before audience | 5 / 5 | 772,217,368 | 6,860,306 |
| After audience | 5 / 5 | 570,528,808 | 142,723,458 |
| After release | 0 / 0, unregistered | 0 | 0 |

These are metadata-only point-in-time observations, **not peak process memory**.
Cache disk bytes are stored RDD blocks, not shuffle spill or persistent tables.

The runtime evidence confirms more than an `InMemoryTableScan` name:

- Each campaign cache scan produced **26,557,961 rows**, after executing both
  cache-building joins. Their output-row task updates totaled 53,115,922
  (one impression output per join); source scans totaled 28,466,540 rows.
- Each audience cache scan produced **26,557,961 rows**, with **zero additional
  source-scan or join output-row task updates** in that SQL execution.
- The audience stages referenced the same cached RDD observed in storage.
  Every selected SQL execution and stage completed successfully, with no failed
  task attempts. The first report had an additional five-task cache-fill stage;
  the second needed neither dimension-build stage. Final aggregation reader
  stages remained at four tasks per report.
- Both ads and profile joins remained **broadcast hash left outer joins** in
  every control and cache build. No automatic switch to shuffle joins occurred.
  The cached audience plan still displays that original join lineage; it is not
  evidence of executing the joins again.

[Automatic cache evidence, every run](cache-evidence.json) ·
[Control campaign plan](cache-plans/01-left/gold.campaign_daily.txt) ·
[Cache-building campaign plan](cache-plans/01-right/gold.campaign_daily.txt) ·
[Cache-reusing audience plan](cache-plans/01-right/gold.audience_daily.txt)

## Costs and interpretation

| Gold metric, median unless stated | Iteration 2 | Iteration 7 |
| --- | ---: | ---: |
| Combined Gold wall time | 7.390 s | 17.178 s |
| Cumulative task CPU | 19.077 s | 54.368 s |
| Shuffle writes | 138.692 MB | 123.260 MB |
| Shuffle disk spill | 0 | 89.302 MB |
| Shuffle memory spill, uncompressed accounting | 0 | 818.945 MB |
| Persistent tables, each run | 630.854 MB | 626.073 MB |
| Parquet files, each run | 38 | 38 |

MB means decimal million bytes. Memory spill is Spark's spill counter, not RAM
resident at one instant. Cache storage is separately listed above. Task input
counters include reading cached blocks and must not be interpreted as physical
disk traffic; all raw counters are retained in the evidence.

The saved join work did not pay for materializing and storing the full result.
The wider cache, substantial CPU increase, disk-backed cache blocks, and new
shuffle spill are consistent with caching and aggregation competing for the
fixed memory budget. This experiment does not isolate how much of the slowdown
came from each internal cost. It does establish that avoiding recomputation
alone does not guarantee a faster pipeline.

Persistent table sizes decreased by **0.76%**, with the same file counts and
logical records. Physical byte layout is not required to match: execution can
change row ordering and encoding. This small storage difference is not a reason
to retain a much slower processing path. It is separate from temporary cache
storage, which disappears on release.

## Correctness and reproducibility

The completed suite passed **30 regression tests**, six full-data rebuilds,
and **40 exact table comparisons** (each of the five later runs' eight tables
against the first control). All **228 Parquet footers** report ZSTD.

Both real entrypoints passed the fixture containing missing profiles and nulls,
with all eight tables exactly equal. Automated tests verify the requested
storage level, actual materialization/reuse/release, identical recorded settings,
rejection of missing/contradictory storage and execution evidence, cleanup after
an injected failure between reports, and preservation of the original error
when cleanup also fails.

Each report preserves **26,557,961 impressions**, **1,366,056 clicks**, and
**1,528,526 missing-profile impressions**. Campaign and audience Gold tables have
1,819,995 and 3,738,362 rows respectively. Exact checks cover schemas, records,
nulls, and duplicate multiplicity, not only these totals.

All six measured runs recorded clean commit `88d7a77b9be5931512d02be8479251000a046464`,
Spark 4.0.1, Delta 4.0.0, Python 3.11.15, Java 21.0.12.1, and identical source
fingerprints and recorded settings. Iterations 0–6 are unchanged. The documented
outcome is tagged `iteration-07-cached-enrichment`.

[Codec checks](codecs.json) · [Suite completion record](suite.json)

Run the complete experiment with:

```bash
bash scripts/test_iteration.sh 7
```

Run a single cached build with:

```bash
bash scripts/run_cached_enrichment.sh
```

The suite generates a `RESULTS.md` linking pipeline comparisons, cache evidence,
and codec checks. Local original evidence remains under
`outputs/comparisons/cache-07-20260928/`, including all runs, logs, initial plans,
event logs, and exact comparison results. Full data and generated tables are
ignored by Git; this directory retains compact reports and executed plans.

## Storage and retained history

Before benchmarking, the authorized cleanup removed only the generated
Bronze/Silver/Gold tables from `compression-01-20260927`: **18 directories,
4,669,734,037 bytes**. Reports, logs, plans, source files, the original baseline
run, and every project version were preserved. The local cleanup manifest is
`outputs/comparisons/compression-01-20260927/table-cleanup.json`.
There was **7.858 GiB free** immediately before the full benchmark, above the
7 GiB target. About **4.1 GiB remained after verification completed**. No
additional datasets were deleted and no resources were raised.
The separate exact-comparison process logged GC/allocation retries while checking
`02-left: silver/impressions`; that table comparison subsequently passed. Those
warnings belong to correctness verification, outside the timed rebuild samples.
All six full-data builds succeeded; intentional failure tests verified cleanup
and original-error preservation. There was no failed full-data attempt to omit.

## Recommendation

Keep iteration 2 as the reference for future independent experiments. Do not
carry this full enrichment cache forward by default. Caching becomes a different
hypothesis when there is substantially more reuse, a smaller reusable dataset,
or another resource budget; none of those was added to this iteration.
The lesson is to measure **cache creation + all uses + release**, verify actual
reuse, and distinguish cache storage from shuffle spill and table storage.
