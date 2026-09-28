# Iteration 7: reuse cached enrichment

Based on iteration 2. The only deliberate optimization is persisting the full
existing enrichment with `MEMORY_AND_DISK_DESER` for both Gold reports.
The campaign write materializes it; the audience write reuses it. There is no
warm-up action, projection, repartition, join hint, or SQL-setting change.
Caching the full result retains columns that individual reports could otherwise
prune. Spark may automatically choose different plans for this wider input.

Run `bash scripts/run_cached_enrichment.sh` for one build, or
`bash scripts/test_iteration.sh 7` for regression tests, three fresh processes
per side in alternating order, 40 exact table comparisons, cache execution
evidence, and actual ZSTD footer checks. Optional arguments follow the existing
comparison runner. Results are linked in the generated `RESULTS.md`.

Registration and materialization are inside `gold.campaign_daily` timing.
Blocking, scoped release is timed as `gold.cache_release` and included in
processing totals. Compare **both Gold writes plus release**, not only the
second report. Failure cleanup preserves the original exception.

Storage snapshots after campaign, before/after audience, and after release are
metadata-only. They describe this RDD's memory/disk blocks, not peak process
memory. Cache disk storage is distinct from shuffle spill and table storage.
Execution checks match SQL IDs to successful stages and task accumulator updates:
first-report source/join work, cache scans in both reports, no repeated source/join
work in the second. Merely seeing a cached plan or its old join lineage is not
sufficient. Missing or contradictory evidence fails verification.

Fixed resources: local[4], 2 GiB heap, 200 initial shuffle partitions, AQE enabled,
ZSTD, unpartitioned tables. Runs that fail remain recorded; resources are not
silently increased. All six full-data runs passed, but caching increased Gold time by 132.45% and
processing time by 22.32% (medians). Keep iteration 2 as the working reference.
See [the documented findings](../../results/07_cached_enrichment/README.md).
