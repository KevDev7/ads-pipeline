# Iteration 1: CSV versus uncompressed Parquet

Status: **succeeded**.

Same typed impressions and queries. Preparation, query actions, and verification are measured separately.

CSV data bytes: 1,088,060,964.
Parquet data bytes: 455,068,985.
Conversion action: 7.335 seconds (one observation).

| Query | CSV median [min–max] seconds | Parquet median [min–max] seconds |
| --- | ---: | ---: |
| narrow | 3.173 [2.951–4.341] | 0.816 [0.392–1.074] |
| wide | 4.100 [4.050–4.747] | 0.859 [0.755–1.342] |
| one_day | 2.850 [2.769–3.585] | 0.338 [0.301–1.324] |

All samples, results, scan metrics, CPU/I/O/shuffle/spill counters, and provenance are in
[format-comparison.json](format-comparison.json). Executed plans are in `queries/<run-id>/`.

Three repeats are the default; one repeat is only a smoke check. OS caches are not flushed.
Timing excludes Spark startup and relation preparation. No data-query warm-up or explicit caching.
Query order rotates; format order alternates. This is a read-workload experiment, not a full lakehouse rebuild.
Original CSV and naturally written Parquet have different file layouts; no repartitioning is added.
Compression is disabled for Parquet so this does not repeat the codec experiment.
