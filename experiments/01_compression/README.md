# Iteration 1: Snappy versus ZSTD

**One deliberate change:** set `spark.sql.parquet.compression.codec` to `zstd`.
The baseline uses the default `snappy`. Both versions remain Delta Lake pipelines
backed by Parquet files. This is a compression experiment, not CSV-versus-Parquet.

The five Python files are a runnable snapshot of the baseline. The only functional
change is the codec setting in `session.py`; `run.py` also has an iteration-specific
description, success message, and default output directory. Transforms, schemas,
validation, measurements, resource budget, and SQL defaults are otherwise identical.
No compression level, join hints, partitioning, sorting, or caching is tuned.

**Completed:** [results and evidence](../../results/01_compression/README.md) show
31.85% smaller tables, no clear processing-speed improvement, and identical
reporting results. Preserved as `iteration-01-compression`.

## Hypothesis and measurements

ZSTD may reduce table bytes and downstream bytes read, but compression/decompression
uses CPU. Measure storage, per-stage and overall time, task CPU time, input bytes,
shuffle, file counts, and spills. Do not promise a speedup. Automatic changes Spark
makes in response to different file sizes are consequences to inspect and explain.

Compare three fresh runs per side in alternating order, with the same three full
source files. Preserve every observation and use medians/ranges. Run existing tests
plus the iteration integration test, then compare every table to the first baseline
run with exact equality. Finally read all Parquet file footers to confirm the
requested codec was actually used. File-footer checks happen outside timed runs.

## Run

```bash
bash scripts/run_compression.sh
bash scripts/compare.sh --right experiments/01_compression/run.py \
  --comparison-id compression-01 --repeats 3 \
  --change 'Parquet compression: Snappy to ZSTD across Bronze, Silver, and Gold' \
  --changed-setting spark.sql.parquet.compression.codec
```

Use a fresh comparison ID each time. The baseline remains in `00_baseline` and
its tag is unchanged. Small summaries are published under `results/01_compression`;
all large output tables and logs stay local. See the [central index](../../results/README.md).

## Learning note

CSV-versus-Parquet is a separate, optional learning lab explaining a format choice
already present in iteration 0. It is not iteration 1 and is not implemented here.

[Spark's codec configuration](https://spark.apache.org/docs/4.0.1/sql-data-sources-parquet.html#configuration)
lists the supported codecs and the default. We verify the actual files rather
than treating the configured setting as sufficient evidence.
