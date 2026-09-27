# ads-pipeline

A learning project for Apache Spark using Python (PySpark), SQL, and the Taobao advertising dataset.

The current version is **baseline only**: full batch rebuilds, correctness checks,
and runtime/storage measurements. No manual performance tuning or cloud services.

```text
Three CSV files → Bronze Delta → Silver Delta → Gold Delta reports
```

Bronze preserves source fields as strings. Silver assigns types and establishes
the impression/ad/user relationships. Gold reports daily impressions, clicks, and
click-through rate by campaign and advertiser/age group. Missing user profiles
remain in the counts. Product price is not advertising spend.

## Setup

Requires Python 3.11, Java 17 or 21, and [uv](https://docs.astral.sh/uv/).
You write Python and SQL; Java runs Spark underneath.

```bash
uv sync --frozen
export JAVA_HOME=/absolute/path/to/your/jdk
```

Alternatively, put `JAVA_HOME=/absolute/path/to/your/jdk` in the ignored
`.env.local` file. This machine already has that file configured. It references
the Java installation retained from the earlier dataset inspection.

Place the complete `raw_sample.csv`, `ad_feature.csv`, and `user_profile.csv`
in `data/raw/taobao/`. They total about 1.14 GB uncompressed and are already
present on this machine. See the [dataset and modeling notes](docs/baseline.md)
for source links. Do not download the much larger behavior log for this version.

## Run

```bash
bash scripts/test_baseline.sh
bash scripts/run_baseline.sh
```

The first Spark startup downloads the Delta JVM dependencies. Every run gets a
new directory under `outputs/00_baseline/`; it contains Bronze/Silver/Gold Delta
tables, `report.json`, Spark event logs, and initial query plans. A run succeeds
only after checks pass. Tables in a failed run are incomplete results.

Optional explicit paths and a descriptive run ID:

```bash
bash scripts/run_baseline.sh --source-dir data/raw/taobao --run-id my-baseline
```

An existing run ID is rejected, protecting earlier results. Rerunning with a new
ID recomputes all layers from the source CSV files and uses additional disk space.
`--output-dir` can place outputs on another local drive. The three raw source
files are shared by all runs.

## Read the measurements

`report.json` records source hashes, runtime versions, effective Spark settings,
row counts, file sizes/counts, per-stage elapsed time, and task input/shuffle/spill
counters. `pipeline_writes` sums Bronze, Silver, and Gold processing; `validation`
is listed separately; `total` also includes fingerprints, startup, and shutdown.
The Spark UI is available at `http://localhost:4040` while the job is running
(Spark chooses another port if it is occupied). Event logs remain after exit.

We use four local worker threads and a 2 GiB driver heap as a fixed resource
budget. Spark's automatic optimizer, adaptive execution, and default compression
remain enabled. This is an untuned baseline, not literally zero optimization.
Local seconds and bytes are measured; no cloud dollar cost is claimed.

The runnable implementation stays in `experiments/00_baseline/`. Raw data,
generated tables, event logs, runtime files, and local configuration are ignored
by Git. Only code, documentation, and selected small result summaries are pushed.

## First complete baseline

The [recorded full-data run](results/00_baseline/README.md) processed 26,557,961
impressions in **33.769 seconds of pipeline work**, or **42.910 seconds overall**.
All correctness checks passed. The eight Delta tables occupy **925.72 MB** in
addition to the source CSV files. This is one local reference measurement; read
the result notes before comparing timings. The preserved version is tagged
`baseline-v0.1`.

## Comparing future iterations

The [central results page](results/README.md) tracks experiments and findings.
The [shared comparison runner](docs/comparisons.md) runs correctness tests first,
repeats both versions in fresh processes, checks actual table equality, and saves
individual measurements plus medians and ranges. The baseline implementation and
its original tag remain unchanged. [Iteration 1](experiments/01_compression/README.md)
tests Snappy versus ZSTD compression. CSV-versus-Parquet is a separate optional
learning lab, not a numbered pipeline iteration.
