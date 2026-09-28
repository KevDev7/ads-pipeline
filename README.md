# ads-pipeline

A learning project for Apache Spark using Python (PySpark), SQL, and the Taobao advertising dataset.

The project includes a preserved **untuned baseline**, **iteration 2: ZSTD
compression**, **iteration 3: date partitioning**, and **iteration 4: ads joins**. Each runs full batch
rebuilds with correctness checks and runtime/storage measurements, locally
without cloud services. Iteration 1 provides a separate CSV-versus-Parquet read-workload experiment.

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

## Comparing iterations

The [central results page](results/README.md) tracks experiments and findings.
The [shared comparison runner](docs/comparisons.md) runs correctness tests first,
repeats both versions in fresh processes, checks actual table equality, and saves
individual measurements plus medians and ranges. The baseline implementation and
its original tag remain unchanged. [Iteration 2](experiments/02_compression/README.md)
tests Snappy versus ZSTD compression. Its [completed results](results/02_compression/README.md)
show **31.85% smaller tables**, with no clear processing-speed improvement across
three runs per version. All 40 exact table comparisons passed. The snapshot and
results are tagged `iteration-02-compression`. [Iteration 1: CSV versus Parquet](experiments/01_csv_vs_parquet/README.md)
compares the same typed impressions and read queries in uncompressed formats.
The numbering follows the learning sequence; compression was completed first.

[Iteration 3](experiments/03_date_partitioning/README.md) partitions Silver
impressions by date while keeping ZSTD. Its [results](results/03_date_partitioning/README.md)
show faster measured date-filtered reads, **7.99% slower rebuilds**, and **14.86%
smaller output tables** relative to iteration 2. All 40 exact table comparisons
and 18 query executions matched. It is preserved as `iteration-03-date-partitioning`.

## Repeat an experiment with one command

```bash
bash scripts/test_iteration.sh 1  # CSV versus Parquet
bash scripts/test_iteration.sh 2  # Compression
bash scripts/test_iteration.sh 3  # Date partitioning
bash scripts/test_iteration.sh 4  # Ads broadcast versus sort-merge join, including executed-plan checks
```

These run the test suite, the iteration’s repeated workloads, exact equality
checks, and actual compression checks. Iteration 1 measures format conversion
and reads; iterations 2–4 also compare full rebuilds. Open the generated `RESULTS.md`
under `outputs/comparisons/<id>/` for all evidence. Each run gets a fresh ID;
large outputs remain local. See [options and limitations](docs/comparisons.md#one-command-per-implemented-iteration).

[Iteration 1's completed format results](results/01_csv_vs_parquet/README.md)
compare uncompressed CSV and Parquet for the same 26.6 million impressions.
Parquet used **58.18% fewer data bytes**, with **3.89–8.44× faster median reads**
across three measured workloads; conversion cost was measured separately. This
is a read-workload comparison, not a full-pipeline speedup claim. All records
and query results matched. Tag: `iteration-01-csv-vs-parquet`.

[Iteration 4](experiments/04_ads_shuffle_join/README.md) changes only the ads join
to sort-merge from the unpartitioned ZSTD reference. Its automated checks verify
the actual ads and profile join strategies from runtime event logs. Its
[completed results](results/04_ads_shuffle_join/README.md) show Gold processing
medians of **7.947 → 14.871 seconds** and about **five times the shuffle writes**
when forcing sort-merge. All 24 full-data table comparisons passed. Two repeats
per side were used due to local disk capacity; overall pipeline timing was more
variable. Automatic broadcasting remains the better fit for this workload.
Tag: `iteration-04-ads-shuffle-join`.
