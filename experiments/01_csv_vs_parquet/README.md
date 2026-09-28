# Iteration 1: CSV versus Parquet

A runnable format experiment on the full `raw_sample.csv` impressions source.
The original lakehouse baseline already uses Parquet; this experiment supplies
the missing CSV comparison without rewriting that preserved baseline.

## One deliberate format change

Read the same six typed fields from the original **uncompressed CSV** and an
**uncompressed Parquet** representation. The explicit schema is `user: long`,
`time_stamp: long`, `adgroup_id: long`, `pid: string`, `nonclk: int`, `clk: int`.
CSV header/numeric parsing is strict; no schema inference is timed. Nulls and
quoted CSV strings are interpreted before the Parquet conversion. We compare
logical records, not CSV text formatting.

Parquet's physical encodings, column-oriented reads, and vectorized decoding are
part of this format comparison. External compression is disabled so the codec
lesson stays in iteration 2. No partitioning, sorting, caching, join hints, or
manual repartitioning is introduced. Both sides keep Spark's defaults.

The CSV is the existing source file. Spark writes Parquet with the natural
partitions of that read. File counts/sizes and subsequent task partitioning may
therefore differ. This measures a practical format conversion, not a laboratory
comparison with artificially identical physical layouts.

## Workloads and measurements

- **Narrow:** group by placement (`pid`), count impressions, sum clicks (`clk`).
- **Wide:** group by placement and sum every other field, forcing all six columns
  to participate. Some sums are deliberately diagnostic, not business KPIs.
- **One day:** the narrow query filtered to May 8, 2017 in Asia/Shanghai using an
  explicit half-open epoch-second range.

All queries force data reads. Three fresh query processes per format are the
default. CSV/Parquet order alternates; query order rotates so each workload is
first once per format. Relation/schema preparation and Spark startup are outside
query timing; planning and collecting actual results are inside it. There is no
explicit data cache or data-query warm-up. OS caches and JIT effects remain.

Prepare Parquet **once** and report conversion time separately as one observation.
Reuse those exact files for all reads. After timed reads, compare all records with
bidirectional `EXCEPT ALL`, preserving types, duplicates, and nulls; verify the
source fingerprint has not changed and every Parquet column chunk is UNCOMPRESSED.
Also compare the small results of every measured query. Save plans, actual file
scan metrics, and task I/O/CPU/shuffle/spill counters.

This is not a full Bronze/Silver/Gold rebuild comparison. Lookup tables and joins
are not involved. Query seconds here must not be compared directly to pipeline
seconds from iterations 0, 2, or 3. No cloud cost is measured.

## Run everything

```bash
bash scripts/test_iteration.sh 1
```

For an explicit ID:

```bash
bash scripts/test_iteration.sh 1 --comparison-id formats-01 --repeats 3
```

Read `outputs/comparisons/<id>/RESULTS.md` and its linked format report. The command
runs the test suite first. Outputs retain one Parquet representation, query logs,
plans, verification, and raw reports; they never overwrite earlier IDs or duplicate
the original CSV. A tiny fixture and `--repeats 1` are suitable for smoke tests.

The other two source files remain part of the lakehouse project; this format
experiment intentionally isolates the large impressions source. Iterations 2
and 3 are preserved. See the [central results index](../../results/README.md).
