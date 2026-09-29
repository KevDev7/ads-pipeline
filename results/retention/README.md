# Generated-table retention

Code and recorded results remain available after regenerable tables are removed.
The [standing retention policy](../../docs/comparisons.md#standing-permission-for-generated-table-cleanup)
authorizes cleanup when space is needed; it preserves source data, the original
baseline, failed runs, current work, and all implementations/reports/logs/plans.

## 2026-09-29: old iterations 6–8

Removed only generated Bronze/Silver/Gold tables from these successful comparisons:

- `shuffle-06-20260927`
- `cache-07-20260928`
- `coalescing-08-20260928`

The cleanup removed 54 directories totaling **11,341,077,825 bytes (10.56 GiB)**.
No Spark or benchmark process was active. The 153 retained evidence files in
those comparison directories have matching paths, sizes and modification times
before and after cleanup. Their generated tables can be recreated with the
existing iteration commands; reruns will have new timing samples.

Iteration 9 outputs, the original baseline, source data, failed runs and all code
were preserved. After cleanup the project occupied approximately 10 GiB and the
filesystem reported approximately 16 GiB available. Free space may change as other
applications use the disk.

[Exact cleanup manifest](cleanup-20260929-iterations06-08.json)
