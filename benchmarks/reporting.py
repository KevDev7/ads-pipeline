"""Summaries describe observations, never automatically declare a winner."""
import statistics


FIXED_ENVIRONMENT = ('python', 'spark', 'delta', 'java', 'platform', 'machine',
                     'master', 'driver_memory', 'git_commit', 'git_dirty')


def validate_reports(reference: dict, current: dict, changed_settings=()) -> dict:
    for report in (reference, current):
        if report.get('status') != 'succeeded':
            raise ValueError('Cannot compare an incomplete or failed run')
    if reference['sources'] != current['sources']:
        raise ValueError('Source fingerprints differ')
    if set(reference['tables']) != set(current['tables']):
        raise ValueError('Output table sets differ')
    for key in FIXED_ENVIRONMENT:
        if reference['environment'][key] != current['environment'][key]:
            raise ValueError(f'Runtime environment differs: {key}')
    before = reference['environment']['spark_sql_settings']
    after = current['environment']['spark_sql_settings']
    differences = {key: {'reference': before.get(key), 'current': after.get(key)}
                   for key in before.keys() | after.keys() if before.get(key) != after.get(key)}
    unexpected = set(differences) - set(changed_settings)
    if unexpected:
        raise ValueError(f'Undeclared Spark setting differences: {sorted(unexpected)}')
    return differences


def measurements(report: dict) -> dict:
    operations = [v for k, v in report['spark_tasks']['by_operation'].items()
                  if k.startswith(('bronze.', 'silver.', 'gold.', 'workload.'))]
    result = {f'seconds.{key}': value for key, value in report['seconds'].items()}
    result['storage.table_bytes'] = report['storage']['table_bytes']
    result['storage.parquet_files'] = sum(t['parquet_files'] for t in report['tables'].values())
    for metric in ('input_bytes', 'shuffle_read_bytes', 'shuffle_write_bytes',
                   'disk_spill_bytes', 'memory_spill_bytes', 'executor_run_ms',
                   'executor_cpu_ns', 'task_attempts', 'failed_task_attempts'):
        result[f'processing.{metric}'] = sum(op[metric] for op in operations)
    result['processing.peak_task_execution_memory_bytes'] = max(
        (op['peak_task_execution_memory_bytes'] for op in operations), default=0)
    return result


def distribution(values: list) -> dict:
    return {'samples': values, 'median': statistics.median(values),
            'min': min(values), 'max': max(values),
            'stdev': statistics.stdev(values) if len(values) > 1 else None}


def summarize(runs: list) -> dict:
    sides = {side: [measurements(run['report']) for run in runs if run['side'] == side]
             for side in ('left', 'right')}
    metric_sets = [set(row) for rows in sides.values() for row in rows]
    shared = set.intersection(*metric_sets)
    summary = {}
    for metric in sorted(shared):
        stats = {side: distribution([row[metric] for row in rows]) for side, rows in sides.items()}
        baseline = stats['left']['median']
        stats['right_vs_left_percent'] = (
            (stats['right']['median'] - baseline) / baseline * 100 if baseline else None)
        summary[metric] = stats
    return summary


def markdown(result: dict) -> str:
    lines = [f"# {result['comparison_id']}", '', f"Status: **{result['status']}**.", '',
             f"Declared change: {result['change']}", '',
             f"Workload: {result['workload']}", '',
             f"Left: `{result['left']}`. Right: `{result['right']}`.", '',
             '## Measurements', '',
             'Median [minimum–maximum]. Positive change means the right-hand value is larger.', '',
             '| Measurement | Left | Right | Change |', '| --- | ---: | ---: | ---: |']
    visible = ('seconds.pipeline_writes', 'seconds.workload', 'seconds.total', 'seconds.validation',
               'storage.table_bytes', 'storage.parquet_files', 'processing.input_bytes',
               'processing.shuffle_write_bytes', 'processing.disk_spill_bytes')
    for metric in visible:
        if metric not in result.get('summary', {}):
            continue
        data = result['summary'][metric]
        cells = [f"{data[side]['median']:,.3f} [{data[side]['min']:,.3f}–{data[side]['max']:,.3f}]"
                 for side in ('left', 'right')]
        delta = data['right_vs_left_percent']
        change = f'{delta:+.2f}%' if delta is not None else 'n/a (left is zero)'
        lines.append(f'| {metric} | {cells[0]} | {cells[1]} | {change} |')
    lines += ['', '## Individual runs', '',
              '| Run | Side | Measured work (s) | Pipeline total (s) |',
              '| --- | --- | ---: | ---: |']
    for item in result['runs']:
        seconds = item['report']['seconds']
        work = seconds.get('workload', seconds.get('pipeline_writes'))
        lines.append(f"| {item['run_id']} | {item['side']} | {work} | {seconds['total']} |")
    checks = result.get('comparisons', [])
    lines += ['', '## Correctness and interpretation', '',
              f"- {sum(len(check['tables']) for check in checks)} exact table comparisons recorded.",
              '- Each measured run is checked against the first left-hand run, not only against totals.',
              '- Exact schema types and row values are compared with duplicates and nulls preserved.',
              '- Tests, full-table equality checks, and their Spark jobs are outside measured pipeline time.',
              '- Fresh sequential processes; order alternates left/right then right/left.',
              '- All measured runs are retained, including the first. No warm-up runs are silently discarded.',
              '- OS caches are not flushed. Source hashing and validations may warm caches.',
              '- A few repetitions describe variability; percentage differences are not proof of a speedup.',
              '- Spark task bytes are not unique physical disk reads. Peak task memory is not process RAM.',
              '- Per-run Spark settings, timing phases, counters, and source hashes are in comparison.json.',
              '- Plans and event logs remain under the ignored local comparison directory. Inspect them to explain a mechanism.',
              '- Human interpretation belongs in the central results page after reviewing this evidence.']
    if result.get('control'):
        lines += ['- **A/A control:** both sides execute the same code. Timing differences are variation, not an optimization.']
    if result.get('error'):
        lines += [f"- Failure: `{result['error']}`. This comparison is not a performance result."]
    return '\n'.join(lines) + '\n'
