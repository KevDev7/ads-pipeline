"""Run an experiment's existing checks and collect one report entry point."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parents[1]
EXPERIMENTS = {
    '8': {
        'name': 'Automatic shuffle-partition coalescing',
        'left': 'experiments/02_compression/run.py',
        'right': 'experiments/08_no_shuffle_coalescing/run.py',
        'change': 'Disable only spark.sql.adaptive.coalescePartitions.enabled; keep AQE and 200 initial partitions',
        'changed_settings': ['spark.sql.adaptive.coalescePartitions.enabled'],
        'queries': False, 'coalescing_evidence': True,
    },
    '7': {
        'name': 'Reuse cached enrichment',
        'left': 'experiments/02_compression/run.py',
        'right': 'experiments/07_cached_enrichment/run.py',
        'change': 'Persist full enrichment for both Gold reports; blocking scoped release',
        'changed_settings': [], 'queries': False, 'cache_evidence': True,
    },
    '1': {'name': 'CSV versus uncompressed Parquet'},
    '6': {
        'name': 'Initial shuffle partitions: 200 versus 32',
        'left': 'experiments/02_compression/run.py',
        'right': 'experiments/06_shuffle_partitions/run.py',
        'change': 'Set spark.sql.shuffle.partitions from 200 to 32; keep AQE, automatic joins and ZSTD unchanged',
        'changed_settings': ['spark.sql.shuffle.partitions'],
        'queries': False,
        'shuffle_evidence': True,
    },
    '5': {
        'name': 'Profile broadcast versus sort-merge join',
        'left': 'experiments/02_compression/run.py',
        'right': 'experiments/05_profile_shuffle_join/run.py',
        'change': 'Request MERGE only for the user-profile join; keep ads join automatic and ZSTD',
        'changed_settings': [],
        'queries': False,
        'join_target': 'profiles',
    },
    '4': {
        'name': 'Ads broadcast versus sort-merge join',
        'join_target': 'ads',
        'left': 'experiments/02_compression/run.py',
        'right': 'experiments/04_ads_shuffle_join/run.py',
        'change': 'Request MERGE only for the ads join; keep user-profile join automatic and ZSTD',
        'changed_settings': [],
        'queries': False,
    },
    '2': {
        'name': 'Snappy versus ZSTD compression',
        'left': 'experiments/00_baseline/run.py',
        'right': 'experiments/02_compression/run.py',
        'change': 'Parquet compression: Snappy to ZSTD across Bronze, Silver, and Gold',
        'changed_settings': ['spark.sql.parquet.compression.codec'],
        'queries': False,
    },
    '3': {
        'name': 'Date partitioning',
        'left': 'experiments/02_compression/run.py',
        'right': 'experiments/03_date_partitioning/run.py',
        'change': 'Partition Silver impressions by reporting_date; keep ZSTD',
        'changed_settings': [],
        'queries': True,
    },
}


def commands(iteration, source, output, comparison_id, repeats):
    spec = EXPERIMENTS[iteration]
    root = output / comparison_id
    if iteration == '1':
        return [('formats', [sys.executable, str(REPO / 'experiments/01_csv_vs_parquet/run.py'),
                             '--source-dir', str(source), '--root', str(root), '--repeats', str(repeats)])]
    pipeline = [sys.executable, '-m', 'benchmarks.compare',
                '--left', spec['left'], '--right', spec['right'],
                '--source-dir', str(source), '--output-dir', str(output),
                '--comparison-id', comparison_id, '--repeats', str(repeats),
                '--change', spec['change']]
    for key in spec['changed_settings']:
        pipeline += ['--changed-setting', key]
    if spec.get('coalescing_evidence'):
        pipeline += ['--observe-coalescing']
    steps = [('pipeline', pipeline)]
    if spec.get('coalescing_evidence'):
        steps.append(('coalescing', [sys.executable, '-m', 'benchmarks.coalescing', str(root)]))
    if spec['queries']:
        steps.append(('queries', [sys.executable, '-m', 'benchmarks.partition_queries',
                                  '--comparison-dir', str(root)]))
    if 'join_target' in spec:
        steps.append(('joins', [sys.executable, '-m', 'benchmarks.join_strategies', str(root),
                                '--target', spec['join_target']]))
    if spec.get('shuffle_evidence'):
        steps.append(('shuffle', [sys.executable, '-m', 'benchmarks.shuffle_partitions', str(root)]))
    if spec.get('cache_evidence'):
        steps.append(('cache', [sys.executable, '-m', 'benchmarks.cache_evidence', str(root)]))
    # Codec inspection is deliberately after the timed query workloads.
    steps.append(('codecs', [sys.executable, '-m', 'benchmarks.storage_codecs', str(root)]))
    return steps


def report(root, state):
    """Keep partial/failed runs visibly distinct from successful evidence."""
    root.mkdir(parents=True, exist_ok=True)
    temporary = root / 'suite.tmp'
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + '\n')
    temporary.replace(root / 'suite.json')
    lines = [f"# Iteration {state['iteration']}: {EXPERIMENTS[state['iteration']]['name']}", '',
             f"Overall status: **{state['status']}**.", '',
             f"Comparison ID: `{state['comparison_id']}`. Repetitions per side: {state['repeats']}.", '',
             '| Check | Status | Log |', '| --- | --- | --- |']
    for name, step in state['steps'].items():
        log_link = f"[Log](../{step['log']})" if (root.parent / step['log']).is_file() else "Not started"
        lines.append(f"| {name} | {step['status']} | {log_link} |")
    if state.get('error'):
        lines += ['', f"Failure: {state['error']}",
                  'This suite is incomplete; do not treat it as a successful experiment.']
    lines += ['', '## Evidence', '']
    for filename, label in [
        ('README.md', 'Format conversion, read timings, and correctness summary' if state['iteration'] == '1'
         else 'Pipeline times, storage, shuffle/spill, and equality summary'),
        ('format-comparison.json', 'CSV/Parquet preparation, every query sample, and exact record verification'),
        ('comparison.json', 'Every pipeline sample and exact table comparison'),
        ('query-comparison.json', 'Every query result, timing, executed scan metric, and task counter'),
        ('join-strategies.json', 'Executed ads and user-profile join strategies'),
        ('coalescing-evidence.json', 'Effective AQE settings, executed reader stages, task durations and file sizes'),
        ('cache-evidence.json', 'Cache materialization, reuse, release, and executed joins'),
        ('codecs.json', 'Actual Parquet file compression checks'),
        ('shuffle-partitions.json', 'Initial shuffle partitions, AQE plans, actual stages/tasks and unchanged joins'),
    ]:
        if (root / filename).exists():
            lines.append(f'- [{label}]({filename})')
    query_path = root / 'query-comparison.json'
    if query_path.exists():
        query_report = json.loads(query_path.read_text())
        if query_report.get('status') == 'succeeded':
            lines += ['', '## Read workloads', '',
                      'Median [minimum–maximum] seconds. Query action only; startup excluded.', '',
                      '| Query | Left | Right | Task input MB, left → right | Files, left → right |',
                      '| --- | ---: | ---: | ---: | ---: |']
            for name, stats in query_report['summary'].items():
                a, b = stats['left'], stats['right']
                def timing(side):
                    t = side['seconds']
                    return f"{t['median']:.3f} [{t['min']:.3f}–{t['max']:.3f}]"
                lines.append(f"| {name} | {timing(a)} | {timing(b)} | "
                             f"{a['task_input_bytes']['median']/1e6:.3f} → {b['task_input_bytes']['median']/1e6:.3f} | "
                             f"{a['files_scanned']['median']} → {b['files_scanned']['median']} |")
            lines += ['', 'Executed query plans and individual reports are under `queries/<run-id>/`.']
    lines += ['', '## Interpretation', '',
              'Measurements describe this workload; they do not automatically establish a winner.',
              'Check correctness first, then compare runtime variation, storage, I/O, and write costs.',
              'OS caches are not flushed. A single repetition is a smoke check, not a speedup claim.',
              'Plans and event logs remain available for investigating unexpected behavior.',
              'This command does not generate the human explanation or replace deeper inspection.']
    (root / 'RESULTS.md').write_text('\n'.join(lines) + '\n')


def run(iteration, source, output, comparison_id, repeats):
    if repeats < 1:
        raise ValueError('repeats must be positive')
    if Path(comparison_id).name != comparison_id or comparison_id in ('', '.', '..'):
        raise ValueError('comparison-id must be a single directory name')
    source, output = source.resolve(), output.resolve()
    if not source.is_dir():
        raise FileNotFoundError(source)
    output.mkdir(parents=True, exist_ok=True)
    root = output / comparison_id
    # The underlying comparison runner owns creation of root. A sibling log
    # directory reserves this ID before launching it and preserves early errors.
    logs = output / f'{comparison_id}-suite-logs'
    if root.exists():
        raise FileExistsError(root)
    logs.mkdir(exist_ok=False)
    steps = commands(iteration, source, output, comparison_id, repeats)
    state = {'iteration': iteration, 'comparison_id': comparison_id, 'repeats': repeats,
             'status': 'running', 'started_at': datetime.now(timezone.utc).isoformat(),
             'steps': {name: {'status': 'pending', 'log': f'{logs.name}/{name}.log'} for name, _ in steps}}
    try:
        for name, command in steps:
            state['steps'][name]['status'] = 'running'
            # Do not precreate root before benchmarks.compare starts.
            if root.exists():
                report(root, state)
            print(f'Running {name}; log: {logs / (name + ".log")}', flush=True)
            with (logs / f'{name}.log').open('w') as log:
                subprocess.run(command, cwd=REPO, stdout=log, stderr=subprocess.STDOUT, check=True)
            artifact = {'coalescing': 'coalescing-evidence.json', 'cache': 'cache-evidence.json', 'formats': 'format-comparison.json', 'pipeline': 'comparison.json', 'joins': 'join-strategies.json', 'shuffle': 'shuffle-partitions.json', 'queries': 'query-comparison.json', 'codecs': 'codecs.json'}[name]
            evidence = json.loads((root / artifact).read_text())
            if name != 'codecs' and evidence.get('status') != 'succeeded':
                raise ValueError(f'{name} did not report success')
            state['steps'][name]['status'] = 'succeeded'
            report(root, state)
        state['status'] = 'succeeded'
    except BaseException as error:
        for step in state['steps'].values():
            if step['status'] == 'running':
                step['status'] = 'failed'
        state['status'] = 'failed'
        state['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        state['finished_at'] = datetime.now(timezone.utc).isoformat()
        report(root, state)
        print(f"Suite {state['status']}: {root / 'RESULTS.md'}", flush=True)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('iteration', choices=EXPERIMENTS, help='Implemented experiment: 1, 2, 3, 4, 5, 6, 7, or 8')
    parser.add_argument('--source-dir', type=Path, default=REPO / 'data/raw/taobao')
    parser.add_argument('--output-dir', type=Path, default=REPO / 'outputs/comparisons')
    parser.add_argument('--comparison-id', default=datetime.now(timezone.utc).strftime('suite-%Y%m%dT%H%M%S%fZ'))
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    run(args.iteration, args.source_dir, args.output_dir, args.comparison_id, args.repeats)


if __name__ == '__main__':
    main()
