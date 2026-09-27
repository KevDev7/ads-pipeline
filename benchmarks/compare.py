"""Run matching experiment entrypoints and verify every result before summarizing."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

from benchmarks.equality import compare_frames
from benchmarks.reporting import markdown, summarize, validate_reports

REPO = Path(__file__).resolve().parents[1]
BASELINE = REPO / 'experiments/00_baseline/run.py'


def save(root: Path, result: dict):
    temporary = root / 'comparison.tmp'
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    temporary.replace(root / 'comparison.json')
    (root / 'README.md').write_text(markdown(result))


def command(argv: list, log: Path):
    with log.open('w') as handle:
        subprocess.run(argv, cwd=REPO, stdout=handle, stderr=subprocess.STDOUT, check=True)


def verify_outputs(root: Path, runs: list, result: dict):
    # Reuse only the baseline session setup. Do not alter the frozen pipeline.
    sys.path.insert(0, str(BASELINE.parent))
    from session import create_spark
    spark = create_spark(root / 'verification-events')
    reference = runs[0]
    try:
        for item in runs[1:]:
            checked = {'reference': reference['run_id'], 'candidate': item['run_id'], 'tables': {}}
            result['comparisons'].append(checked)
            for name in sorted(reference['report']['tables']):
                print(f"Comparing {item['run_id']}: {name}", flush=True)
                spark.sparkContext.setJobGroup(f"compare.{item['run_id']}.{name}", name)
                left = spark.read.format('delta').load(str(root / 'runs' / reference['run_id'] / name))
                right = spark.read.format('delta').load(str(root / 'runs' / item['run_id'] / name))
                check = compare_frames(left, right)
                checked['tables'][name] = check
                save(root, result)
                if not check['equal']:
                    raise ValueError(f"Result mismatch: {item['run_id']} {name}")
    finally:
        spark.stop()


def run_comparison(left: Path, right: Path, source: Path, output: Path,
                   comparison_id: str, repeats: int, change: str, workload: str,
                   changed_settings=()) -> dict:
    if repeats < 1:
        raise ValueError('repeats must be positive')
    if Path(comparison_id).name != comparison_id or comparison_id in ('', '.', '..'):
        raise ValueError('comparison-id must be a single directory name')
    left, right, source = left.resolve(), right.resolve(), source.resolve()
    for entry in (left, right):
        if not entry.is_file():
            raise FileNotFoundError(entry)
    if not source.is_dir():
        raise FileNotFoundError(source)
    root = output.resolve() / comparison_id
    root.mkdir(parents=True, exist_ok=False)
    (root / 'logs').mkdir()
    result = {'comparison_id': comparison_id, 'status': 'running', 'control': left == right,
              'change': change, 'workload': workload, 'repeats_per_side': repeats,
              'left': str(left.relative_to(REPO)) if left.is_relative_to(REPO) else str(left),
              'right': str(right.relative_to(REPO)) if right.is_relative_to(REPO) else str(right),
              'declared_setting_changes': list(changed_settings), 'runs': [], 'comparisons': [],
              'started_at': datetime.now(timezone.utc).isoformat()}
    started = time.perf_counter()
    save(root, result)
    try:
        print('Running the correctness test suite first', flush=True)
        command([sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-v'], root / 'logs/tests.log')
        result['tests_passed'] = True
        save(root, result)
        for repetition in range(1, repeats + 1):
            order = ('left', 'right') if repetition % 2 else ('right', 'left')
            for side in order:
                entry = left if side == 'left' else right
                run_id = f'{repetition:02d}-{side}'
                print(f'Running {run_id}: {entry.name}', flush=True)
                command([sys.executable, str(entry), '--source-dir', str(source),
                         '--output-dir', str(root / 'runs'), '--run-id', run_id],
                        root / 'logs' / f'{run_id}.log')
                report = json.loads((root / 'runs' / run_id / 'report.json').read_text())
                item = {'run_id': run_id, 'side': side, 'report': report}
                result['runs'].append(item)
                reference = result['runs'][0]['report']
                # Repeated controls must have identical settings. Declared changes
                # are permitted only between the left and right implementations.
                allowed = changed_settings if side == 'right' else ()
                item['setting_differences'] = validate_reports(reference, report, allowed)
                if side == 'right':
                    prior = next(r['report'] for r in result['runs'] if r['side'] == 'right')
                    validate_reports(prior, report)
                save(root, result)
        begin_checks = time.perf_counter()
        verify_outputs(root, result['runs'], result)
        result['verification_seconds'] = round(time.perf_counter() - begin_checks, 3)
        result['summary'] = summarize(result['runs'])
        result['status'] = 'succeeded'
    except BaseException as error:
        result['status'] = 'failed'
        result['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        result['harness_seconds'] = round(time.perf_counter() - started, 3)
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        save(root, result)
    print(f'Comparison succeeded: {root / "README.md"}', flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--left', type=Path, default=BASELINE)
    parser.add_argument('--right', type=Path, default=BASELINE)
    parser.add_argument('--source-dir', type=Path, default=REPO / 'data/raw/taobao')
    parser.add_argument('--output-dir', type=Path, default=REPO / 'outputs/comparisons')
    parser.add_argument('--comparison-id', default=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--change', required=True, help='Describe the one deliberate change (or the A/A control)')
    parser.add_argument('--workload', default='Full three-source Bronze/Silver/Gold rebuild')
    parser.add_argument('--changed-setting', action='append', default=[], help='A Spark SQL setting intentionally varied by this experiment')
    args = parser.parse_args()
    run_comparison(args.left, args.right, args.source_dir, args.output_dir,
                   args.comparison_id, args.repeats, args.change, args.workload, args.changed_setting)


if __name__ == '__main__':
    main()
