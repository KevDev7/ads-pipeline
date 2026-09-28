"""Repeated read workloads on completed pipeline outputs, separate from rebuilds."""
import argparse
from datetime import date
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time

from pyspark.sql import functions as F

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / 'experiments/02_compression'))
from session import create_spark
from measurement import environment, summarize_events

WORKLOADS = ('one_day', 'three_days', 'full_period')


def query(frame, workload):
    if workload == 'one_day':
        frame = frame.filter(F.col('reporting_date') == F.lit(date(2017, 5, 8)))
    elif workload == 'three_days':
        frame = frame.filter(F.col('reporting_date').between(
            F.lit(date(2017, 5, 8)), F.lit(date(2017, 5, 10))))
    elif workload != 'full_period':
        raise ValueError(workload)
    # SUM(clicked) requires reading data; this is not a metadata-only COUNT.
    return frame.groupBy('placement_id').agg(
        F.count('*').alias('impressions'), F.sum('clicked').alias('clicks'))


def scan_metrics(plan):
    """Inspect the executed AQE plan, not an estimate from inputFiles()."""
    scans = []

    def visit(node):
        name = str(node.nodeName())
        if name == 'AdaptiveSparkPlan':
            visit(node.executedPlan())
            return
        if name.endswith('QueryStage'):
            visit(node.plan())
            return
        if name.startswith('Scan parquet'):
            metrics = {}
            iterator = node.metrics().iterator()
            while iterator.hasNext():
                pair = iterator.next()
                metrics[str(pair._1())] = int(pair._2().value())
            scans.append({'node': name, 'metrics': metrics})
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())

    visit(plan)
    if not scans or not all('numFiles' in scan['metrics'] for scan in scans):
        raise ValueError('Executed file-scan metrics are missing')
    return scans


def run_queries(table: Path, output: Path, order):
    output.mkdir(parents=True, exist_ok=False)
    spark = create_spark(output / 'events')
    result = {'status': 'running', 'order': list(order), 'queries': {}}
    try:
        result['environment'] = environment(spark)
        from delta.tables import DeltaTable
        # Warm metadata identically on both sides; no data query is a warm-up.
        detail = DeltaTable.forPath(spark, str(table.resolve())).detail().select(
            'partitionColumns', 'numFiles', 'sizeInBytes').first().asDict()
        result['table'] = detail
        frame = spark.read.format('delta').load(str(table.resolve()))
        for workload in order:
            spark.sparkContext.setJobGroup(f'workload.{workload}', workload)
            started = time.perf_counter()
            df = query(frame, workload)
            rows = df.collect()
            elapsed = time.perf_counter() - started
            values = sorted([r.asDict() for r in rows], key=lambda r: json.dumps(r, sort_keys=True))
            plan = df._jdf.queryExecution().executedPlan()
            plan_text = str(plan.toString()).replace(str(REPO), '<repo>')
            (output / f'{workload}.txt').write_text(plan_text)
            result['queries'][workload] = {
                'seconds': elapsed, 'rows': values, 'scans': scan_metrics(plan)}
        result['status'] = 'succeeded'
    finally:
        spark.stop()
        result['spark_tasks'] = summarize_events(output / 'events')
        (output / 'report.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    return result


def compare_queries(root: Path):
    comparison = json.loads((root / 'comparison.json').read_text())
    if comparison['status'] != 'succeeded':
        raise ValueError('Complete the pipeline comparison and equality checks first')
    output = root / 'queries'
    output.mkdir(exist_ok=False)
    result = {'status': 'running', 'runs': [], 'summary': {},
              'method': 'Fresh process per pipeline output; metadata warmed; no data warm-up or cache; workload order rotates by repetition. Timings include planning and collect, exclude Spark startup, metadata warm-up, and plan/metric inspection.'}
    try:
        for item in comparison['runs']:
            run_id = item['run_id']
            repetition = int(run_id.split('-')[0]) - 1
            offset = repetition % len(WORKLOADS)
            order = WORKLOADS[offset:] + WORKLOADS[:offset]
            print(f'Querying {run_id}: {order}', flush=True)
            with (output / f'{run_id}.log').open('w') as log:
                subprocess.run([sys.executable, '-m', 'benchmarks.partition_queries',
                                '--table', str(root / 'runs' / run_id / 'silver/impressions'),
                                '--output', str(output / run_id), '--order', *order],
                               cwd=REPO, stdout=log, stderr=subprocess.STDOUT, check=True)
            report = json.loads((output / run_id / 'report.json').read_text())
            if report['status'] != 'succeeded':
                raise ValueError(f'Failed query run: {run_id}')
            if report['environment'] != item['report']['environment']:
                raise ValueError('Query environment differs from its pipeline run')
            if result['runs']:
                reference = result['runs'][0]['report']['queries']
                for name in WORKLOADS:
                    if report['queries'][name]['rows'] != reference[name]['rows']:
                        raise ValueError(f'Query result mismatch: {run_id} {name}')
            result['runs'].append({'run_id': run_id, 'side': item['side'], 'report': report})
        for workload in WORKLOADS:
            stats = {}
            for side in ('left', 'right'):
                records = [x['report'] for x in result['runs'] if x['side'] == side]
                def distribution(values):
                    return {'samples': values, 'median': statistics.median(values),
                            'min': min(values), 'max': max(values)}
                stats[side] = {
                    'seconds': distribution([r['queries'][workload]['seconds'] for r in records]),
                    'files_scanned': distribution([sum(s['metrics']['numFiles'] for s in r['queries'][workload]['scans']) for r in records]),
                    'selected_file_bytes': distribution([sum(s['metrics']['filesSize'] for s in r['queries'][workload]['scans']) for r in records]),
                    'task_input_bytes': distribution([r['spark_tasks']['by_operation'][f'workload.{workload}']['input_bytes'] for r in records]),
                }
            result['summary'][workload] = stats
        result['status'] = 'succeeded'
    finally:
        (root / 'query-comparison.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print('All query results match; query-comparison.json written', flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--comparison-dir', type=Path)
    parser.add_argument('--table', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--order', nargs=3, choices=WORKLOADS, default=list(WORKLOADS))
    args = parser.parse_args()
    if args.comparison_dir:
        compare_queries(args.comparison_dir.resolve())
    elif args.table and args.output:
        run_queries(args.table, args.output, args.order)
    else:
        parser.error('Provide --comparison-dir, or --table and --output')


if __name__ == '__main__':
    main()
