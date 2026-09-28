"""Compare equivalent typed CSV and uncompressed Parquet impressions."""
import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

from pyspark.sql import functions as F, types as T

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / 'experiments/00_baseline'))
from session import create_spark
from measurement import environment, fingerprint, storage, summarize_events
from benchmarks.equality import compare_frames
from benchmarks.reporting import distribution
from benchmarks.storage_codecs import inspect_table

SCHEMA = T.StructType([T.StructField(n, t, True) for n, t in [
    ('user', T.LongType()), ('time_stamp', T.LongType()), ('adgroup_id', T.LongType()),
    ('pid', T.StringType()), ('nonclk', T.IntegerType()), ('clk', T.IntegerType())]])
WORKLOADS = ('narrow', 'wide', 'one_day')
DAY_START = int(datetime.fromisoformat('2017-05-08T00:00:00+08:00').timestamp())


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def read_csv(spark, source):
    with source.open(newline='') as handle:
        if next(csv.reader(handle)) != SCHEMA.fieldNames():
            raise ValueError('Unexpected raw_sample.csv header')
    return (spark.read.schema(SCHEMA).option('header', True).option('enforceSchema', False)
            .option('mode', 'FAILFAST').option('nullValue', 'NULL').option('escape', '"').csv(str(source.resolve())))


def read(spark, source, root, format_name):
    return read_csv(spark, source) if format_name == 'csv' else spark.read.parquet(str(root / 'parquet'))


def query(frame, workload):
    if workload == 'one_day':
        frame = frame.filter((F.col('time_stamp') >= DAY_START) & (F.col('time_stamp') < DAY_START + 86400))
    if workload not in WORKLOADS:
        raise ValueError(workload)
    if workload == 'wide':
        return frame.groupBy('pid').agg(F.count('*').alias('impressions'), *[
            F.sum(c).alias(f'sum_{c}') for c in ('user', 'time_stamp', 'adgroup_id', 'nonclk', 'clk')])
    return frame.groupBy('pid').agg(F.count('*').alias('impressions'), F.sum('clk').alias('clicks'))


def scans(plan):
    result = []
    def visit(node):
        name = str(node.nodeName())
        if name == 'AdaptiveSparkPlan':
            visit(node.executedPlan()); return
        if name.endswith('QueryStage'):
            visit(node.plan()); return
        if name.startswith(('Scan csv', 'Scan parquet')):
            metrics = {}; iterator = node.metrics().iterator()
            while iterator.hasNext():
                pair = iterator.next(); metrics[str(pair._1())] = int(pair._2().value())
            result.append({'node': name, 'metrics': metrics})
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())
    visit(plan)
    if not result:
        raise ValueError('Missing executed file scan')
    return result


def prepare(source, root):
    spark = create_spark(root / 'prepare-events')
    result = {'status': 'running', 'source': fingerprint(source), 'environment': environment(spark)}
    try:
        frame = read_csv(spark, source)
        spark.sparkContext.setJobGroup('prepare.parquet', 'CSV to uncompressed Parquet')
        started = time.perf_counter()
        frame.write.mode('errorifexists').option('compression', 'uncompressed').parquet(str(root / 'parquet'))
        result['conversion_seconds'] = time.perf_counter() - started
        result['parquet_storage'] = storage(root / 'parquet')
        result['status'] = 'succeeded'
    finally:
        spark.stop()
        result['spark_tasks'] = summarize_events(root / 'prepare-events')
        save(root / 'preparation.json', result)


def run_queries(source, root, run_id, format_name, order):
    output = root / 'queries' / run_id; output.mkdir(parents=True, exist_ok=False)
    spark = create_spark(output / 'events')
    result = {'status': 'running', 'format': format_name, 'order': order,
              'environment': environment(spark), 'queries': {}}
    try:
        # Relation/schema/file-list preparation is excluded equally on both sides.
        frame = read(spark, source, root, format_name)
        result['schema'] = frame.schema.jsonValue()
        for name in order:
            spark.sparkContext.setJobGroup(f'workload.{name}', name)
            started = time.perf_counter()
            df = query(frame, name); rows = df.collect()
            elapsed = time.perf_counter() - started
            plan = df._jdf.queryExecution().executedPlan()
            (output / f'{name}.txt').write_text(str(plan.toString()).replace(str(REPO), '<repo>'))
            result['queries'][name] = {'seconds': elapsed,
                'rows': sorted([r.asDict() for r in rows], key=lambda r: json.dumps(r, sort_keys=True)),
                'scans': scans(plan)}
        result['status'] = 'succeeded'
    finally:
        spark.stop()
        result['spark_tasks'] = summarize_events(output / 'events')
        save(output / 'report.json', result)


def verify(source, root):
    spark = create_spark(root / 'verification-events')
    try:
        a, b = read_csv(spark, source), spark.read.parquet(str(root / 'parquet'))
        check = compare_frames(a, b)
        if not check['equal']:
            raise ValueError('CSV and Parquet records differ')
        rows = b.count()
        footer = inspect_table(spark, root / 'parquet', 'UNCOMPRESSED')
        if footer['rows'] != rows:
            raise ValueError('Footer rows disagree with data')
        prepared = json.loads((root / 'preparation.json').read_text())
        if footer['bytes'] != prepared['parquet_storage']['parquet_bytes']:
            raise ValueError('Parquet file sizes changed')
        if fingerprint(source) != prepared['source']:
            raise ValueError('CSV source changed during comparison')
        save(root / 'verification.json', {'status': 'succeeded', 'equality': check,
                                          'rows': rows, 'parquet_footer': footer})
    finally:
        spark.stop()


def markdown(result):
    lines = ['# Iteration 1: CSV versus uncompressed Parquet', '',
             f"Status: **{result['status']}**.", '',
             'Same typed impressions and queries. Preparation, query actions, and verification are measured separately.', '']
    if 'preparation' in result:
        p = result['preparation']
        lines += [f"CSV data bytes: {p['source']['bytes']:,}.",
                  f"Parquet data bytes: {p['parquet_storage']['parquet_bytes']:,}.",
                  f"Conversion action: {p['conversion_seconds']:.3f} seconds (one observation).", '']
    lines += ['| Query | CSV median [min–max] seconds | Parquet median [min–max] seconds |', '| --- | ---: | ---: |']
    for name, stats in result.get('summary', {}).items():
        def cell(side):
            t = stats[side]['seconds']; return f"{t['median']:.3f} [{t['min']:.3f}–{t['max']:.3f}]"
        lines.append(f'| {name} | {cell("csv")} | {cell("parquet")} |')
    lines += ['', 'All samples, results, scan metrics, CPU/I/O/shuffle/spill counters, and provenance are in',
              '[format-comparison.json](format-comparison.json). Executed plans are in `queries/<run-id>/`.', '',
              'Three repeats are the default; one repeat is only a smoke check. OS caches are not flushed.',
              'Timing excludes Spark startup and relation preparation. No data-query warm-up or explicit caching.',
              'Query order rotates; format order alternates. This is a read-workload experiment, not a full lakehouse rebuild.',
              'Original CSV and naturally written Parquet have different file layouts; no repartitioning is added.',
              'Compression is disabled for Parquet so this does not repeat the codec experiment.']
    if result.get('error'): lines += ['', f"Failure: {result['error']}"]
    return '\n'.join(lines) + '\n'


def compare(source, root, repeats):
    root.mkdir(parents=True, exist_ok=False); (root / 'logs').mkdir()
    result = {'status': 'running', 'repeats': repeats, 'runs': [], 'summary': {},
              'started_at': datetime.now(timezone.utc).isoformat()}
    def command(args, log):
        with (root / 'logs' / log).open('w') as handle:
            subprocess.run(args, cwd=REPO, stdout=handle, stderr=subprocess.STDOUT, check=True)
    def child(stage, *args):
        return [sys.executable, str(Path(__file__).resolve()), '--stage', stage,
                '--source-dir', str(source.parent), '--root', str(root), *args]
    def persist():
        save(root / 'format-comparison.json', result)
        (root / 'README.md').write_text(markdown(result))
    try:
        persist()
        print('Running test suite', flush=True)
        command([sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-v'], 'tests.log')
        result['tests_passed'] = True
        print('Preparing uncompressed Parquet once', flush=True)
        command(child('prepare'), 'prepare.log')
        result['preparation'] = json.loads((root / 'preparation.json').read_text()); persist()
        for repetition in range(repeats):
            formats = ('csv', 'parquet') if repetition % 2 == 0 else ('parquet', 'csv')
            offset = repetition % len(WORKLOADS)
            order = WORKLOADS[offset:] + WORKLOADS[:offset]
            for format_name in formats:
                run_id = f'{repetition+1:02d}-{format_name}'
                print(f'Querying {run_id}: {order}', flush=True)
                command(child('query', '--format', format_name, '--run-id', run_id, '--order', *order), f'{run_id}.log')
                report = json.loads((root / 'queries' / run_id / 'report.json').read_text())
                if report['status'] != 'succeeded' or report['environment'] != result['preparation']['environment']:
                    raise ValueError('Query status/environment mismatch')
                if result['runs']:
                    reference = result['runs'][0]['report']
                    if report['schema'] != reference['schema']:
                        raise ValueError('Read schemas differ')
                    for name in WORKLOADS:
                        if report['queries'][name]['rows'] != reference['queries'][name]['rows']:
                            raise ValueError(f'Query mismatch: {run_id} {name}')
                result['runs'].append({'run_id': run_id, 'report': report}); persist()
        print('Verifying every source row and every Parquet codec', flush=True)
        command(child('verify'), 'verify.log')
        result['verification'] = json.loads((root / 'verification.json').read_text())
        for name in WORKLOADS:
            result['summary'][name] = {}
            for format_name in ('csv', 'parquet'):
                reports = [x['report'] for x in result['runs'] if x['report']['format'] == format_name]
                metrics = {'seconds': distribution([x['queries'][name]['seconds'] for x in reports])}
                for metric in ('input_bytes', 'executor_cpu_ns', 'shuffle_write_bytes', 'disk_spill_bytes'):
                    metrics[metric] = distribution([x['spark_tasks']['by_operation'][f'workload.{name}'][metric] for x in reports])
                result['summary'][name][format_name] = metrics
        result['status'] = 'succeeded'
    except BaseException as error:
        result['status'] = 'failed'; result['error'] = f'{type(error).__name__}: {error}'; raise
    finally:
        result['finished_at'] = datetime.now(timezone.utc).isoformat(); persist()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=['compare', 'prepare', 'query', 'verify'], default='compare')
    parser.add_argument('--source-dir', type=Path, default=REPO / 'data/raw/taobao')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--format', choices=['csv', 'parquet'])
    parser.add_argument('--run-id')
    parser.add_argument('--order', nargs=3, choices=WORKLOADS, default=list(WORKLOADS))
    args = parser.parse_args(); root = args.root.resolve(); source = args.source_dir.resolve() / 'raw_sample.csv'
    if args.repeats < 1: parser.error('repeats must be positive')
    if not source.is_file(): raise FileNotFoundError(source)
    if args.stage == 'compare': compare(source, root, args.repeats)
    elif args.stage == 'prepare': prepare(source, root)
    elif args.stage == 'verify': verify(source, root)
    elif args.format and args.run_id:
        if Path(args.run_id).name != args.run_id or args.run_id in ('.', '..'): parser.error('Invalid run-id')
        run_queries(source, root, args.run_id, args.format, args.order)
    else: parser.error('query requires --format and --run-id')


if __name__ == '__main__':
    main()
