"""Run iteration 7 with cached enrichment and ZSTD compression: CSV -> Bronze -> Silver -> Gold -> checks."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from cache_observation import CachedEnrichment
from measurement import environment, fingerprint, storage, summarize_events
from session import create_spark
from tables import SOURCES, aggregate, clean, enrich, read_csv, read_table, write_table
from validation import require, valid_dimensions, valid_gold, valid_impressions


def run(source: Path, output: Path, run_id: str) -> dict:
    if Path(run_id).name != run_id or run_id in ('', '.', '..'):
        raise ValueError('run-id must be a single directory name')
    source, output = source.resolve(), output.resolve()
    for filename, _ in SOURCES.values():
        if not (source / filename).is_file():
            raise FileNotFoundError(source / filename)
    root = output / run_id
    root.mkdir(parents=True, exist_ok=False)
    report = {'run_id': run_id, 'status': 'running', 'started_at': datetime.now(timezone.utc).isoformat(),
              'sources': {}, 'seconds': {}, 'tables': {}, 'checks': {}}
    started = time.perf_counter()
    spark = None
    cached = None
    release_attempted = False

    def save_report():
        path = root / 'report.json'
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
        temporary.replace(path)

    @contextmanager
    def timed(name):
        print(f'Starting {name}', flush=True)
        if spark is not None:
            spark.sparkContext.setJobGroup(name, name)
        begin = time.perf_counter()
        try:
            yield
        finally:
            report['seconds'][name] = round(time.perf_counter() - begin, 3)
            save_report()
            print(f'{name}: {report["seconds"][name]}s', flush=True)

    def record_table(layer, name, rows):
        report['tables'][f'{layer}/{name}'] = {'rows': rows, **storage(root / layer / name)}

    def release_cache():
        nonlocal release_attempted
        if cached is None or release_attempted:
            return
        release_attempted = True
        with timed('gold.cache_release'):
            cached.release()
            report['cache']['snapshots']['after_release'] = cached.snapshot(spark)

    save_report()
    try:
        with timed('source_fingerprints'):
            report['sources'] = {filename: fingerprint(source / filename) for filename, _ in SOURCES.values()}
        with timed('spark_startup'):
            spark = create_spark(root / 'events')
            report['environment'] = environment(spark)
        for name in SOURCES:
            with timed(f'bronze.{name}'):
                write_table(read_csv(spark, source, name), root, 'bronze', name)
        for name in SOURCES:
            with timed(f'silver.{name}'):
                write_table(clean(name, read_table(spark, root, 'bronze', name)), root, 'silver', name)
        silver = {name: read_table(spark, root, 'silver', name) for name in SOURCES}
        with timed('validate.silver'):
            for name in SOURCES:
                bronze_count = read_table(spark, root, 'bronze', name).count()
                silver_count = silver[name].count()
                require(bronze_count == silver_count and bronze_count > 0, f'{name}: empty source or lost rows')
                record_table('bronze', name, bronze_count)
                record_table('silver', name, silver_count)
            report['checks'].update(valid_dimensions(silver['ads'], silver['user_profiles']))
            expected = valid_impressions(silver['impressions'], silver['ads'], silver['user_profiles'])
            report['checks']['silver_totals'] = expected
            report['checks']['bronze_silver_row_counts_match'] = True
        plans = root / 'plans'
        plans.mkdir()
        for name, audience in [('campaign_daily', False), ('audience_daily', True)]:
            if audience:
                report['cache']['snapshots']['before_audience'] = cached.snapshot(spark)
                save_report()
            with timed(f'gold.{name}'):
                if cached is None:
                    cached = CachedEnrichment(enrich(silver['impressions'], silver['ads'], silver['user_profiles']))
                    report['cache'] = {'requested_storage_level': 'MEMORY_AND_DISK_DESER', 'snapshots': {}}
                result = aggregate(cached.frame, audience)
                (plans / f'{name}.txt').write_text(result._jdf.queryExecution().toString())
                write_table(result, root, 'gold', name)
            report['cache']['snapshots']['after_audience' if audience else 'after_campaign'] = cached.snapshot(spark)
            save_report()
            with timed(f'validate.{name}'):
                stats = valid_gold(read_table(spark, root, 'gold', name), expected, name)
                report['checks'][name] = stats
                record_table('gold', name, stats['rows'])
        release_cache()
        with timed('spark_shutdown'):
            spark.stop()
            spark = None
        report['spark_tasks'] = summarize_events(root / 'events')
        report['seconds']['pipeline_writes'] = round(sum(v for k, v in report['seconds'].items() if k.startswith(('bronze.', 'silver.', 'gold.'))), 3)
        report['seconds']['validation'] = round(sum(v for k, v in report['seconds'].items() if k.startswith('validate.')), 3)
        report['storage'] = {'source_bytes': sum(v['bytes'] for v in report['sources'].values()),
                             'table_bytes': sum(v['total_bytes'] for v in report['tables'].values()),
                             'event_log_bytes': storage(root / 'events')['total_bytes']}
        report['status'] = 'succeeded'
    except BaseException as error:
        report['status'] = 'failed'
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        if spark is not None:
            try:
                release_cache()
            except Exception as cleanup_error:
                report['cache_cleanup_error'] = str(cleanup_error)
        if spark is not None:
            try:
                spark.stop()
            except Exception as shutdown_error:
                # A dead JVM must not hide the original failure or prevent its report.
                report['shutdown_error'] = str(shutdown_error)
        report['seconds']['total'] = round(time.perf_counter() - started, 3)
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        save_report()
    print(f'Cached enrichment iteration succeeded. Report: {root / "report.json"}', flush=True)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir', type=Path, default=Path('data/raw/taobao'))
    parser.add_argument('--output-dir', type=Path, default=Path('outputs/07_cached_enrichment'))
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    args = parser.parse_args()
    run(args.source_dir, args.output_dir, args.run_id)
