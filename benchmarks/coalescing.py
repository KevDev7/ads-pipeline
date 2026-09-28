"""Observe coalescing through effective settings, executed plans and actual tasks."""
import argparse
import json
import math
from pathlib import Path
import re
import statistics

from benchmarks.join_strategies import GOLD, REPO, join_nodes
from benchmarks.observed_run import SETTINGS
from benchmarks.reporting import distribution, validate_reports

KEY = 'spark.sql.adaptive.coalescePartitions.enabled'


def sizes(values):
    values = sorted(values)
    if not values:
        raise ValueError('Missing size/duration observations')
    return dict(count=len(values), total=sum(values), min=values[0], median=statistics.median(values),
                p95=values[math.ceil(.95 * len(values)) - 1], max=values[-1])


def inspect_run(event_dir, report, enabled, plans_dir=None):
    settings = report['environment']['spark_sql_settings']
    if any(key not in settings for key in SETTINGS):
        raise ValueError('Missing effective coalescing settings')
    for key, expected in {KEY: str(enabled).lower(), 'spark.sql.adaptive.enabled': 'true',
                          'spark.sql.shuffle.partitions': '200'}.items():
        if settings.get(key) != expected:
            raise ValueError(f'Contradictory setting {key}: expected {expected}')
    plans, ends, stages = {}, {}, {}
    for path in sorted(event_dir.rglob('*')):
        if not path.is_file() or path.name.startswith(('.', 'appstatus')):
            continue
        with path.open() as handle:
            for line in handle:
                e = json.loads(line); kind = e.get('Event', '')
                if 'sparkPlanInfo' in e:
                    plans[str(e['executionId'])] = e
                if kind.endswith('SparkListenerSQLExecutionEnd'):
                    ends[str(e['executionId'])] = e
                if kind == 'SparkListenerStageSubmitted':
                    info = e['Stage Info']; props = e.get('Properties') or {}
                    if props.get('spark.jobGroup.id') not in GOLD:
                        continue
                    key = (info['Stage ID'], info['Stage Attempt ID'])
                    stages[key] = dict(stage_id=key[0], attempt_id=key[1],
                        execution_id=str(props.get('spark.sql.execution.id')), group=props['spark.jobGroup.id'],
                        scheduled_tasks=info['Number of Tasks'], completed=False, tasks=0, failed_tasks=0,
                        task_durations_ms=[], shuffle_read_bytes=0, shuffle_write_bytes=0,
                        disk_spill_bytes=0, memory_spill_bytes=0, executor_cpu_ns=0, executor_run_ms=0)
                if kind == 'SparkListenerStageCompleted':
                    info = e['Stage Info']; key = (info['Stage ID'], info['Stage Attempt ID'])
                    if key in stages:
                        stages[key]['completed'] = not bool(info.get('Failure Reason'))
                if kind == 'SparkListenerTaskEnd':
                    key = (e['Stage ID'], e['Stage Attempt ID'])
                    if key not in stages:
                        continue
                    s = stages[key]; m = e.get('Task Metrics', {}); t = e['Task Info']
                    s['tasks'] += 1
                    s['failed_tasks'] += int(e.get('Task End Reason', {}).get('Reason') != 'Success')
                    if t.get('Finish Time', 0) < t.get('Launch Time', 0) or 'Finish Time' not in t or 'Launch Time' not in t:
                        raise ValueError('Missing or contradictory task timing evidence')
                    s['task_durations_ms'].append(t['Finish Time'] - t['Launch Time'])
                    read = m.get('Shuffle Read Metrics', {})
                    s['shuffle_read_bytes'] += read.get('Remote Bytes Read', 0) + read.get('Local Bytes Read', 0)
                    s['shuffle_write_bytes'] += m.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0)
                    for dest, source in [('disk_spill_bytes', 'Disk Bytes Spilled'), ('memory_spill_bytes', 'Memory Bytes Spilled'),
                                         ('executor_cpu_ns', 'Executor CPU Time'), ('executor_run_ms', 'Executor Run Time')]:
                        s[dest] += m.get(source, 0)
    result = {}
    for execution, group in sorted({(s['execution_id'], s['group']) for s in stages.values()}):
        if execution not in plans:
            raise ValueError('Missing executed SQL plan')
        plan = plans[execution]; joins = join_nodes(plan['sparkPlanInfo'])
        if not joins:
            continue
        if group in result:
            raise ValueError(f'Ambiguous Gold execution: {group}')
        if execution not in ends or ends[execution].get('errorMessage'):
            raise ValueError(f'Gold SQL did not succeed: {group}')
        if {j['key'] for j in joins} != {'user_id', 'adgroup_id'} or len(joins) != 2 or any('LeftOuter' not in j['description'] for j in joins):
            raise ValueError('Unexpected logical join evidence')
        exchanges, reads = [], []
        def walk(node):
            if node['nodeName'] == 'Exchange' and 'hashpartitioning(' in node['simpleString']:
                match = re.search(r'hashpartitioning\(.*?,\s*(\d+)\),', node['simpleString'])
                if not match:
                    raise ValueError('Unreadable initial partition count')
                exchanges.append(dict(description=node['simpleString'], initial_partitions=int(match[1])))
            if node['nodeName'] == 'AQEShuffleRead':
                reads.append(node['simpleString'])
            for child in node.get('children', []): walk(child)
        walk(plan['sparkPlanInfo'])
        if not exchanges or any(x['initial_partitions'] != 200 for x in exchanges):
            raise ValueError('Missing or contradictory initial 200 partitions')
        coalesced = any('coalesced' in x.lower() for x in reads)
        if not enabled and coalesced:
            raise ValueError('Coalesced reader contradicts disabled setting')
        selected = [s for s in stages.values() if s['execution_id'] == execution]
        if not selected or any(not s['completed'] or s['failed_tasks'] or s['tasks'] != s['scheduled_tasks'] for s in selected):
            raise ValueError('Missing, incomplete or failed stage/task evidence')
        readers = [s for s in selected if s['shuffle_read_bytes'] > 0]
        if not readers:
            raise ValueError('Missing executed shuffle-reader stage')
        for s in selected:
            s['task_duration_ms'] = sizes(s['task_durations_ms'])
        result[group] = dict(execution_id=execution, joins=joins, exchanges=exchanges,
            aqe_shuffle_reads=reads, coalescing_observed=coalesced, stages=selected,
            reader_stage_ids=[s['stage_id'] for s in readers],
            reader_tasks=sum(s['tasks'] for s in readers),
            task_duration_ms=sizes([v for s in selected for v in s['task_durations_ms']]))
        if plans_dir:
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / f'{group}.txt').write_text(plan['physicalPlanDescription'].replace(str(REPO), '<repo>'))
    if set(result) != set(GOLD):
        raise ValueError('Missing Gold execution evidence')
    selected_ids = {r['execution_id'] for r in result.values()}
    other = [s for s in stages.values() if s['execution_id'] not in selected_ids]
    for s in other:
        if s['task_durations_ms']: s['task_duration_ms'] = sizes(s['task_durations_ms'])
    return dict(settings={k: settings[k] for k in SETTINGS}, executions=result,
                other_gold_stages=other,
                other_gold_note='Outside the joined report SQL, primarily Delta metadata work; not counted as Gold shuffle-reader stages.')


def inspect_comparison(root):
    comparison = json.loads((root / 'comparison.json').read_text())
    result = dict(status='running', runs={}, method='Effective runtime settings, latest successful Gold SQL plans, and executed stage/task evidence. Reader counts are observed, not prescribed. Task durations include execution overhead; they are not a direct measurement of scheduler overhead. Size p95 uses nearest rank.')
    try:
        if comparison['status'] != 'succeeded': raise ValueError('Successful comparison required')
        reference = comparison['runs'][0]['report']
        for run in comparison['runs']:
            report = run['report']; enabled = run['side'] == 'left'
            diff = validate_reports(reference, report, [] if enabled else [KEY])
            if not enabled and diff != {KEY: {'reference': 'true', 'current': 'false'}}:
                raise ValueError('Expected exactly the declared coalescing setting change')
            data = inspect_run(root/'runs'/run['run_id']/'events', report, enabled,
                               root/'coalescing-plans'/run['run_id'])
            data['combined_gold_seconds'] = round(sum(report['seconds'][k] for k in GOLD), 3)
            data['validation_seconds'] = report['seconds']['validation']
            data['files'] = {}
            for table, stats in report['tables'].items():
                files = [p.stat().st_size for p in (root/'runs'/run['run_id']/table).rglob('*.parquet')]
                data['files'][table] = sizes(files)
                if len(files) != stats['parquet_files'] or sum(files) != stats['parquet_bytes']:
                    raise ValueError('File observations disagree with pipeline report')
            result['runs'][run['run_id']] = data
        result['combined_gold_seconds'] = {side: distribution([result['runs'][r['run_id']]['combined_gold_seconds']
            for r in comparison['runs'] if r['side'] == side]) for side in ('left','right')}
        result['status'] = 'succeeded'
    except BaseException as error:
        result.update(status='failed', error=str(error)); raise
    finally:
        (root/'coalescing-evidence.json').write_text(json.dumps(result, indent=2, sort_keys=True)+'\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('comparison_dir', type=Path)
    inspect_comparison(parser.parse_args().comparison_dir.resolve())
