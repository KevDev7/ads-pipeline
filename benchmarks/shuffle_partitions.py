"""Verify initial Gold shuffle partitions and observe actual AQE reader tasks."""
import argparse
import json
from pathlib import Path
import re

from benchmarks.join_strategies import GOLD, REPO, join_nodes


def inspect_run(event_dir, expected_initial, plans_dir=None):
    plans, ends, stages = {}, {}, {}
    for path in sorted(event_dir.rglob('*')):
        if not path.is_file() or path.name.startswith(('.', 'appstatus')):
            continue
        for line in path.open():
            event = json.loads(line)
            kind = event.get('Event', '')
            if 'sparkPlanInfo' in event:
                plans[str(event['executionId'])] = event
            if kind.endswith('SparkListenerSQLExecutionEnd'):
                ends[str(event['executionId'])] = event
            if kind == 'SparkListenerStageSubmitted':
                info = event['Stage Info']; props = event.get('Properties') or {}
                if props.get('spark.jobGroup.id') not in GOLD:
                    continue
                key = (info['Stage ID'], info['Stage Attempt ID'])
                stages[key] = {
                    'stage_id': key[0], 'attempt_id': key[1],
                    'execution_id': str(props['spark.sql.execution.id']),
                    'group': props['spark.jobGroup.id'], 'scheduled_tasks': info['Number of Tasks'],
                    'completed': False, 'task_attempts': 0, 'failed_task_attempts': 0,
                    'shuffle_read_bytes': 0, 'shuffle_write_bytes': 0,
                    'disk_spill_bytes': 0, 'executor_cpu_ns': 0,
                }
            if kind == 'SparkListenerStageCompleted':
                info = event['Stage Info']; key = (info['Stage ID'], info['Stage Attempt ID'])
                if key in stages:
                    stages[key]['completed'] = True
                    stages[key]['failure_reason'] = info.get('Failure Reason')
            if kind == 'SparkListenerTaskEnd':
                key = (event['Stage ID'], event['Stage Attempt ID'])
                if key not in stages:
                    continue
                row = stages[key]; metric = event.get('Task Metrics', {})
                read = metric.get('Shuffle Read Metrics', {})
                row['task_attempts'] += 1
                row['failed_task_attempts'] += int(event.get('Task End Reason', {}).get('Reason') != 'Success')
                row['shuffle_read_bytes'] += read.get('Remote Bytes Read', 0) + read.get('Local Bytes Read', 0)
                row['shuffle_write_bytes'] += metric.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0)
                row['disk_spill_bytes'] += metric.get('Disk Bytes Spilled', 0)
                row['executor_cpu_ns'] += metric.get('Executor CPU Time', 0)
    result = {}
    executions = {(row['execution_id'], row['group']) for row in stages.values()}
    for execution, group in sorted(executions):
        if execution not in plans:
            continue
        plan = plans[execution]; joins = join_nodes(plan['sparkPlanInfo'])
        if not joins:
            continue  # Exclude Delta metadata SQL executions sharing the Gold job group.
        if group in result:
            raise ValueError(f'Ambiguous joined executions for {group}')
        if execution not in ends or ends[execution].get('errorMessage'):
            raise ValueError(f'Gold SQL execution did not succeed: {group}')
        actual = {node['key']: node['operator'] for node in joins}
        if len(joins) != 2 or actual != {'adgroup_id': 'BroadcastHashJoin', 'user_id': 'BroadcastHashJoin'}:
            raise ValueError(f'Join strategy changed: {group}: {joins}')
        if not all('LeftOuter' in node['description'] for node in joins):
            raise ValueError(f'Join type changed: {group}')
        exchanges, reads = [], []
        def walk(node):
            if node['nodeName'] == 'Exchange' and 'hashpartitioning(' in node['simpleString']:
                match = re.search(r'hashpartitioning\(.*?,\s*(\d+)\),', node['simpleString'])
                if not match:
                    raise ValueError(f'Cannot read initial partition count: {node["simpleString"]}')
                exchanges.append({'description': node['simpleString'], 'initial_partitions': int(match[1])})
            if node['nodeName'] == 'AQEShuffleRead':
                reads.append(node['simpleString'])
            for child in node.get('children', []):
                walk(child)
        walk(plan['sparkPlanInfo'])
        if not exchanges or any(x['initial_partitions'] != expected_initial for x in exchanges):
            raise ValueError(f'{group}: expected initial partitions {expected_initial}, found {exchanges}')
        selected = sorted((row for row in stages.values() if row['execution_id'] == execution),
                          key=lambda row: (row['stage_id'], row['attempt_id']))
        if not selected or any(not row['completed'] or row.get('failure_reason') or row['failed_task_attempts']
                               or row['task_attempts'] != row['scheduled_tasks'] for row in selected):
            raise ValueError(f'Incomplete or failed stage/task evidence: {group}')
        readers = [row for row in selected if row['shuffle_read_bytes'] > 0]
        if not readers:
            raise ValueError(f'Missing actual shuffle-reader stage: {group}')
        result[group] = {'execution_id': execution, 'sql_succeeded': True, 'joins': joins,
                         'exchanges': exchanges, 'aqe_shuffle_reads': reads,
                         'stages': selected, 'shuffle_reader_stages': readers,
                         'matches_expected': True}
        if plans_dir:
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / f'{group}.txt').write_text(plan['physicalPlanDescription'].replace(str(REPO), '<repo>'))
    if set(result) != set(GOLD):
        raise ValueError(f'Missing Gold evidence: {set(GOLD) - set(result)}')
    return result


def inspect_comparison(root):
    comparison = json.loads((root / 'comparison.json').read_text())
    if comparison['status'] != 'succeeded':
        raise ValueError('A successful pipeline comparison is required')
    result = {'status': 'running', 'runs': {}, 'method':
        'Latest runtime plan for each successful joined Gold SQL execution; stages matched by SQL execution ID, excluding Delta metadata SQL. Initial hash-partition counts are checked; actual reader tasks and AQE coalescing are observed, not prescribed. Both joins must remain broadcast left outer. Task counters are not unique physical disk reads.'}
    try:
        for run in comparison['runs']:
            expected = 200 if run['side'] == 'left' else 32
            settings = run['report']['environment']['spark_sql_settings']
            if settings['spark.sql.shuffle.partitions'] != str(expected) or settings['spark.sql.adaptive.enabled'] != 'true':
                raise ValueError(f'Unexpected shuffle/AQE setting in {run["run_id"]}')
            result['runs'][run['run_id']] = inspect_run(
                root / 'runs' / run['run_id'] / 'events', expected,
                root / 'shuffle-plans' / run['run_id'])
        result['status'] = 'succeeded'
    except BaseException as error:
        result['status'] = 'failed'; result['error'] = str(error)
        raise
    finally:
        (root / 'shuffle-partitions.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print('Initial partitions, actual stages and unchanged joins verified', flush=True)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison_dir', type=Path)
    args = parser.parse_args()
    inspect_comparison(args.comparison_dir.resolve())
