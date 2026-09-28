"""Prove regular Python date evaluation using execution-scoped runtime evidence."""
import argparse
import json
from pathlib import Path

from benchmarks.join_strategies import GOLD, REPO, join_nodes
from benchmarks.reporting import distribution, validate_reports

ARROW_KEY = 'spark.sql.execution.pythonUDF.arrow.enabled'
GROUPS = ('silver.impressions', *GOLD)
REQUIRED_METRICS = ('data sent to Python workers', 'data returned from Python workers', 'number of output rows')


def nodes(plan):
    yield plan
    for child in plan.get('children', []):
        yield from nodes(child)


def inspect_run(event_dir, report, python, plans_dir=None):
    settings = report.get('environment', {}).get('spark_sql_settings', {})
    if settings.get(ARROW_KEY) not in ('true', 'false'):
        raise ValueError('Missing effective Python Arrow setting')
    for key, expected in {'spark.sql.session.timeZone': 'Asia/Shanghai',
                          'spark.sql.adaptive.enabled': 'true',
                          'spark.sql.shuffle.partitions': '200',
                          'spark.sql.parquet.compression.codec': 'zstd'}.items():
        if settings.get(key) != expected:
            raise ValueError(f'Contradictory setting: {key}')
    if report.get('status') != 'succeeded':
        raise ValueError('Successful pipeline required')
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
                    if props.get('spark.jobGroup.id') in GROUPS:
                        key = (info['Stage ID'], info['Stage Attempt ID'])
                        stages[key] = dict(stage_id=key[0], attempt_id=key[1],
                            execution_id=str(props.get('spark.sql.execution.id')),
                            group=props['spark.jobGroup.id'], scheduled_tasks=info['Number of Tasks'],
                            completed=False, tasks=0, failed_tasks=0, task_ids=[], updates={},
                            task_durations_ms=[], executor_cpu_ns=0, executor_run_ms=0,
                            shuffle_read_bytes=0, shuffle_write_bytes=0, disk_spill_bytes=0, memory_spill_bytes=0)
                if kind == 'SparkListenerStageCompleted':
                    info = e['Stage Info']; key = (info['Stage ID'], info['Stage Attempt ID'])
                    if key in stages:
                        stages[key]['completed'] = not bool(info.get('Failure Reason'))
                if kind == 'SparkListenerTaskEnd':
                    key = (e['Stage ID'], e['Stage Attempt ID'])
                    if key not in stages:
                        continue
                    s = stages[key]; t = e['Task Info']; m = e.get('Task Metrics', {})
                    s['tasks'] += 1; s['task_ids'].append(t['Index'])
                    s['failed_tasks'] += int(e.get('Task End Reason', {}).get('Reason') != 'Success')
                    if t.get('Finish Time', -1) < t.get('Launch Time', 0):
                        raise ValueError('Missing or contradictory task timing')
                    s['task_durations_ms'].append(t['Finish Time'] - t['Launch Time'])
                    for a in t.get('Accumulables', []):
                        if a.get('Metadata') == 'sql' and str(a.get('Update', '')).isdigit():
                            ident = str(a['ID']); s['updates'][ident] = s['updates'].get(ident, 0) + int(a['Update'])
                    read = m.get('Shuffle Read Metrics', {})
                    s['shuffle_read_bytes'] += read.get('Remote Bytes Read', 0) + read.get('Local Bytes Read', 0)
                    s['shuffle_write_bytes'] += m.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0)
                    for dest, source in [('executor_cpu_ns', 'Executor CPU Time'), ('executor_run_ms', 'Executor Run Time'),
                                         ('disk_spill_bytes', 'Disk Bytes Spilled'), ('memory_spill_bytes', 'Memory Bytes Spilled')]:
                        s[dest] += m.get(source, 0)
    result = {}
    for execution, group in sorted({(s['execution_id'], s['group']) for s in stages.values()}):
        if execution not in plans:
            raise ValueError('Missing executed SQL plan')
        event = plans[execution]; tree = list(nodes(event['sparkPlanInfo']))
        python_nodes = [n for n in tree if 'Python' in n.get('nodeName', '') or 'Pandas' in n.get('nodeName', '')]
        if python_nodes and (not python or group in GOLD):
            raise ValueError('Unexpected Python evaluation in control or Gold')
        joins = join_nodes(event['sparkPlanInfo'])
        silver = group == 'silver.impressions'
        # The data-producing Silver execution projects reporting_date from the Bronze
        # Parquet scan. Delta log/state queries in the same group do not qualify.
        scan = any(n.get('nodeName', '').startswith('Scan parquet') and
                   '/bronze/impressions' in json.dumps(n) for n in tree)
        projection = any(n.get('nodeName') == 'Project' and 'reporting_date#' in n.get('simpleString', '') for n in tree)
        if (silver and not (scan and projection)) or (not silver and not joins):
            continue
        if group in result:
            raise ValueError(f'Ambiguous data execution: {group}')
        if execution not in ends or ends[execution].get('errorMessage'):
            raise ValueError(f'SQL did not succeed: {group}')
        selected = [s for s in stages.values() if s['execution_id'] == execution]
        if not selected or any(not s['completed'] or s['failed_tasks'] or s['tasks'] != s['scheduled_tasks'] or
                               len(set(s['task_ids'])) != s['scheduled_tasks'] for s in selected):
            raise ValueError('Incomplete or contradictory stage/task evidence')
        metrics = {}
        if silver and python:
            if len(python_nodes) != 1 or python_nodes[0]['nodeName'] != 'BatchEvalPython':
                raise ValueError('Missing regular BatchEvalPython execution')
            for m in python_nodes[0].get('metrics', []):
                ident = str(m['accumulatorId'])
                values = [s['updates'][ident] for s in selected if ident in s['updates']]
                if values:
                    metrics[m['name']] = dict(accumulator_id=ident, metric_type=m.get('metricType'), total=sum(values))
            if any(name not in metrics for name in REQUIRED_METRICS):
                raise ValueError('Missing executed Python metrics')
            if (metrics[REQUIRED_METRICS[0]]['total'] <= 0 or metrics[REQUIRED_METRICS[1]]['total'] <= 0 or
                metrics[REQUIRED_METRICS[2]]['total'] != report['tables']['silver/impressions']['rows']):
                raise ValueError('Contradictory Python bytes or output rows')
        if not silver and (len(joins) != 2 or {j['key'] for j in joins} != {'user_id', 'adgroup_id'} or
                           any('LeftOuter' not in j['description'] for j in joins)):
            raise ValueError('Unexpected Gold logical joins')
        result[group] = dict(execution_id=execution, python_operators=[n['nodeName'] for n in python_nodes],
            python_metrics=metrics, joins=joins,
            stages=[{k: v for k, v in s.items() if k not in ('updates', 'task_ids')} for s in selected])
        if plans_dir:
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / f'{group}.txt').write_text(event['physicalPlanDescription'].replace(str(REPO), '<repo>'))
    if set(result) != set(GROUPS):
        raise ValueError('Missing Silver or Gold data execution evidence')
    return dict(executions=result)


def inspect_comparison(root):
    comparison = json.loads((root / 'comparison.json').read_text())
    result = dict(status='running', runs={}, method='Successful data SQL executions, completed stages and task accumulator updates; no extra data actions. JVM task CPU/memory exclude complete Python worker usage. Worker timings are cumulative, not wall-clock.')
    try:
        if comparison['status'] != 'succeeded' or comparison['declared_setting_changes']:
            raise ValueError('Successful comparison with no setting changes required')
        reference = comparison['runs'][0]['report']
        for run in comparison['runs']:
            report = run['report']; validate_reports(reference, report)
            evidence = inspect_run(root / 'runs' / run['run_id'] / 'events', report,
                run['side'] == 'right', root / 'python-udf-plans' / run['run_id'])
            evidence['combined_gold_seconds'] = round(sum(v for k, v in report['seconds'].items() if k.startswith('gold.')), 3)
            result['runs'][run['run_id']] = evidence
        result['combined_gold_seconds'] = {side: distribution([
            result['runs'][r['run_id']]['combined_gold_seconds'] for r in comparison['runs'] if r['side'] == side]) for side in ('left', 'right')}
        result['status'] = 'succeeded'
    except BaseException as error:
        result.update(status='failed', error=str(error)); raise
    finally:
        (root / 'python-udf-evidence.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print('Python date execution and downstream Gold evidence verified', flush=True)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison_dir', type=Path)
    inspect_comparison(parser.parse_args().comparison_dir.resolve())
