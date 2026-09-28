"""Verify cache lifecycle with metadata snapshots and execution-scoped task metrics."""
import argparse
import json
from pathlib import Path
from benchmarks.join_strategies import GOLD, REPO, join_nodes
from benchmarks.reporting import distribution, validate_reports


def lifecycle(report):
    cache = report.get('cache', {})
    if cache.get('requested_storage_level') != 'MEMORY_AND_DISK_DESER':
        raise ValueError('Missing requested cache storage level')
    snapshots = cache.get('snapshots', {})
    identities = set()
    for name in ('after_campaign', 'before_audience', 'after_audience'):
        s = snapshots.get(name, {})
        if (not s.get('registered') or not s.get('storage_info_present') or
            s.get('storage_level') != dict(use_memory=True, use_disk=True, deserialized=True, replication=1) or
            s.get('total_partitions', 0) <= 0 or s.get('cached_partitions') != s.get('total_partitions') or
            s.get('memory_bytes', 0) + s.get('disk_bytes', 0) <= 0 or s.get('rdd_id') is None):
            raise ValueError(f'Missing or contradictory cache materialization: {name}')
        identities.add(s['rdd_id'])
        if s['total_partitions'] != snapshots['after_campaign']['total_partitions']:
            raise ValueError('Contradictory cache partition counts')
    s = snapshots.get('after_release', {})
    if (len(identities) != 1 or s.get('rdd_id') not in identities or s.get('registered') is not False or
        s.get('storage_info_present') is not False or any(s.get(k) != 0 for k in
        ('cached_partitions', 'memory_bytes', 'disk_bytes'))):
        raise ValueError('Missing or contradictory cache release/identity')
    return next(iter(identities))


def inspect_run(event_dir, report, cached=True, plans_dir=None):
    rdd_id = lifecycle(report) if cached else None
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
                    if props.get('spark.jobGroup.id') in GOLD:
                        stages[(info['Stage ID'], info['Stage Attempt ID'])] = dict(
                            stage_id=info['Stage ID'], execution=str(props['spark.sql.execution.id']),
                            group=props['spark.jobGroup.id'], expected=info['Number of Tasks'], tasks=0,
                            failed=0, completed=False, updates={},
                            rdd_ids=[r['RDD ID'] for r in info.get('RDD Info', [])])
                if kind == 'SparkListenerStageCompleted':
                    info = e['Stage Info']; key = (info['Stage ID'], info['Stage Attempt ID'])
                    if key in stages:
                        stages[key]['completed'] = not bool(info.get('Failure Reason'))
                if kind == 'SparkListenerTaskEnd':
                    key = (e['Stage ID'], e['Stage Attempt ID'])
                    if key in stages:
                        s = stages[key]; s['tasks'] += 1
                        s['failed'] += int(e.get('Task End Reason', {}).get('Reason') != 'Success')
                        for a in e['Task Info'].get('Accumulables', []):
                            if a.get('Metadata') == 'sql' and str(a.get('Update', '')).isdigit():
                                key = str(a['ID']); s['updates'][key] = s['updates'].get(key, 0) + int(a['Update'])
    result = {}
    for execution, group in sorted({(s['execution'], s['group']) for s in stages.values()}):
        if execution not in plans:
            raise ValueError('Missing executed SQL plan')
        plan = plans[execution]; joins = join_nodes(plan['sparkPlanInfo'])
        if not joins:
            continue
        if group in result or execution not in ends or ends[execution].get('errorMessage'):
            raise ValueError(f'Ambiguous or unsuccessful Gold SQL: {group}')
        selected = [s for s in stages.values() if s['execution'] == execution]
        if not selected or any(not s['completed'] or s['failed'] or s['tasks'] != s['expected'] for s in selected):
            raise ValueError('Incomplete stage/task evidence')
        metrics = {'cache': [], 'join': [], 'source': []}
        def walk(n):
            name = n.get('nodeName', '')
            category = 'cache' if name == 'InMemoryTableScan' else 'join' if 'Join' in name else 'source' if name.startswith('Scan parquet') else None
            if category:
                metrics[category] += [str(m['accumulatorId']) for m in n.get('metrics', []) if m['name'] == 'number of output rows']
            for child in n.get('children', []):
                walk(child)
        walk(plan['sparkPlanInfo'])
        counts = {k: sum(s['updates'].get(m, 0) for s in selected for m in set(ids)) for k, ids in metrics.items()}
        audience = group == 'gold.audience_daily'
        if cached:
            if counts['cache'] != report['checks']['silver_totals']['impressions'] or not any(rdd_id in s['rdd_ids'] for s in selected):
                raise ValueError(f'Missing executed cache scan: {group}')
            if not metrics['join'] or not metrics['source']:
                raise ValueError('Missing cache-building lineage metrics')
            if audience and (counts['join'] or counts['source']):
                raise ValueError('Audience recomputed cached joins or source scans')
            if not audience and (counts['join'] <= 0 or counts['source'] <= 0):
                raise ValueError('Missing first-report cache-building work')
        elif counts['cache'] or counts['join'] <= 0 or counts['source'] <= 0:
            raise ValueError('Unexpected control execution evidence')
        result[group] = dict(execution_id=execution, joins=joins, task_output_rows=counts,
                             stages=[{k:v for k,v in s.items() if k != 'updates'} for s in selected])
        if plans_dir:
            plans_dir.mkdir(parents=True, exist_ok=True)
            (plans_dir / f'{group}.txt').write_text(plan['physicalPlanDescription'].replace(str(REPO), '<repo>'))
    if set(result) != set(GOLD):
        raise ValueError('Missing Gold execution evidence')
    return dict(snapshots=report.get('cache', {}).get('snapshots'), executions=result)


def inspect_comparison(root):
    comparison = json.loads((root / 'comparison.json').read_text())
    result = dict(status='running', runs={}, method='Live storage snapshots plus successful SQL executions and stage-scoped task accumulator updates. Cached lineage alone is not proof of reuse. Snapshot bytes are not peak process memory.')
    try:
        if comparison['status'] != 'succeeded':
            raise ValueError('Successful comparison required')
        reference = comparison['runs'][0]['report']
        for run in comparison['runs']:
            validate_reports(reference, run['report'], [])
            result['runs'][run['run_id']] = inspect_run(root / 'runs' / run['run_id'] / 'events',
                run['report'], run['side'] == 'right', root / 'cache-plans' / run['run_id'])
            result['runs'][run['run_id']]['combined_gold_seconds'] = round(sum(
                v for k, v in run['report']['seconds'].items() if k.startswith('gold.')), 3)
        result['combined_gold_seconds'] = {side: distribution([
            result['runs'][r['run_id']]['combined_gold_seconds'] for r in comparison['runs']
            if r['side'] == side]) for side in ('left', 'right')}
        result['status'] = 'succeeded'
    except BaseException as error:
        result.update(status='failed', error=str(error)); raise
    finally:
        (root / 'cache-evidence.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__); p.add_argument('comparison_dir', type=Path)
    inspect_comparison(p.parse_args().comparison_dir.resolve())
