"""Verify completed Gold join strategies from Spark runtime event logs."""
import argparse
import json
from pathlib import Path
import re

REPO = Path(__file__).resolve().parents[1]
GOLD = ('gold.campaign_daily', 'gold.audience_daily')


def join_nodes(plan):
    result = []
    def walk(node):
        name = node['nodeName']
        if name in ('BroadcastHashJoin', 'SortMergeJoin', 'ShuffledHashJoin', 'BroadcastNestedLoopJoin'):
            text = node['simpleString']
            keys = re.findall(r'\[([a-z_]+)#', text)
            result.append({'operator': name, 'key': keys[0] if keys else None,
                           'description': text})
        for child in node.get('children', []):
            walk(child)
    walk(plan)
    return result


def inspect_run(event_dir, ads_operator, plans_dir=None, profile_operator='BroadcastHashJoin'):
    plans, groups, ended = {}, {}, set()
    for path in sorted(event_dir.rglob('*')):
        if not path.is_file() or path.name.startswith(('.', 'appstatus')):
            continue
        with path.open() as handle:
            for line in handle:
                event = json.loads(line)
                kind = event.get('Event', '')
                if 'sparkPlanInfo' in event:
                    plans[str(event['executionId'])] = event
                if kind.endswith('SparkListenerSQLExecutionEnd'):
                    ended.add(str(event['executionId']))
                if kind == 'SparkListenerStageSubmitted':
                    props = event.get('Properties') or {}
                    group = props.get('spark.jobGroup.id')
                    if group in GOLD:
                        groups[str(props['spark.sql.execution.id'])] = group
    result = {}
    for execution, group in groups.items():
        if execution not in plans:
            continue
        event = plans[execution]
        joins = join_nodes(event['sparkPlanInfo'])
        if not joins:
            continue  # Delta metadata queries are also tagged with the job group.
        if group in result:
            raise ValueError(f'Ambiguous joined executions for {group}')
        if execution not in ended:
            raise ValueError(f'Joined SQL execution has not completed: {group}')
        expected = {'adgroup_id': ads_operator, 'user_id': profile_operator}
        actual = {node['key']: node['operator'] for node in joins}
        if len(joins) != 2 or actual != expected:
            raise ValueError(f'{group}: expected {expected}, found {joins}')
        if not all('LeftOuter' in node['description'] for node in joins):
            raise ValueError(f'{group}: join type changed')
        result[group] = {'execution_id': int(execution), 'execution_completed': True,
                         'joins': joins, 'matches_expected': True}
        if plans_dir:
            plans_dir.mkdir(parents=True, exist_ok=True)
            text = event['physicalPlanDescription'].replace(str(REPO), '<repo>')
            (plans_dir / f'{group}.txt').write_text(text)
    if set(result) != set(GOLD):
        raise ValueError(f'Missing Gold join evidence: {set(GOLD) - set(result)}')
    return result


def inspect_comparison(root, target='ads'):
    if target not in ('ads', 'profiles'):
        raise ValueError(f'Unknown join target: {target}')
    comparison = json.loads((root / 'comparison.json').read_text())
    if comparison['status'] != 'succeeded':
        raise ValueError('A successful pipeline comparison is required')
    result = {'status': 'running', 'runs': {}, 'target': target,
              'method': f'Latest structured runtime plan for completed Gold SQL executions. Join keys identify ads versus profiles. Control must broadcast both; variant must sort-merge {target} and broadcast the other dimension. Hints alone are not proof.'}
    try:
        for run in comparison['runs']:
            ads = 'SortMergeJoin' if run['side'] == 'right' and target == 'ads' else 'BroadcastHashJoin'
            profiles = 'SortMergeJoin' if run['side'] == 'right' and target == 'profiles' else 'BroadcastHashJoin'
            result['runs'][run['run_id']] = inspect_run(
                root / 'runs' / run['run_id'] / 'events', ads,
                root / 'join-plans' / run['run_id'], profile_operator=profiles)
        result['status'] = 'succeeded'
    except BaseException as error:
        result['status'] = 'failed'; result['error'] = str(error)
        raise
    finally:
        (root / 'join-strategies.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print('All executed Gold join strategies verified', flush=True)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison_dir', type=Path)
    parser.add_argument('--target', choices=('ads', 'profiles'), default='ads')
    args = parser.parse_args()
    inspect_comparison(args.comparison_dir.resolve(), args.target)
