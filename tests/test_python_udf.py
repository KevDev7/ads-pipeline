"""Date semantics, real entrypoints, observer neutrality and evidence rejection."""
import copy
from datetime import datetime, date
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from benchmarks.equality import compare_frames
from benchmarks.experiment import commands
from benchmarks.python_udf import ARROW_KEY, inspect_run
from benchmarks.reporting import validate_reports
from test_baseline import write_fixture, create_spark

ROOT = Path(__file__).resolve().parents[1]


class PythonUdfTests(unittest.TestCase):
    def test_preset(self):
        steps = commands('9', Path('data'), Path('out'), 'test', 3)
        self.assertEqual([name for name, _ in steps], ['pipeline', 'python-udf', 'codecs'])
        self.assertIn('experiments/02_compression/run.py', steps[0][1])
        self.assertIn('experiments/09_python_date_udf/run.py', steps[0][1])
        self.assertIn(ARROW_KEY, steps[0][1])
        self.assertNotIn('--changed-setting', steps[0][1])

    def test_real_entrypoints_dates_and_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); write_fixture(root); reports = {}
            for name, version, observed in [('direct', '02_compression', False),
                                           ('control', '02_compression', True),
                                           ('variant', '09_python_date_udf', False)]:
                entry = ROOT / 'experiments' / version / 'run.py'
                command = ([sys.executable, '-m', 'benchmarks.observed_run', '--entry', str(entry),
                            '--only-extra-settings', '--extra-setting', ARROW_KEY]
                           if observed else [sys.executable, str(entry)])
                with (root / f'{name}.log').open('w') as log:
                    p = subprocess.run(command + ['--source-dir', str(root), '--output-dir', str(root / 'runs'),
                                                  '--run-id', name], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.assertEqual(p.returncode, 0, (root / f'{name}.log').read_text()[-10000:])
                reports[name] = json.loads((root / 'runs' / name / 'report.json').read_text())
            observed = copy.deepcopy(reports['control'])
            observed['environment']['spark_sql_settings'].pop(ARROW_KEY)
            self.assertEqual(validate_reports(reports['direct'], observed), {})
            self.assertEqual(validate_reports(reports['control'], reports['variant']), {})
            control = inspect_run(root / 'runs/control/events', reports['control'], False)
            variant = inspect_run(root / 'runs/variant/events', reports['variant'], True)
            execution = variant['executions']['silver.impressions']
            self.assertEqual(execution['python_metrics']['number of output rows']['total'], 5)
            self.assertEqual(control['executions']['silver.impressions']['python_operators'], [])
            events = []
            for path in sorted((root / 'runs/variant/events').rglob('*')):
                if path.is_file() and not path.name.startswith(('.', 'appstatus')):
                    events.extend(json.loads(line) for line in path.open())
            damaged = root / 'damaged'; damaged.mkdir()
            metric = execution['python_metrics']['number of output rows']['accumulator_id']
            for mutation, message in [('sql', 'did not succeed'), ('plan', 'regular BatchEvalPython'),
                                      ('metric', 'Contradictory Python'), ('missing_metric', 'Missing executed Python'),
                                      ('stage', 'stage/task'), ('missing_plan', 'Missing executed SQL')]:
                with self.subTest(mutation=mutation):
                    changed = copy.deepcopy(events)
                    for e in changed:
                        if mutation == 'sql' and e.get('Event', '').endswith('SparkListenerSQLExecutionEnd') and str(e['executionId']) == execution['execution_id']:
                            e['errorMessage'] = 'injected failure'
                        if mutation == 'plan' and 'sparkPlanInfo' in e and str(e['executionId']) == execution['execution_id']:
                            from benchmarks.python_udf import nodes
                            for n in nodes(e['sparkPlanInfo']):
                                if n['nodeName'] == 'BatchEvalPython': n['nodeName'] = 'ArrowEvalPython'
                        if mutation in ('metric', 'missing_metric') and e.get('Event') == 'SparkListenerTaskEnd':
                            acc = e['Task Info'].get('Accumulables', [])
                            if mutation == 'missing_metric':
                                e['Task Info']['Accumulables'] = [a for a in acc if str(a['ID']) != metric]
                            else:
                                for a in acc:
                                    if str(a['ID']) == metric: a['Update'] = '0'
                    if mutation == 'stage':
                        stage = execution['stages'][0]['stage_id']
                        changed = [e for e in changed if not (e.get('Event') == 'SparkListenerTaskEnd' and e['Stage ID'] == stage)]
                    if mutation == 'missing_plan':
                        changed = [e for e in changed if not ('sparkPlanInfo' in e and str(e['executionId']) == execution['execution_id'])]
                    (damaged / 'events').write_text('\n'.join(map(json.dumps, changed)))
                    with self.assertRaisesRegex(ValueError, message): inspect_run(damaged, reports['variant'], True)
            bad = copy.deepcopy(reports['variant']); bad['environment']['spark_sql_settings'].pop(ARROW_KEY)
            with self.assertRaisesRegex(ValueError, 'Missing effective'): inspect_run(root, bad, True)
            bad['environment']['spark_sql_settings'][ARROW_KEY] = 'false'
            bad['environment']['spark_sql_settings']['spark.sql.session.timeZone'] = 'UTC'
            with self.assertRaisesRegex(ValueError, 'Contradictory setting'): inspect_run(root, bad, True)
            with self.assertRaisesRegex(ValueError, 'Unexpected Python'):
                inspect_run(root / 'runs/variant/events', reports['variant'], False)
            spark = create_spark(root / 'verification-events')
            try:
                for candidate in ('control', 'variant'):
                    for table in reports['direct']['tables']:
                        with self.subTest(candidate=candidate, table=table):
                            a = spark.read.format('delta').load(str(root / 'runs/direct' / table))
                            b = spark.read.format('delta').load(str(root / 'runs' / candidate / table))
                            self.assertTrue(compare_frames(a, b)['equal'])
                self.check_date_semantics(spark)
                print('Iteration 9 fixture evidence: ' + json.dumps(dict(
                    exact_table_comparisons=16, settings_identical=True,
                    python_metrics=execution['python_metrics'],
                    gold_python_operators={k: v['python_operators'] for k, v in variant['executions'].items() if k.startswith('gold.')})), flush=True)
            finally:
                spark.stop()

    def check_date_semantics(self, spark):
        from pyspark.sql import functions as F, types as T
        from pyspark.util import PythonEvalType
        spec = importlib.util.spec_from_file_location('date_udf_under_test', ROOT / 'experiments/09_python_date_udf/tables.py')
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        self.assertEqual(module.reporting_date_udf.evalType, PythonEvalType.SQL_BATCHED_UDF)
        cases = [('2017-05-05T15:59:59+00:00', date(2017,5,5)),
                 ('2017-05-05T16:00:00+00:00', date(2017,5,6)),
                 ('2020-02-28T16:00:00+00:00', date(2020,2,29)),
                 ('1969-12-31T15:59:59+00:00', date(1969,12,31))]
        data = [(int(datetime.fromisoformat(text).timestamp()), expected) for text, expected in cases] + [(None, None)]
        before = os.environ.get('TZ')
        try:
            for zone in ('UTC', 'America/Los_Angeles'):
                os.environ['TZ'] = zone; time.tzset()
                for seconds, expected in data: self.assertEqual(module.reporting_date_python(seconds), expected)
            frame = spark.createDataFrame(data, T.StructType([T.StructField('seconds', T.LongType()), T.StructField('expected', T.DateType())]))
            rows = frame.select('expected', module.reporting_date_udf('seconds').alias('python'),
                                F.to_date(F.timestamp_seconds('seconds')).alias('builtin')).collect()
            for row in rows:
                self.assertEqual(row['python'], row['expected']); self.assertEqual(row['builtin'], row['expected'])
        finally:
            if before is None: os.environ.pop('TZ', None)
            else: os.environ['TZ'] = before
            time.tzset()
