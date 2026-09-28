"""Join hints must preserve rows and actually change only the intended join."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmarks.equality import compare_frames
from benchmarks.join_strategies import inspect_run
from benchmarks.reporting import validate_reports
from test_baseline import write_fixture, create_spark

ROOT = Path(__file__).resolve().parents[1]


class JoinStrategyTests(unittest.TestCase):
    def test_real_entrypoints_preserve_rows_and_change_only_target_strategy(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); write_fixture(root)
            reports = {}
            for name, ads, profiles in [
                ('02_compression', 'BroadcastHashJoin', 'BroadcastHashJoin'),
                ('04_ads_shuffle_join', 'SortMergeJoin', 'BroadcastHashJoin'),
                ('05_profile_shuffle_join', 'BroadcastHashJoin', 'SortMergeJoin')]:
                with (root / f'{name}.log').open('w') as log:
                    result = subprocess.run([sys.executable, str(ROOT / 'experiments' / name / 'run.py'),
                        '--source-dir', str(root), '--output-dir', str(root / 'runs'), '--run-id', name],
                        cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.assertEqual(result.returncode, 0, (root / f'{name}.log').read_text()[-8000:])
                run = root / 'runs' / name
                reports[name] = json.loads((run / 'report.json').read_text())
                proof = inspect_run(run / 'events', ads, profile_operator=profiles)
                self.assertEqual(len(proof), 2)
                self.assertTrue(all(p['matches_expected'] for p in proof.values()))
            for name in ('04_ads_shuffle_join', '05_profile_shuffle_join'):
                self.assertEqual(validate_reports(reports['02_compression'], reports[name]), {})
            with self.assertRaisesRegex(ValueError, 'expected'):
                inspect_run(root / 'runs/04_ads_shuffle_join/events', 'BroadcastHashJoin')
            with self.assertRaisesRegex(ValueError, 'expected'):
                inspect_run(root / 'runs/05_profile_shuffle_join/events', 'SortMergeJoin', profile_operator='SortMergeJoin')
            spark = create_spark(root / 'verification-events')
            try:
                for table in reports['02_compression']['tables']:
                    with self.subTest(table=table):
                        a = spark.read.format('delta').load(str(root / 'runs/02_compression' / table))
                        for name in ('04_ads_shuffle_join', '05_profile_shuffle_join'):
                            b = spark.read.format('delta').load(str(root / 'runs' / name / table))
                            self.assertTrue(compare_frames(a, b)['equal'])
            finally:
                spark.stop()

    def test_missing_runtime_evidence_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, 'Missing Gold'):
                inspect_run(Path(folder), 'SortMergeJoin')

    def test_latest_plan_detects_an_unintended_profile_join_change(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            def plan(ads, users):
                return {'nodeName': 'root', 'children': [
                    {'nodeName': ads, 'simpleString': f'{ads} [adgroup_id#1L], [adgroup_id#2L], LeftOuter'},
                    {'nodeName': users, 'simpleString': f'{users} [user_id#3L], [user_id#4L], LeftOuter'}]}
            events = [
                {'Event': 'SparkListenerStageSubmitted', 'Properties': {
                    'spark.jobGroup.id': 'gold.campaign_daily', 'spark.sql.execution.id': '1'}},
                {'executionId': 1, 'sparkPlanInfo': plan('SortMergeJoin', 'BroadcastHashJoin')},
                {'executionId': 1, 'sparkPlanInfo': plan('SortMergeJoin', 'SortMergeJoin')},
                {'Event': 'SparkListenerSQLExecutionEnd', 'executionId': 1},
            ]
            (root / 'events').write_text('\n'.join(map(json.dumps, events)))
            with self.assertRaisesRegex(ValueError, 'expected'):
                inspect_run(root, 'SortMergeJoin')
