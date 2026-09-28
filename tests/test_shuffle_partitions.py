"""Check the actual shuffle strategy and exact outputs, not only the setting."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmarks.equality import compare_frames
from benchmarks.reporting import validate_reports
from benchmarks.shuffle_partitions import inspect_run
from test_baseline import write_fixture, create_spark

ROOT = Path(__file__).resolve().parents[1]


class ShufflePartitionTests(unittest.TestCase):
    def test_real_entrypoints_change_only_initial_count_and_preserve_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); write_fixture(root)
            reports = {}
            for name, count in [('02_compression', 200), ('06_shuffle_partitions', 32)]:
                with (root / f'{name}.log').open('w') as log:
                    process = subprocess.run([sys.executable, str(ROOT / 'experiments' / name / 'run.py'),
                        '--source-dir', str(root), '--output-dir', str(root / 'runs'), '--run-id', name],
                        cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.assertEqual(process.returncode, 0, (root / f'{name}.log').read_text()[-8000:])
                run = root / 'runs' / name
                reports[name] = json.loads((run / 'report.json').read_text())
                proof = inspect_run(run / 'events', count)
                self.assertEqual(len(proof), 2)
                self.assertTrue(all(p['sql_succeeded'] for p in proof.values()))
                self.assertTrue(all(p['shuffle_reader_stages'] for p in proof.values()))
            key = 'spark.sql.shuffle.partitions'
            self.assertEqual(validate_reports(reports['02_compression'], reports['06_shuffle_partitions'], [key]),
                             {key: {'reference': '200', 'current': '32'}})
            with self.assertRaisesRegex(ValueError, 'expected initial partitions'):
                inspect_run(root / 'runs/06_shuffle_partitions/events', 200)
            spark = create_spark(root / 'verification-events')
            try:
                for table in reports['02_compression']['tables']:
                    with self.subTest(table=table):
                        a = spark.read.format('delta').load(str(root / 'runs/02_compression' / table))
                        b = spark.read.format('delta').load(str(root / 'runs/06_shuffle_partitions' / table))
                        self.assertTrue(compare_frames(a, b)['equal'])
            finally:
                spark.stop()

    def test_missing_evidence_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, 'Missing Gold evidence'):
                inspect_run(Path(folder), 32)

    def test_failed_sql_execution_is_rejected_even_if_it_ended(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            plan = {'nodeName': 'BroadcastHashJoin', 'simpleString': 'BroadcastHashJoin [user_id#1L], [user_id#2L], LeftOuter'}
            events = [
                {'Event': 'SparkListenerStageSubmitted', 'Stage Info': {
                    'Stage ID': 1, 'Stage Attempt ID': 0, 'Number of Tasks': 4},
                 'Properties': {'spark.jobGroup.id': 'gold.campaign_daily', 'spark.sql.execution.id': '1'}},
                {'executionId': 1, 'sparkPlanInfo': plan},
                {'Event': 'SparkListenerSQLExecutionEnd', 'executionId': 1, 'errorMessage': 'Memory failure'},
            ]
            (root / 'events').write_text('\n'.join(map(json.dumps, events)))
            with self.assertRaisesRegex(ValueError, 'did not succeed'):
                inspect_run(root, 32)
