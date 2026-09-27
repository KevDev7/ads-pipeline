import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.compare import run_comparison
from benchmarks.equality import compare_frames
from benchmarks.reporting import distribution, summarize, validate_reports

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'experiments/00_baseline'))
from session import create_spark


def report():
    return json.loads((ROOT / 'results/00_baseline/baseline-full-20260927.json').read_text())


class ComparisonRulesTests(unittest.TestCase):
    def test_same_sources_environment_and_tables_required(self):
        reference = report()
        validate_reports(reference, copy.deepcopy(reference))
        for key, value, message in [
            ('status', 'failed', 'incomplete or failed'),
            ('sources', {}, 'fingerprints'),
            ('tables', {}, 'table sets'),
        ]:
            other = copy.deepcopy(reference)
            other[key] = value
            with self.assertRaisesRegex(ValueError, message):
                validate_reports(reference, other)
        other = copy.deepcopy(reference)
        other['environment']['driver_memory'] = '4g'
        with self.assertRaisesRegex(ValueError, 'driver_memory'):
            validate_reports(reference, other)

    def test_only_declared_settings_may_differ(self):
        reference, other = report(), report()
        key = 'spark.sql.parquet.compression.codec'
        other['environment']['spark_sql_settings'][key] = 'zstd'
        with self.assertRaisesRegex(ValueError, 'Undeclared'):
            validate_reports(reference, other)
        self.assertEqual(validate_reports(reference, other, [key])[key]['current'], 'zstd')

    def test_statistics_keep_outliers_and_zero_denominators(self):
        self.assertEqual(distribution([1, 2, 99])['median'], 2)
        self.assertEqual(distribution([1, 2, 99])['max'], 99)
        runs = []
        for side, seconds in [('left', 10), ('right', 8), ('left', 20), ('right', 10)]:
            item = report()
            item['seconds']['pipeline_writes'] = seconds
            runs.append({'side': side, 'report': item})
        summary = summarize(runs)
        self.assertEqual(summary['seconds.pipeline_writes']['left']['samples'], [10, 20])
        self.assertEqual(summary['seconds.pipeline_writes']['right_vs_left_percent'], -40)
        self.assertIsNone(summary['processing.disk_spill_bytes']['right_vs_left_percent'])

    def test_runner_tests_first_alternates_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            observed = []

            def fake_command(argv, log):
                observed.append(argv)
                log.write_text('mock subprocess success\n')
                if '--run-id' in argv:
                    run_id = argv[argv.index('--run-id') + 1]
                    output = Path(argv[argv.index('--output-dir') + 1]) / run_id
                    output.mkdir(parents=True)
                    (output / 'report.json').write_text(json.dumps(report()))

            with patch('benchmarks.compare.command', side_effect=fake_command), patch('benchmarks.compare.verify_outputs') as verify:
                result = run_comparison(ROOT / 'experiments/00_baseline/run.py',
                                        ROOT / 'experiments/00_baseline/run.py', root,
                                        root / 'out', 'check', 2, 'A/A', 'full pipeline')
                verify.assert_called_once()
                with self.assertRaises(FileExistsError):
                    run_comparison(ROOT / 'experiments/00_baseline/run.py',
                                   ROOT / 'experiments/00_baseline/run.py', root,
                                   root / 'out', 'check', 2, 'A/A', 'full pipeline')
            self.assertIn('unittest', observed[0])
            self.assertEqual([r['run_id'] for r in result['runs']], ['01-left', '01-right', '02-right', '02-left'])
            self.assertEqual(result['status'], 'succeeded')
            self.assertEqual(len(result['summary']['seconds.total']['left']['samples']), 2)

    def test_subprocess_failure_is_persisted_without_summary(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with patch('benchmarks.compare.command', side_effect=subprocess.CalledProcessError(1, 'tests')):
                with self.assertRaises(subprocess.CalledProcessError):
                    run_comparison(ROOT / 'experiments/00_baseline/run.py',
                                   ROOT / 'experiments/00_baseline/run.py', root,
                                   root / 'out', 'failure', 1, 'A/A', 'full pipeline')
            result = json.loads((root / 'out/failure/comparison.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertNotIn('summary', result)


class ExactEqualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.spark = create_spark(Path(cls.temp.name) / 'events')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_nulls_duplicates_and_reordered_rows_columns(self):
        left = self.spark.createDataFrame([(1, None), (1, None), (2, 'x')], 'id long, value string')
        right = self.spark.createDataFrame([(2, 'x'), (1, None), (1, None)], 'id long, value string').select('value', 'id')
        self.assertTrue(compare_frames(left, right)['equal'])
        changed = self.spark.createDataFrame([(1, None), (2, 'x'), (2, 'x')], 'id long, value string')
        self.assertFalse(compare_frames(left, changed)['equal'])

    def test_matching_totals_do_not_hide_wrong_group_results(self):
        left = self.spark.createDataFrame([('a', 10, 1), ('b', 10, 0)], 'campaign string, impressions long, clicks long')
        right = self.spark.createDataFrame([('a', 10, 0), ('b', 10, 1)], left.schema)
        self.assertFalse(compare_frames(left, right)['equal'])

    def test_schema_changes_and_empty_tables(self):
        left = self.spark.createDataFrame([], 'id long, value string')
        right = self.spark.createDataFrame([], 'value string, id long')
        self.assertTrue(compare_frames(left, right)['equal'])
        other = self.spark.createDataFrame([], 'id string, value string')
        self.assertEqual(compare_frames(left, other)['reason'], 'schema_mismatch')
        self.assertFalse(compare_frames(left, self.spark.createDataFrame([(1, None)], left.schema))['equal'])
