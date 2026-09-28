"""Verify orchestration, failure reporting, and preservation of earlier evidence."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.experiment import commands, run


class ExperimentRunnerTests(unittest.TestCase):
    def test_presets_choose_reference_and_only_applicable_checks(self):
        formats = commands('1', Path('data'), Path('out'), 'test', 3)
        self.assertEqual([name for name, _ in formats], ['formats'])
        self.assertIn('--repeats', formats[0][1])
        compression = commands('2', Path('data'), Path('out'), 'test', 3)
        joins = commands('4', Path('data'), Path('out'), 'test', 3)
        self.assertEqual([name for name, _ in joins], ['pipeline', 'joins', 'codecs'])
        self.assertIn('experiments/02_compression/run.py', joins[0][1])
        self.assertNotIn('--changed-setting', joins[0][1])
        self.assertEqual(joins[1][1][-2:], ['--target', 'ads'])
        profiles = commands('5', Path('data'), Path('out'), 'test', 3)
        self.assertEqual([name for name, _ in profiles], ['pipeline', 'joins', 'codecs'])
        self.assertIn('experiments/02_compression/run.py', profiles[0][1])
        self.assertIn('experiments/05_profile_shuffle_join/run.py', profiles[0][1])
        self.assertNotIn('--changed-setting', profiles[0][1])
        self.assertEqual(profiles[1][1][-2:], ['--target', 'profiles'])
        shuffle = commands('6', Path('data'), Path('out'), 'test', 3)
        self.assertEqual([name for name, _ in shuffle], ['pipeline', 'shuffle', 'codecs'])
        self.assertIn('experiments/02_compression/run.py', shuffle[0][1])
        self.assertIn('experiments/06_shuffle_partitions/run.py', shuffle[0][1])
        self.assertIn('spark.sql.shuffle.partitions', shuffle[0][1])
        cache = commands('7', Path('data'), Path('out'), 'test', 3)
        self.assertEqual([name for name, _ in cache], ['pipeline', 'cache', 'codecs'])
        self.assertIn('experiments/02_compression/run.py', cache[0][1])
        self.assertIn('experiments/07_cached_enrichment/run.py', cache[0][1])
        self.assertNotIn('--changed-setting', cache[0][1])
        partitioning = commands('3', Path('data'), Path('out'), 'test', 3)
        self.assertEqual([name for name, _ in compression], ['pipeline', 'codecs'])
        self.assertEqual([name for name, _ in partitioning], ['pipeline', 'queries', 'codecs'])
        self.assertIn('experiments/00_baseline/run.py', compression[0][1])
        self.assertIn('spark.sql.parquet.compression.codec', compression[0][1])
        self.assertIn('experiments/02_compression/run.py', partitioning[0][1])
        self.assertNotIn('--changed-setting', partitioning[0][1])

    def test_failure_stops_later_steps_and_keeps_failed_report(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            root = output / 'trial'
            def execute(command, **kwargs):
                if command[2] == 'benchmarks.compare':
                    root.mkdir()
                    (root / 'comparison.json').write_text('{"status":"succeeded"}')
                else:
                    raise subprocess.CalledProcessError(1, command)
            with patch('benchmarks.experiment.subprocess.run', side_effect=execute) as process:
                with self.assertRaises(subprocess.CalledProcessError):
                    run('3', output, output, 'trial', 1)
                self.assertEqual(process.call_count, 2)
            report = json.loads((root / 'suite.json').read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertEqual(report['steps']['pipeline']['status'], 'succeeded')
            self.assertEqual(report['steps']['queries']['status'], 'failed')
            self.assertEqual(report['steps']['codecs']['status'], 'pending')
            self.assertIn('incomplete', (root / 'RESULTS.md').read_text())
            before = (root / 'suite.json').read_bytes()
            with self.assertRaises(FileExistsError):
                run('3', output, output, 'trial', 1)
            self.assertEqual((root / 'suite.json').read_bytes(), before)

    def test_success_requires_successful_artifacts(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            root = output / 'trial'
            def execute(command, **kwargs):
                if command[2] == 'benchmarks.compare':
                    root.mkdir()
                    (root / 'comparison.json').write_text('{"status":"failed"}')
            with patch('benchmarks.experiment.subprocess.run', side_effect=execute) as process:
                with self.assertRaisesRegex(ValueError, 'did not report success'):
                    run('2', output, output, 'trial', 1)
                self.assertEqual(process.call_count, 1)
            self.assertEqual(json.loads((root / 'suite.json').read_text())['status'], 'failed')
