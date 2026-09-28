"""Real entrypoints, exact data, failure cleanup, and contradictory evidence."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from benchmarks.cache_evidence import inspect_run, lifecycle
from benchmarks.equality import compare_frames
from benchmarks.reporting import validate_reports
from test_baseline import write_fixture, create_spark

ROOT = Path(__file__).resolve().parents[1]


class CacheTests(unittest.TestCase):
    def test_entrypoints_and_cache_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); write_fixture(root); reports = {}
            for name in ('02_compression', '07_cached_enrichment'):
                with (root / f'{name}.log').open('w') as log:
                    process = subprocess.run([sys.executable, str(ROOT / 'experiments' / name / 'run.py'),
                        '--source-dir', str(root), '--output-dir', str(root / 'runs'), '--run-id', name],
                        cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.assertEqual(process.returncode, 0, (root / f'{name}.log').read_text()[-8000:])
                run = root / 'runs' / name
                reports[name] = json.loads((run / 'report.json').read_text())
                proof = inspect_run(run / 'events', reports[name], name.startswith('07'))
            self.assertEqual(validate_reports(reports['02_compression'], reports['07_cached_enrichment'], []), {})
            report = reports['07_cached_enrichment']
            self.assertIn('gold.cache_release', report['seconds'])
            for point in ('after_campaign', 'before_audience', 'after_audience', 'after_release'):
                bad = copy.deepcopy(report); del bad['cache']['snapshots'][point]
                with self.assertRaises(ValueError): lifecycle(bad)
            bad = copy.deepcopy(report); bad['cache']['snapshots']['before_audience']['rdd_id'] += 1
            with self.assertRaises(ValueError): lifecycle(bad)
            # Valid live storage alone cannot establish execution or reuse.
            with self.assertRaisesRegex(ValueError, 'Missing Gold'):
                inspect_run(root / 'absent-events', report)
            # Contradict storage snapshots with evidence of recomputation in audience.
            target_stage = proof['executions']['gold.audience_daily']['stages'][0]['stage_id']
            damaged = root / 'contradictory-events'; damaged.mkdir()
            events = []
            for path in sorted((root / 'runs/07_cached_enrichment/events').rglob('*')):
                if not path.is_file() or path.name.startswith(('.', 'appstatus')): continue
                with path.open() as handle:
                    for line in handle: events.append(json.loads(line))
            campaign = proof['executions']['gold.campaign_daily']['execution_id']
            source_ids = set()
            def collect(node):
                if node.get('nodeName', '').startswith('Scan parquet'):
                    source_ids.update(m['accumulatorId'] for m in node.get('metrics', [])
                                      if m['name'] == 'number of output rows')
                for child in node.get('children', []): collect(child)
            for event in events:
                if 'sparkPlanInfo' in event and str(event['executionId']) == campaign:
                    collect(event['sparkPlanInfo'])
            for event in events:
                if event.get('Event') == 'SparkListenerTaskEnd' and event['Stage ID'] == target_stage:
                    event['Task Info']['Accumulables'].append(dict(ID=next(iter(source_ids)),
                        Metadata='sql', Update='1'))
            (damaged / 'events').write_text('\n'.join(map(json.dumps, events)))
            with self.assertRaisesRegex(ValueError, 'recomputed'):
                inspect_run(damaged, report)
            spark = create_spark(root / 'verification-events')
            try:
                for table in reports['02_compression']['tables']:
                    with self.subTest(table=table):
                        a = spark.read.format('delta').load(str(root / 'runs/02_compression' / table))
                        b = spark.read.format('delta').load(str(root / 'runs/07_cached_enrichment' / table))
                        self.assertTrue(compare_frames(a, b)['equal'])
            finally:
                spark.stop()

    def test_failure_cleanup_and_original_error(self):
        # Separate processes prevent baseline module imports leaking into the variant.
        for cleanup_error in (False, True):
            with self.subTest(cleanup_error=cleanup_error), tempfile.TemporaryDirectory() as folder:
                root = Path(folder); write_fixture(root)
                code = '''
import sys
from pathlib import Path
sys.path.insert(0, str(Path('experiments/07_cached_enrichment').resolve()))
import run
original_release = run.CachedEnrichment.release
def fail_between_reports(*args, **kwargs):
    raise RuntimeError('injected between reports')
def release(self):
    original_release(self)
    if sys.argv[2] == 'True':
        raise RuntimeError('injected cleanup failure')
run.valid_gold = fail_between_reports
run.CachedEnrichment.release = release
try:
    run.run(Path(sys.argv[1]), Path(sys.argv[1]) / 'runs', 'failed')
except RuntimeError as error:
    assert str(error) == 'injected between reports', str(error)
else:
    raise AssertionError('failure not raised')
'''
                with (root / 'failure.log').open('w') as log:
                    p = subprocess.run([sys.executable, '-c', code, str(root), str(cleanup_error)],
                                       cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.assertEqual(p.returncode, 0, (root / 'failure.log').read_text()[-8000:])
                report = json.loads((root / 'runs/failed/report.json').read_text())
                self.assertEqual(report['status'], 'failed')
                self.assertIn('injected between reports', report['error'])
                self.assertNotIn('gold.audience_daily', report['seconds'])
                self.assertIn('gold.cache_release', report['seconds'])
                if cleanup_error:
                    self.assertEqual(report['cache_cleanup_error'], 'injected cleanup failure')
                else:
                    self.assertFalse(report['cache']['snapshots']['after_release']['registered'])
                    self.assertFalse(report['cache']['snapshots']['after_release']['storage_info_present'])

    def test_missing_storage_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'Missing requested'):
            lifecycle({})
