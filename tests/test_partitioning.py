"""Exercise date partitioning, exact rows, and executed pruning on real Delta files."""
import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmarks.equality import compare_frames
from benchmarks.reporting import validate_reports
from test_baseline import write_fixture, create_spark

ROOT = Path(__file__).resolve().parents[1]


class PartitioningTests(unittest.TestCase):
    def test_layout_pruning_and_full_results(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_fixture(root)
            # Move the existing midnight/timezone fixture to the benchmark dates.
            raw = root / 'raw_sample.csv'
            with raw.open(newline='') as handle:
                rows = list(csv.reader(handle))
            for row in rows[1:]:
                row[1] = str(int(row[1]) + 2 * 86400)
            with raw.open('w', newline='') as handle:
                csv.writer(handle).writerows(rows)
            reports = {}
            query_reports = {}
            for name in ('02_compression', '03_date_partitioning'):
                with (root / f'{name}.log').open('w') as log:
                    process = subprocess.run([
                        sys.executable, str(ROOT / 'experiments' / name / 'run.py'),
                        '--source-dir', str(root), '--output-dir', str(root / 'runs'), '--run-id', name,
                    ], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.assertEqual(process.returncode, 0, (root / f'{name}.log').read_text()[-8000:])
                reports[name] = json.loads((root / 'runs' / name / 'report.json').read_text())
                with (root / f'{name}-queries.log').open('w') as log:
                    process = subprocess.run([
                        sys.executable, '-m', 'benchmarks.partition_queries',
                        '--table', str(root / 'runs' / name / 'silver/impressions'),
                        '--output', str(root / 'queries' / name),
                    ], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.assertEqual(process.returncode, 0, (root / f'{name}-queries.log').read_text()[-8000:])
                query_reports[name] = json.loads((root / 'queries' / name / 'report.json').read_text())
            self.assertEqual(validate_reports(reports['02_compression'], reports['03_date_partitioning']), {})
            a, b = query_reports['02_compression'], query_reports['03_date_partitioning']
            self.assertEqual(a['table']['partitionColumns'], [])
            self.assertEqual(b['table']['partitionColumns'], ['reporting_date'])
            for name in ('one_day', 'three_days', 'full_period'):
                self.assertEqual(a['queries'][name]['rows'], b['queries'][name]['rows'])
            self.assertEqual(sum(r['impressions'] for r in b['queries']['one_day']['rows']), 4)
            self.assertEqual(sum(r['clicks'] for r in b['queries']['one_day']['rows']), 2)
            one = sum(s['metrics']['numFiles'] for s in b['queries']['one_day']['scans'])
            full = sum(s['metrics']['numFiles'] for s in b['queries']['full_period']['scans'])
            self.assertGreater(one, 0)
            self.assertLess(one, full)
            spark = create_spark(root / 'verify-events')
            try:
                for table in reports['02_compression']['tables']:
                    with self.subTest(table=table):
                        left = spark.read.format('delta').load(str(root / 'runs/02_compression' / table))
                        right = spark.read.format('delta').load(str(root / 'runs/03_date_partitioning' / table))
                        self.assertTrue(compare_frames(left, right)['equal'])
                        log = root / 'runs/03_date_partitioning' / table / '_delta_log/00000000000000000000.json'
                        metadata = next(x['metaData'] for x in map(json.loads, log.read_text().splitlines()) if 'metaData' in x)
                        self.assertEqual(metadata['partitionColumns'], ['reporting_date'] if table == 'silver/impressions' else [])
            finally:
                spark.stop()
