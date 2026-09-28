"""Real format roundtrip, query equality, typed nulls, quotes, and duplicates."""
import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('format_experiment', ROOT / 'experiments/01_csv_vs_parquet/run.py')
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)


class FormatTests(unittest.TestCase):
    def test_roundtrip_queries_and_uncompressed_files(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); source = root / 'raw_sample.csv'; output = root / 'output'; output.mkdir()
            row = [1, lab.DAY_START, 10, 'slot,"quoted"', 0, 1]
            with source.open('w', newline='') as handle:
                writer = csv.writer(handle); writer.writerow(lab.SCHEMA.fieldNames())
                writer.writerows([row, row, ['NULL', lab.DAY_START + 86399, 20, '', 1, 0],
                                  [2, lab.DAY_START - 1, 20, 'p2', 1, 0],
                                  [3, lab.DAY_START + 86400, 20, 'p2', 0, 1]])
            lab.prepare(source, output)
            lab.run_queries(source, output, 'csv', 'csv', list(lab.WORKLOADS))
            lab.run_queries(source, output, 'parquet', 'parquet', list(lab.WORKLOADS))
            lab.verify(source, output)
            a = json.loads((output / 'queries/csv/report.json').read_text())
            b = json.loads((output / 'queries/parquet/report.json').read_text())
            self.assertEqual(a['schema'], b['schema'])
            self.assertIn('slot,"quoted"', [r['pid'] for r in a['queries']['narrow']['rows']])
            self.assertIn(None, [r['pid'] for r in a['queries']['narrow']['rows']])
            for name in lab.WORKLOADS:
                self.assertEqual(a['queries'][name]['rows'], b['queries'][name]['rows'])
            self.assertEqual(sum(x['impressions'] for x in a['queries']['one_day']['rows']), 3)
            self.assertEqual(sum(x['clicks'] for x in a['queries']['one_day']['rows']), 2)
            self.assertEqual(sum(x['impressions'] for x in a['queries']['narrow']['rows']), 5)
            proof = json.loads((output / 'verification.json').read_text())
            self.assertTrue(proof['equality']['equal'])
            self.assertEqual(proof['rows'], 5)
            self.assertEqual(proof['parquet_footer']['codecs'], ['UNCOMPRESSED'])
