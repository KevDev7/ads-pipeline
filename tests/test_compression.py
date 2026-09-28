"""Exercise the real iteration entrypoint and inspect actual file codecs."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmarks.equality import compare_frames
from benchmarks.reporting import validate_reports
from benchmarks.storage_codecs import inspect_table
from test_baseline import write_fixture

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'experiments/00_baseline'))
from session import create_spark


class CompressionTests(unittest.TestCase):
    def test_entrypoints_preserve_all_tables_and_write_requested_codecs(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_fixture(root)
            for name in ('00_baseline', '02_compression'):
                with (root / f'{name}.log').open('w') as log:
                    process = subprocess.run(
                        [sys.executable, str(ROOT / 'experiments' / name / 'run.py'),
                         '--source-dir', str(root), '--output-dir', str(root / 'runs'), '--run-id', name],
                        cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                self.assertEqual(process.returncode, 0, (root / f'{name}.log').read_text()[-8000:])
            left = json.loads((root / 'runs/00_baseline/report.json').read_text())
            right = json.loads((root / 'runs/02_compression/report.json').read_text())
            key = 'spark.sql.parquet.compression.codec'
            self.assertEqual(validate_reports(left, right, [key]), {
                key: {'reference': 'snappy', 'current': 'zstd'}})
            spark = create_spark(root / 'verification-events')
            try:
                for name in left['tables']:
                    a, b = root / 'runs/00_baseline' / name, root / 'runs/02_compression' / name
                    with self.subTest(table=name):
                        self.assertTrue(compare_frames(spark.read.format('delta').load(str(a)),
                                                       spark.read.format('delta').load(str(b)))['equal'])
                        self.assertEqual(inspect_table(spark, a, 'snappy')['rows'], left['tables'][name]['rows'])
                        self.assertEqual(inspect_table(spark, b, 'zstd')['rows'], right['tables'][name]['rows'])
                with self.assertRaisesRegex(ValueError, 'expected snappy'):
                    inspect_table(spark, root / 'runs/02_compression/gold/campaign_daily', 'snappy')
            finally:
                spark.stop()
