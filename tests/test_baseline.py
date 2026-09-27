"""A tiny, hand-checkable dataset exercises correctness, including reruns."""
import csv
from datetime import datetime
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'experiments/00_baseline'))
from pyspark.sql import functions as F
from run import run
from session import create_spark
from tables import SOURCES, aggregate, clean, enrich, read_csv
from validation import valid_dimensions, valid_gold, valid_impressions


def write_fixture(root):
    def epoch(text):
        return int(datetime.fromisoformat(text + '+08:00').timestamp())
    rows = {
        'impressions': [
            [1, epoch('2017-05-06T00:00:00'), 10, 'p1', 0, 1],
            [1, epoch('2017-05-06T23:59:59'), 10, 'p1', 1, 0],
            [2, epoch('2017-05-06T12:00:00'), 20, 'p2', 0, 1],
            [3, epoch('2017-05-06T18:00:00'), 20, 'p2', 1, 0],
            [2, epoch('2017-05-07T00:00:00'), 10, 'p1', 1, 0],
        ],
        'ads': [[10, 1, 100, 1000, 'NULL', 170], [20, 2, 200, 1000, 999, 99999999]],
        'user_profiles': [[1, 0, 1, 1, 3, '', 2, 0, 1], [2, 0, 1, 2, 4, 2, 2, 0, '']],
    }
    for name, (filename, header) in SOURCES.items():
        # Reproduce the actual trailing space in the profile CSV header.
        if name == 'user_profiles':
            header += ' '
        with (root / filename).open('w', newline='') as handle:
            writer = csv.writer(handle)
            writer.writerow(header.split(','))
            writer.writerows(rows[name])


class TransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        write_fixture(cls.root)
        cls.spark = create_spark(cls.root / 'events')
        cls.bronze = {n: read_csv(cls.spark, cls.root, n) for n in SOURCES}
        cls.silver = {n: clean(n, d) for n, d in cls.bronze.items()}

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_source_values_nulls_and_price_are_preserved(self):
        self.assertEqual(self.bronze['ads'].filter("adgroup_id = '10'").first().brand, 'NULL')
        self.assertIsNone(self.silver['ads'].filter('adgroup_id = 10').first().brand_id)
        self.assertEqual(self.silver['ads'].filter('adgroup_id = 20').first().product_price, 99999999)
        self.assertIsNone(self.silver['user_profiles'].filter('user_id = 1').first().pvalue_level)
        self.assertIn('new_user_class_level', self.bronze['user_profiles'].columns)

    def test_counts_ctr_unknown_profile_and_timezone(self):
        imps, ads, users = [self.silver[n] for n in SOURCES]
        expected = valid_impressions(imps, ads, users)
        self.assertEqual(expected, {'impressions': 5, 'clicks': 2,
                                   'first_date': '2017-05-06', 'last_date': '2017-05-07',
                                   'missing_profile_impressions': 1})
        enriched = enrich(imps, ads, users)
        gold = aggregate(enriched)
        valid_gold(gold, expected, 'campaign_daily')
        actual = {(str(r.reporting_date), r.campaign_id): (r.impressions, r.clicks, r.ctr) for r in gold.collect()}
        self.assertEqual(actual, {('2017-05-06', 100): (2, 1, 0.5),
                                  ('2017-05-06', 200): (2, 1, 0.5),
                                  ('2017-05-07', 100): (1, 0, 0.0)})
        unknown = aggregate(enriched, True).filter('age_level IS NULL').first()
        self.assertEqual((unknown.impressions, unknown.missing_profile_impressions), (1, 1))

    def test_duplicate_dimension_key_rejected(self):
        ads = self.silver['ads']
        with self.assertRaisesRegex(ValueError, 'duplicate adgroup_id'):
            valid_dimensions(ads.unionByName(ads.limit(1)), self.silver['user_profiles'])

    def test_invalid_flags_and_missing_ads_rejected(self):
        imps, ads, users = [self.silver[n] for n in SOURCES]
        with self.assertRaisesRegex(ValueError, 'click flags'):
            valid_impressions(imps.withColumn('clicked', F.lit(2)), ads, users)
        with self.assertRaisesRegex(ValueError, 'unmatched ad'):
            valid_impressions(imps, ads.filter('adgroup_id = 10'), users)

    def test_malformed_numeric_value_is_not_silently_nulled(self):
        bad = self.bronze['impressions'].withColumn('time_stamp', F.lit('broken'))
        with self.assertRaises(Exception):
            clean('impressions', bad).collect()

    def test_unexpected_header_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'raw_sample.csv').write_text('wrong,header\n1,2\n')
            with self.assertRaisesRegex(ValueError, 'Unexpected CSV header'):
                read_csv(self.spark, root, 'impressions')


class ZEndToEndTests(unittest.TestCase):
    def test_fresh_runs_reconcile_and_existing_run_is_protected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_fixture(root)
            first = run(root, root / 'output', 'first')
            second = run(root, root / 'output', 'second')
            self.assertEqual(first['status'], 'succeeded')
            self.assertEqual(first['checks'], second['checks'])
            self.assertEqual(first['tables']['gold/campaign_daily']['rows'], 3)
            self.assertEqual(first['tables']['gold/audience_daily']['rows'], 4)
            self.assertGreater(first['spark_tasks']['events_read'], 0)
            self.assertGreater(first['spark_tasks']['by_operation']['bronze.impressions']['input_bytes'], 0)
            self.assertEqual(json.loads((root / 'output/first/report.json').read_text())['status'], 'succeeded')
            with self.assertRaises(FileExistsError):
                run(root, root / 'output', 'first')
            (root / 'raw_sample.csv').write_text('wrong,header\n1,2\n')
            with self.assertRaisesRegex(ValueError, 'Unexpected CSV header'):
                run(root, root / 'output', 'bad-header')
            failed = json.loads((root / 'output/bad-header/report.json').read_text())
            self.assertEqual(failed['status'], 'failed')
            self.assertIn('Unexpected CSV header', failed['error'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
