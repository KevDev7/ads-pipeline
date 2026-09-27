"""Check the codecs actually written to Parquet, outside measured pipeline time."""
import argparse
import json
from pathlib import Path
import sys


def inspect_table(spark, table: Path, expected: str) -> dict:
    files = sorted(table.rglob('*.parquet'))
    if not files:
        raise ValueError(f'No Parquet data files in {table}')
    jvm = spark._jvm
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    codecs = set()
    row_groups = rows = column_chunks = 0
    for file in files:
        footer = jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(
            hadoop, jvm.org.apache.hadoop.fs.Path(str(file.resolve())))
        for block in footer.getBlocks():
            row_groups += 1
            rows += block.getRowCount()
            for column in block.getColumns():
                column_chunks += 1
                codecs.add(str(column.getCodec().name()))
    if codecs != {expected.upper()}:
        raise ValueError(f'{table}: expected {expected}, found {sorted(codecs)}')
    return {'files': len(files), 'bytes': sum(p.stat().st_size for p in files),
            'row_groups': row_groups, 'rows': rows, 'column_chunks': column_chunks,
            'codecs': sorted(codecs), 'matches_expected': True}


def inspect_comparison(root: Path, spark) -> dict:
    comparison = json.loads((root / 'comparison.json').read_text())
    if comparison['status'] != 'succeeded':
        raise ValueError('Inspect a completed successful comparison')
    result = {'comparison_id': comparison['comparison_id'], 'runs': {},
              'method': 'Read every Parquet data-file footer and every column chunk; no pipeline timing included.'}
    for run in comparison['runs']:
        report = run['report']
        expected = report['environment']['spark_sql_settings']['spark.sql.parquet.compression.codec']
        tables = {}
        for name, stats in report['tables'].items():
            table = root / 'runs' / run['run_id'] / name
            check = inspect_table(spark, table, expected)
            if check['rows'] != stats['rows'] or check['files'] != stats['parquet_files']:
                raise ValueError(f'{run["run_id"]} {name}: footer totals disagree with run report')
            tables[name] = check
        result['runs'][run['run_id']] = {'expected_codec': expected, 'tables': tables}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison_dir', type=Path)
    args = parser.parse_args()
    root = args.comparison_dir.resolve()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'experiments/00_baseline'))
    from session import create_spark
    spark = create_spark(root / 'codec-inspection-events')
    try:
        result = inspect_comparison(root, spark)
        (root / 'codecs.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
        print(f'All data-file codecs verified: {root / "codecs.json"}')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
