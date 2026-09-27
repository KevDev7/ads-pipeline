"""Exact logical comparison, independent of file layout and row order."""
from pyspark.sql import DataFrame


def compare_frames(left: DataFrame, right: DataFrame) -> dict:
    left_types = {field.name: field.dataType.jsonValue() for field in left.schema}
    right_types = {field.name: field.dataType.jsonValue() for field in right.schema}
    if len(left_types) != len(left.columns) or len(right_types) != len(right.columns):
        raise ValueError('Cannot compare ambiguous duplicate column names')
    if left_types != right_types:
        return {'equal': False, 'reason': 'schema_mismatch',
                'left_schema': left_types, 'right_schema': right_types}
    columns = sorted(left.columns)
    # exceptAll resolves columns by position. Align by name before comparing.
    # ALL retains duplicate multiplicity; SQL set operations compare nulls safely.
    left, right = left.select(*columns), right.select(*columns)
    left_extra = left.exceptAll(right).limit(1).count() != 0
    right_extra = right.exceptAll(left).limit(1).count() != 0
    return {'equal': not (left_extra or right_extra),
            'left_has_unmatched_rows': left_extra, 'right_has_unmatched_rows': right_extra,
            'method': 'bidirectional EXCEPT ALL; exact values/types; unordered rows; duplicates retained'}
