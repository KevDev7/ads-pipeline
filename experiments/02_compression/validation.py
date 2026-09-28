"""Checks protect reported counts; missing optional attributes remain data."""
from pyspark.sql import DataFrame, functions as F


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def no_rows(df: DataFrame, message: str) -> None:
    require(df.limit(1).count() == 0, message)


def valid_dimensions(ads: DataFrame, users: DataFrame) -> dict:
    for name, df, key in [('ads', ads, 'adgroup_id'), ('user_profiles', users, 'user_id')]:
        no_rows(df.filter(F.col(key).isNull()), f'{name}: null {key}')
        no_rows(df.groupBy(key).count().filter('count > 1'), f'{name}: duplicate {key}')
    no_rows(ads.filter('campaign_id IS NULL OR advertiser_id IS NULL'), 'ads: missing campaign/advertiser')
    return {'dimension_keys_unique_and_present': True}


def valid_impressions(df: DataFrame, ads: DataFrame, users: DataFrame) -> dict:
    invalid = (
        F.col('user_id').isNull() | F.col('adgroup_id').isNull()
        | F.col('timestamp_seconds').isNull() | (F.col('timestamp_seconds') < 0)
        | F.col('placement_id').isNull() | (F.trim('placement_id') == '')
        | F.col('clicked').isNull() | F.col('nonclick').isNull()
        | ~F.col('clicked').isin(0, 1) | ~F.col('nonclick').isin(0, 1)
        | ((F.col('clicked') + F.col('nonclick')) != 1)
    )
    no_rows(df.filter(invalid), 'impressions: invalid required fields or click flags')
    no_rows(df.join(ads.select('adgroup_id'), 'adgroup_id', 'left_anti'), 'impressions: unmatched ad')
    stats = df.agg(F.count('*').alias('impressions'), F.sum('clicked').alias('clicks'),
                   F.min('reporting_date').cast('string').alias('first_date'),
                   F.max('reporting_date').cast('string').alias('last_date')).first().asDict()
    require(stats['impressions'] > 0, 'Empty impression source')
    stats['missing_profile_impressions'] = df.join(users.select('user_id'), 'user_id', 'left_anti').count()
    return stats


def valid_gold(df: DataFrame, expected: dict, name: str) -> dict:
    stats = df.agg(F.count('*').alias('rows'), F.sum('impressions').alias('impressions'),
                   F.sum('clicks').alias('clicks'),
                   F.sum('missing_profile_impressions').alias('missing_profile_impressions')).first().asDict()
    for metric in ['impressions', 'clicks', 'missing_profile_impressions']:
        require(stats[metric] == expected[metric], f'{name}: {metric} does not reconcile: {stats[metric]} != {expected[metric]}')
    no_rows(df.filter('ctr IS NULL OR ctr < 0 OR ctr > 1 OR impressions <= 0'), f'{name}: invalid CTR/count')
    return stats
