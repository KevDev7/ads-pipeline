"""Source-grounded transforms. Only the profile relation receives a MERGE join hint."""
import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


SOURCES = {
    'impressions': ('raw_sample.csv', 'user,time_stamp,adgroup_id,pid,nonclk,clk'),
    'ads': ('ad_feature.csv', 'adgroup_id,cate_id,campaign_id,customer,brand,price'),
    'user_profiles': ('user_profile.csv', 'userid,cms_segid,cms_group_id,final_gender_code,age_level,pvalue_level,shopping_level,occupation,new_user_class_level'),
}


def read_csv(spark: SparkSession, source: Path, name: str) -> DataFrame:
    filename, columns = SOURCES[name]
    path = source / filename
    with path.open(newline='', encoding='utf-8') as handle:
        actual = next(csv.reader(handle))
    if [column.strip() for column in actual] != columns.split(','):
        raise ValueError(f'Unexpected CSV header in {filename}: {actual}')
    # Use the actual header for CSV validation, then normalize header whitespace.
    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in actual])
    return (
        spark.read.schema(schema).option('header', True)
        .option('enforceSchema', False).option('mode', 'FAILFAST').csv(str(path))
        .toDF(*columns.split(','))
        .withColumn('source_file', F.lit(filename))
    )


def numeric(name: str, datatype: str, alias: str | None = None):
    value = F.trim(F.col(name))
    return F.when(value.isin('', 'NULL'), None).otherwise(value).cast(datatype).alias(alias or name)


def clean(name: str, df: DataFrame) -> DataFrame:
    if name == 'impressions':
        return df.select(
            numeric('user', 'long', 'user_id'), numeric('time_stamp', 'long', 'timestamp_seconds'),
            numeric('adgroup_id', 'long'), F.col('pid').alias('placement_id'),
            numeric('nonclk', 'int', 'nonclick'), numeric('clk', 'int', 'clicked'), 'source_file',
        ).withColumn('event_timestamp', F.timestamp_seconds('timestamp_seconds')).withColumn(
            'reporting_date', F.to_date('event_timestamp'))
    if name == 'ads':
        return df.select(
            numeric('adgroup_id', 'long'), numeric('cate_id', 'long', 'category_id'),
            numeric('campaign_id', 'long'), numeric('customer', 'long', 'advertiser_id'),
            numeric('brand', 'long', 'brand_id'), numeric('price', 'double', 'product_price'), 'source_file',
        )
    if name == 'user_profiles':
        return df.select(numeric('userid', 'long', 'user_id'), *[
            numeric(c, 'int') for c in SOURCES[name][1].split(',')[1:]
        ], 'source_file')
    raise ValueError(name)


def enrich(impressions: DataFrame, ads: DataFrame, users: DataFrame) -> DataFrame:
    # Left joins preserve observed impressions. Duplicate dimension keys are checked
    # before this function runs; otherwise a join could multiply the facts.
    return (
        impressions.join(ads.select('adgroup_id', 'campaign_id', 'advertiser_id'), 'adgroup_id', 'left')
        .join(users.select('user_id', 'age_level').withColumn('profile_found', F.lit(True)).hint('merge'), 'user_id', 'left')
        .withColumn('missing_profile', F.col('profile_found').isNull().cast('long'))
    )


def aggregate(enriched: DataFrame, audience: bool = False) -> DataFrame:
    keys = ['reporting_date', 'advertiser_id', 'age_level'] if audience else [
        'reporting_date', 'campaign_id', 'advertiser_id']
    return enriched.groupBy(*keys).agg(
        F.count('*').alias('impressions'), F.sum('clicked').alias('clicks'),
        F.sum('missing_profile').alias('missing_profile_impressions'),
    ).withColumn('ctr', F.col('clicks') / F.col('impressions'))


def read_table(spark: SparkSession, root: Path, layer: str, name: str) -> DataFrame:
    return spark.read.format('delta').load(str(root / layer / name))


def write_table(df: DataFrame, root: Path, layer: str, name: str) -> None:
    df.write.format('delta').mode('errorifexists').save(str(root / layer / name))
