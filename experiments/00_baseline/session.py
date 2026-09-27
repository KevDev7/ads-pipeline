"""Only runtime, Delta integration, timestamp, and observability settings."""
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark(event_dir: Path) -> SparkSession:
    event_dir.mkdir(parents=True, exist_ok=True)
    runtime = Path('.runtime').resolve()
    runtime.mkdir(exist_ok=True)
    builder = (
        SparkSession.builder.appName('ads-pipeline-baseline')
        .master('local[4]')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.session.timeZone', 'Asia/Shanghai')
        .config('spark.jars.ivy', str(runtime / 'ivy'))
        .config('spark.local.dir', str(runtime / 'scratch'))
        .config('spark.eventLog.enabled', 'true')
        .config('spark.eventLog.dir', event_dir.resolve().as_uri())
        .config('spark.eventLog.compress', 'false')
        .config('spark.eventLog.rolling.enabled', 'false')
        .config('spark.ui.showConsoleProgress', 'false')
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark
