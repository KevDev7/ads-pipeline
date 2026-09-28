"""Run an unchanged experiment while reading extra effective SQL settings."""
import argparse
import importlib.util
from pathlib import Path
import sys

SETTINGS = (
    'spark.sql.adaptive.coalescePartitions.enabled',
    'spark.sql.adaptive.coalescePartitions.initialPartitionNum',
    'spark.sql.adaptive.coalescePartitions.parallelismFirst',
    'spark.sql.adaptive.coalescePartitions.minPartitionSize',
    'spark.sql.adaptive.advisoryPartitionSizeInBytes',
    'spark.sql.adaptive.localShuffleReader.enabled',
    'spark.sql.adaptive.skewJoin.enabled',
)


def run(entry, source, output, run_id):
    # Invoked in its own process: experiment-local imports cannot leak across runs.
    entry = entry.resolve()
    sys.path.insert(0, str(entry.parent))
    spec = importlib.util.spec_from_file_location('observed_pipeline', entry)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    original = module.environment

    def environment(spark):
        result = original(spark)
        result['spark_sql_settings'].update({key: spark.conf.get(key) for key in SETTINGS
                                             if key not in result['spark_sql_settings']})
        return result

    module.environment = environment
    return module.run(source, output, run_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--entry', type=Path, required=True)
    parser.add_argument('--source-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    run(args.entry, args.source_dir, args.output_dir, args.run_id)
