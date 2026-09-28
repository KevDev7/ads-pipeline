"""Small JSON reports and task counters; full event logs stay out of Git."""
import hashlib
import json
import platform
import subprocess
from pathlib import Path


def fingerprint(path: Path) -> dict:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return {'bytes': path.stat().st_size, 'sha256': digest.hexdigest()}


def storage(path: Path) -> dict:
    files = [p for p in path.rglob('*') if p.is_file()]
    parquet = [p for p in files if p.suffix == '.parquet']
    return {'total_bytes': sum(p.stat().st_size for p in files),
            'parquet_bytes': sum(p.stat().st_size for p in parquet),
            'parquet_files': len(parquet)}


def environment(spark) -> dict:
    def git(*args):
        result = subprocess.run(['git', *args], capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None
    keys = ['spark.sql.adaptive.enabled', 'spark.sql.shuffle.partitions',
            'spark.sql.autoBroadcastJoinThreshold', 'spark.sql.files.maxPartitionBytes',
            'spark.sql.parquet.compression.codec', 'spark.sql.ansi.enabled',
            'spark.sql.session.timeZone']
    return {'python': platform.python_version(), 'spark': spark.version, 'delta': '4.0.0',
            'java': spark._jvm.java.lang.System.getProperty('java.version'),
            'platform': platform.platform(), 'machine': platform.machine(),
            'master': spark.sparkContext.master,
            'driver_memory': spark.sparkContext.getConf().get('spark.driver.memory', '1g'),
            'git_commit': git('rev-parse', 'HEAD'), 'git_dirty': bool(git('status', '--porcelain')),
            'spark_sql_settings': {key: spark.conf.get(key) for key in keys}}


def summarize_events(event_dir: Path) -> dict:
    groups = {}
    totals = {}
    events_read = 0
    for path in sorted(event_dir.rglob('*')):
        if not path.is_file() or path.name.startswith('.') or path.name.startswith('appstatus'):
            continue
        with path.open(encoding='utf-8') as handle:
            for line in handle:
                event = json.loads(line)
                events_read += 1
                kind = event.get('Event')
                if kind == 'SparkListenerStageSubmitted':
                    stage = event['Stage Info']['Stage ID']
                    groups[stage] = (event.get('Properties') or {}).get('spark.jobGroup.id', 'internal')
                if kind != 'SparkListenerTaskEnd':
                    continue
                group = groups.get(event['Stage ID'], 'internal')
                acc = totals.setdefault(group, {key: 0 for key in [
                    'task_attempts', 'failed_task_attempts', 'executor_run_ms', 'executor_cpu_ns',
                    'input_bytes', 'input_records', 'shuffle_read_bytes', 'shuffle_write_bytes',
                    'memory_spill_bytes', 'disk_spill_bytes', 'peak_task_execution_memory_bytes']})
                metric = event.get('Task Metrics', {})
                acc['task_attempts'] += 1
                acc['failed_task_attempts'] += int(event.get('Task End Reason', {}).get('Reason') != 'Success')
                acc['executor_run_ms'] += metric.get('Executor Run Time', 0)
                acc['executor_cpu_ns'] += metric.get('Executor CPU Time', 0)
                acc['input_bytes'] += metric.get('Input Metrics', {}).get('Bytes Read', 0)
                acc['input_records'] += metric.get('Input Metrics', {}).get('Records Read', 0)
                read = metric.get('Shuffle Read Metrics', {})
                acc['shuffle_read_bytes'] += read.get('Remote Bytes Read', 0) + read.get('Local Bytes Read', 0)
                acc['shuffle_write_bytes'] += metric.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0)
                acc['memory_spill_bytes'] += metric.get('Memory Bytes Spilled', 0)
                acc['disk_spill_bytes'] += metric.get('Disk Bytes Spilled', 0)
                acc['peak_task_execution_memory_bytes'] = max(acc['peak_task_execution_memory_bytes'], metric.get('Peak Execution Memory', 0))
    if not events_read:
        raise ValueError('No Spark events found; cannot finish measurement')
    return {'events_read': events_read, 'by_operation': totals,
            'note': 'All task attempts, including validation and retries. Input bytes are task counters, not unique physical disk reads. Peak memory is per task, not process RAM.'}
