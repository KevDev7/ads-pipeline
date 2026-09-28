"""Exercise real pipelines, observer neutrality and execution-evidence failures."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from benchmarks.coalescing import inspect_run, KEY
from benchmarks.observed_run import SETTINGS
from benchmarks.equality import compare_frames
from benchmarks.reporting import validate_reports
from test_baseline import write_fixture, create_spark

ROOT = Path(__file__).resolve().parents[1]


class CoalescingTests(unittest.TestCase):
    def test_real_entrypoints_observer_neutrality_and_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); write_fixture(root); reports={}
            for name, version, observed in [('direct','02_compression',False),
                                           ('control','02_compression',True),
                                           ('variant','08_no_shuffle_coalescing',True)]:
                entry=ROOT/'experiments'/version/'run.py'
                command=([sys.executable,'-m','benchmarks.observed_run','--entry',str(entry)]
                         if observed else [sys.executable,str(entry)])
                with (root/f'{name}.log').open('w') as log:
                    p=subprocess.run(command+['--source-dir',str(root),'--output-dir',str(root/'runs'),
                                              '--run-id',name],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
                self.assertEqual(p.returncode,0,(root/f'{name}.log').read_text()[-8000:])
                reports[name]=json.loads((root/'runs'/name/'report.json').read_text())
            original=reports['direct']; observed=copy.deepcopy(reports['control'])
            for key in SETTINGS: observed['environment']['spark_sql_settings'].pop(key)
            self.assertEqual(validate_reports(original,observed),{})
            self.assertEqual(validate_reports(reports['control'],reports['variant'],[KEY]),
                             {KEY:{'reference':'true','current':'false'}})
            control=inspect_run(root/'runs/control/events',reports['control'],True)
            variant=inspect_run(root/'runs/variant/events',reports['variant'],False)
            self.assertTrue(all(x['coalescing_observed'] for x in control['executions'].values()))
            self.assertTrue(all(not x['coalescing_observed'] for x in variant['executions'].values()))
            self.assertTrue(all(x['reader_tasks']>0 for x in variant['executions'].values()))
            with self.assertRaisesRegex(ValueError,'Contradictory setting'):
                inspect_run(root/'runs/variant/events',reports['variant'],True)
            events=[]
            for path in sorted((root/'runs/variant/events').rglob('*')):
                if not path.is_file() or path.name.startswith(('.', 'appstatus')): continue
                with path.open() as handle: events.extend(json.loads(line) for line in handle)
            execution=variant['executions']['gold.campaign_daily']['execution_id']
            stage=variant['executions']['gold.campaign_daily']['stages'][0]['stage_id']
            damaged=root/'damaged'; damaged.mkdir()
            for mutation, message in [('sql','did not succeed'),('plan','contradicts'),('stage','stage/task')]:
                with self.subTest(mutation=mutation):
                    changed=copy.deepcopy(events)
                    for event in changed:
                        if mutation=='sql' and event.get('Event','').endswith('SparkListenerSQLExecutionEnd') and str(event['executionId'])==execution:
                            event['errorMessage']='injected SQL failure'
                        if mutation=='plan' and 'sparkPlanInfo' in event and str(event['executionId'])==execution:
                            event['sparkPlanInfo']={'nodeName':'AQEShuffleRead','simpleString':'AQEShuffleRead coalesced',
                                                   'children':[event['sparkPlanInfo']]}
                    if mutation=='stage':
                        changed=[e for e in changed if not(e.get('Event')=='SparkListenerTaskEnd' and e['Stage ID']==stage)]
                    (damaged/'events').write_text('\n'.join(map(json.dumps,changed)))
                    with self.assertRaisesRegex(ValueError,message): inspect_run(damaged,reports['variant'],False)
            spark=create_spark(root/'verification-events')
            try:
                for candidate in ('control','variant'):
                    for table in original['tables']:
                        with self.subTest(candidate=candidate,table=table):
                            a=spark.read.format('delta').load(str(root/'runs/direct'/table))
                            b=spark.read.format('delta').load(str(root/'runs'/candidate/table))
                            self.assertTrue(compare_frames(a,b)['equal'])
            finally: spark.stop()

    def test_missing_settings_and_execution_evidence(self):
        report={'environment':{'spark_sql_settings':{}}}
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError,'Missing effective'):
                inspect_run(Path(folder),report,False)
            settings={k:None for k in SETTINGS}
            settings.update({KEY:'false','spark.sql.adaptive.enabled':'true','spark.sql.shuffle.partitions':'200'})
            report['environment']['spark_sql_settings']=settings
            with self.assertRaisesRegex(ValueError,'Missing Gold'):
                inspect_run(Path(folder),report,False)
