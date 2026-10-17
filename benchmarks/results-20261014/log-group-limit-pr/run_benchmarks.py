"""Run only through campaign run_job.py benchmark (exclusive real-disk lock)."""
import argparse
import csv
import hashlib
import importlib.util
import json
import os
import re
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
CAMP = HERE.parent.parent
spec = importlib.util.spec_from_file_location('runner', HERE / 'runner_core.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
r.COORD = HERE
r.OUT = CAMP / 'benchmarks/log-pr-1024-20261014'
os.environ.pop('PREFIX_DISABLE', None)

original_newmode = r.newmode
def newmode(mode):
    folder, manifest = original_newmode(mode)
    manifest['policy_runner_sha256'] = r.sha(__file__)
    manifest['prefix_disable_environment'] = 'unset explicitly'
    r.save(folder/'manifest.json', manifest)
    return folder, manifest
r.newmode = newmode

original_cmdrun = r.cmdrun
def cmdrun(folder, manifest, label, profile, binary, args, env=None, timeout=600):
    rec = original_cmdrun(folder, manifest, label, profile, binary, args, env, timeout)
    rec['environment_overrides'] = env or {}
    r.save(folder/'manifest.json', manifest)
    return rec
r.cmdrun = cmdrun

def log_matrix(mode='log-async', rounds=None, thread_counts=None, seconds=8):
    folder, m = r.newmode(mode)
    m['scope'] = 'New default 1024 versus explicit 1024 and main; short PR readiness screen'
    m['verification'] = 'Positive inserts, API errors, successful draining close; recovery validated separately'
    data = []
    options = {'main': ('main', 0), 'default1024': ('candidate', 0),
               'explicit1024': ('candidate', 1024)}
    rounds = [['main', 'default1024', 'explicit1024',
               'explicit1024', 'default1024', 'main']]
    for threads in thread_counts or [1, 8]:
        for rep, order in enumerate(rounds):
            for index, version in enumerate(order):
                profile, pages = options[version]
                label = f't{threads}-r{rep}-{index}-{version}'
                stem = folder / label
                db = folder / 'scratch.db'
                assert not db.exists()
                args = ['--db', db, '--csv', str(stem)+'.csv', '--summary', str(stem)+'.summary.csv', '--records', 100000000, '--seconds', seconds, '--report-ms', 100, '--cache-mib', 256, '--disk-gib', 16, '--direct', '--inserters', threads, '--readers', 0, '--memtable-bg', 1, '--normal-bg', 3, '--memtable-mib', 8, '--key-size', 16, '--value-size', 16, '--min-free-gib', 12, '--seed', 42, '--log-group-size-pages', pages]
                rec = r.cmdrun(folder, m, label, profile, 'insert_benchmark', args, timeout=180)
                summary = r.rows(Path(str(stem)+'.summary.csv'))
                assert len(summary) == 1
                summary = summary[0]
                inserted = int(summary['inserted'])
                assert inserted > 0
                log = (folder / (label+'.log')).read_text()
                error = re.search(r'load_end .* error=([0-9]+)(?: |\n)', log)
                assert error and int(error[1]) == 0
                close = re.search(r'close_s=([\d.]+) close_rc=0', log)
                assert close
                assert not re.search(r'(automatic.*(failed|error)|durable barrier.*failed)', log, re.I), log[-2000:]
                intervals = r.rows(Path(str(stem)+'.csv'))
                cpu = sum(float(x['cpu_cores'])*float(x['interval_s']) for x in intervals)
                row = dict(threads=threads, rep=rep, index=index, version=version, ops_s=float(summary['achieved_insert_ops_s']), inserted=inserted, elapsed_s=float(summary['elapsed_s']), close_s=float(close[1]), drain_ops_s=inserted/(float(summary['elapsed_s'])+float(close[1])), cpu_us_op=cpu*1e6/inserted, p99_us=1000*float(summary['insert_service_p99_ms']), p999_us=1000*float(summary['insert_service_p999_ms']), max_ms=float(summary['insert_service_max_ms']), read_B_op=sum(float(x['read_mib_s'])*float(x['interval_s']) for x in intervals)*2**20/inserted, write_B_op=sum(float(x['write_mib_s'])*float(x['interval_s']) for x in intervals)*2**20/inserted)
                data.append(row)
                row['process_cpu_us_op'] = rec['process_cpu_s']*1e6/inserted
                row['process_read_B_op'] = rec['process_read_bytes']/inserted
                row['process_write_B_op'] = rec['process_write_bytes']/inserted
                m['metric_windows'] = 'cpu_us_op/read_B_op/write_B_op are sampled active-window totals divided by joined insert count; process_* includes startup and draining close'
                rec.update(verified=True, errors=0, inserted=inserted, close_s=row['close_s'], allocated_bytes=db.stat().st_blocks*512)
                r.save(folder/'manifest.json', m)
                r.csvwrite(folder/'results.csv', data)
                db.unlink()
    m['complete'] = True
    r.save(folder/'manifest.json', m)

if __name__ == '__main__':
    log_matrix(mode='default-check', seconds=10)
