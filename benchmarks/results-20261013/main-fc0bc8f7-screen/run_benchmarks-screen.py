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
spec = importlib.util.spec_from_file_location('runner', HERE.parent / 'main-7bb5fab/benchmarks/run_matrix.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
r.COORD = HERE
r.OUT = CAMP / 'benchmarks/main-fc0bc8f7'

def log_matrix():
    folder, m = r.newmode('log-async')
    m['scope'] = 'Same main base; sync versus async policy, disabled policy, and threshold sensitivity'
    m['verification'] = 'Positive inserts, API errors, successful draining close; recovery validated separately'
    data = []
    options = {'main': ('main', 0), 'disabled': ('log-async', 2**64-1),
               'sync64': ('log-sync', 0), 'async64': ('log-async', 0),
               'sync1024': ('log-sync', 1024), 'async1024': ('log-async', 1024)}
    rounds = [
        ['main', 'disabled', 'sync64', 'async64', 'sync1024', 'async1024', 'main'],
        ['main', 'async1024', 'sync1024', 'async64', 'sync64', 'disabled', 'main'],
        ['main', 'async64', 'disabled', 'sync1024', 'async1024', 'sync64', 'main'],
    ]
    for threads in [1, 8]:
        for rep, order in enumerate(rounds):
            for index, version in enumerate(order):
                profile, pages = options[version]
                label = f't{threads}-r{rep}-{index}-{version}'
                stem = folder / label
                db = folder / 'scratch.db'
                assert not db.exists()
                args = ['--db', db, '--csv', str(stem)+'.csv', '--summary', str(stem)+'.summary.csv', '--records', 100000000, '--seconds', 8, '--report-ms', 100, '--cache-mib', 256, '--disk-gib', 16, '--direct', '--inserters', threads, '--readers', 0, '--memtable-bg', 1, '--normal-bg', 3, '--memtable-mib', 8, '--key-size', 16, '--value-size', 16, '--min-free-gib', 12, '--seed', 42, '--log-group-size-pages', pages]
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
                assert not re.search(r'automatic.*(failed|error)', log, re.I), log[-2000:]
                intervals = r.rows(Path(str(stem)+'.csv'))
                cpu = sum(float(x['cpu_cores'])*float(x['interval_s']) for x in intervals)
                row = dict(threads=threads, rep=rep, index=index, version=version, ops_s=float(summary['achieved_insert_ops_s']), inserted=inserted, elapsed_s=float(summary['elapsed_s']), close_s=float(close[1]), drain_ops_s=inserted/(float(summary['elapsed_s'])+float(close[1])), cpu_us_op=cpu*1e6/inserted, p99_us=1000*float(summary['insert_service_p99_ms']), p999_us=1000*float(summary['insert_service_p999_ms']), max_ms=float(summary['insert_service_max_ms']), read_B_op=sum(float(x['read_mib_s'])*float(x['interval_s']) for x in intervals)*2**20/inserted, write_B_op=sum(float(x['write_mib_s'])*float(x['interval_s']) for x in intervals)*2**20/inserted)
                data.append(row)
                rec.update(verified=True, errors=0, inserted=inserted, close_s=row['close_s'], allocated_bytes=db.stat().st_blocks*512)
                r.save(folder/'manifest.json', m)
                r.csvwrite(folder/'results.csv', data)
                db.unlink()
    m['complete'] = True
    r.save(folder/'manifest.json', m)

def branch_screen():
    folder, m = r.newmode('branch-screen')
    m['scope'] = 'Small resident integration screen; real-disk databases, not comprehensive retuning'
    fixtures = {}
    for profile, pattern in [('main', 'ordinary'), ('main', 'common'), ('branch-prefix', 'common'), ('main', 'random'), ('branch-prefix', 'random')]:
        label = f'fixture-{profile}-{pattern}'
        db = folder / (label+'.db')
        binary = 'kvbench' if pattern == 'ordinary' else 'prefix_kvbench'
        args = r.kvargs(db, folder/(label+'.csv'), profile, 'resident', 0, secs=1, create=True)+['load:random']
        rec = r.cmdrun(folder, m, label, profile, binary, args, {'PREFIX_PATTERN': pattern} if pattern != 'ordinary' else None)
        r.verifykv(folder/(label+'.csv'), 0, 1)
        with db.open('rb') as f:
            os.fsync(f.fileno())
        rec.update(verified=True, fixture_sha256=r.sha(db), allocated_bytes=db.stat().st_blocks*512)
        fixtures[(profile, pattern)] = db
        r.save(folder/'manifest.json', m)
    cases = [('ordinary', ['main', 'cache-invalid-addresses', 'directory-direct', 'directory-bucket', 'directory-bucket-simd', 'directory-bucket-atomic', 'main', 'directory-bucket-atomic', 'directory-bucket-simd', 'directory-bucket', 'directory-direct', 'cache-invalid-addresses', 'main']),
             ('common', ['main', 'branch-prefix', 'branch-prefix', 'main']),
             ('random', ['main', 'branch-prefix', 'branch-prefix', 'main'])]
    for pattern, order in cases:
        for index, profile in enumerate(order):
            label = f'{pattern}-{index}-{profile}'
            db = folder/'scratch.db'
            assert not db.exists()
            fixture = fixtures[('branch-prefix' if profile == 'branch-prefix' else 'main', pattern)]
            r.subprocess.run(['cp', '--reflink=auto', '--sparse=always', str(fixture), str(db)], check=True)
            with db.open('rb') as f:
                os.fsync(f.fileno())
            csvpath = folder/(pattern+'-results.csv')
            args = r.kvargs(db, csvpath, profile, 'resident', index, secs=3)+['warm', 'point:uniform@8', 'range:uniform:1@8', 'range:uniform:10000@8']
            rec = r.cmdrun(folder, m, label, profile, 'kvbench' if pattern == 'ordinary' else 'prefix_kvbench', args, {'PREFIX_PATTERN': pattern} if pattern != 'ordinary' else None)
            rows = r.verifykv(csvpath, index, 4)
            rec.update(verified=True, errors=0, violations=0, fixture=str(fixture), allocated_bytes=db.stat().st_blocks*512)
            r.save(folder/'manifest.json', m)
            db.unlink()
    m['complete'] = True
    r.save(folder/'manifest.json', m)

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['log', 'screen'])
    a = p.parse_args()
    {'log': log_matrix, 'screen': branch_screen}[a.mode]()
