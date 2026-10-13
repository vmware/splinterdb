#!/usr/bin/env python3
"""Coordinate this campaign's builds/tests and disk-backed benchmarks."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
p = argparse.ArgumentParser()
p.add_argument('mode', choices=['build', 'test', 'benchmark'])
p.add_argument('--name', required=True)
p.add_argument('--db-dir', type=Path)
p.add_argument('command', nargs=argparse.REMAINDER)
a = p.parse_args()
command = a.command[1:] if a.command[:1] == ['--'] else a.command
if not command:
    p.error('command required after --')
if a.mode == 'benchmark':
    if a.db_dir is None:
        p.error('benchmark requires --db-dir')
    a.db_dir.mkdir(parents=True, exist_ok=True)
    db_dir = a.db_dir.resolve()
    if not db_dir.is_relative_to(ROOT / 'benchmarks'):
        p.error('benchmark database directory must be under the campaign benchmarks directory')
    fs = subprocess.check_output(['findmnt', '-n', '-o', 'FSTYPE,SOURCE', '-T', str(db_dir)], text=True).strip()
    if fs.split()[0] in {'tmpfs', 'ramfs'} or not fs.split()[1].startswith('/dev/'):
        p.error('benchmark storage must be a verified disk filesystem: ' + fs)
    bench_lock = open(ROOT / 'coordination' / 'benchmark.lock', 'a+')
    fcntl.flock(bench_lock, fcntl.LOCK_EX)
    print('Benchmark database storage:', fs, str(db_dir), flush=True)
machine_lock = open(ROOT / 'coordination' / 'machine.lock', 'a+')
fcntl.flock(machine_lock, fcntl.LOCK_EX if a.mode == 'benchmark' else fcntl.LOCK_SH)
record = dict(name=a.name, mode=a.mode, cwd=os.getcwd(), command=command,
              started=time.time(), pid=os.getpid())
if a.db_dir is not None:
    record['db_dir'] = str(a.db_dir.resolve())
print('Starting', a.name, a.mode, flush=True)
try:
    # Tool commands run in distinct PID namespaces; GNU make's FIFO names
    # otherwise collide when several builds choose /tmp/GMfifo8.
    with tempfile.TemporaryDirectory(prefix='splinter-' + a.name + '-', dir='/tmp') as temporary:
        environment = dict(os.environ, TMPDIR=temporary)
        result = subprocess.run(command, env=environment)
    record['returncode'] = result.returncode
finally:
    record['finished'] = time.time()
    with open(ROOT / 'coordination' / 'jobs.jsonl', 'a') as journal:
        fcntl.flock(journal, fcntl.LOCK_EX)
        journal.write(json.dumps(record) + '\n')
raise SystemExit(result.returncode)
