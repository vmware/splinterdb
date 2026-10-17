#!/usr/bin/env python3
"""Run invalid-address checks in private disk-backed working directories.

Invoke this script under run_job.py test or build, depending on mode.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

ROOT = Path(__file__).resolve().parent.parent
TREE = ROOT / 'worktrees/cache-invalid-addresses'
OUT = ROOT / 'coordination/cache-invalid-addresses-validation'
BUILD = TREE / 'build'
p = argparse.ArgumentParser()
p.add_argument('mode', choices=['debug', 'build-release', 'build-asan', 'release', 'asan'])
a = p.parse_args()
OUT.mkdir(parents=True, exist_ok=True)

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

unit = lambda mode, suite, *args: [str(BUILD / mode / 'bin/unit' / suite), *args]
driver = lambda *args: [str(BUILD / 'debug/bin/driver_test'), *args,
                        '--cache-capacity-mib', '64', '--db-capacity-gib', '2']
if a.mode == 'debug':
    cases = [('debug-writeback', unit('debug','writeback_set_test'), 180),
             ('debug-quick', unit('debug','splinterdb_quick_test'), 300),
             ('debug-core-propagation', unit('debug','splinter_test','test_cache_read_errors_reach_core_operations'), 240),
             ('debug-prefetch', driver('cache_test','--prefetch-admission'), 180),
             ('debug-async', driver('cache_test','--async'), 300)]
elif a.mode == 'release':
    cases = [('release-writeback', unit('release','writeback_set_test'), 180),
             ('release-quick', unit('release','splinterdb_quick_test'), 240),
             ('release-core-propagation', unit('release','splinter_test','test_cache_read_errors_reach_core_operations'), 240)]
elif a.mode == 'asan':
    cases = [('asan-writeback', unit('debug-asan','writeback_set_test'), 180)]
else:
    mode = 'release' if a.mode == 'build-release' else 'debug-asan'
    suites = ['writeback_set_test', 'splinterdb_quick_test', 'splinter_test'] if mode == 'release' else ['writeback_set_test']
    command = ['make', '-j4', 'BUILD_MODE=release' if mode == 'release' else 'BUILD_MODE=debug']
    if mode == 'debug-asan':
        command += ['BUILD_ASAN=1']
    command += ['build/' + mode + '/bin/unit/' + suite for suite in suites]
    record = dict(name=a.mode, command=command, cwd=str(TREE), started=time.time())
    print('Building', a.mode, flush=True)
    with (OUT / (a.mode + '.log')).open('w') as log:
        result = subprocess.run(command, cwd=TREE, stdout=log, stderr=subprocess.STDOUT, timeout=900)
    record.update(returncode=result.returncode, finished=time.time())
    (OUT / (a.mode + '.json')).write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(record), flush=True)
    raise SystemExit(result.returncode)

records = []
for name, command, limit in cases:
    scratch = OUT / ('scratch-' + name)
    scratch.mkdir(exist_ok=True)
    link = scratch / 'build'
    if not link.exists():
        link.symlink_to(BUILD, target_is_directory=True)
    storage = subprocess.check_output(['findmnt','-n','-o','FSTYPE,SOURCE','-T',str(scratch)], text=True).strip()
    assert storage.split()[0] not in ('tmpfs', 'ramfs') and storage.split()[1].startswith('/dev/'), storage
    record = dict(name=name, command=command, cwd=str(scratch), storage=storage,
                  started=time.time(), binary_sha256=digest(command[0]))
    env = dict(os.environ, ASAN_OPTIONS='detect_leaks=0:abort_on_error=1')
    if 'asan' in name:
        record['ASAN_OPTIONS'] = env['ASAN_OPTIONS']
    print('Running', name, flush=True)
    logfile = OUT / (name + '.log')
    with logfile.open('w') as log:
        result = subprocess.run(command, cwd=scratch, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=limit)
    record.update(returncode=result.returncode, finished=time.time(), log_sha256=digest(logfile))
    text = logfile.read_text(errors='replace')
    matches = re.findall(r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)', text)
    if matches:
        n, passed, failed, skipped = map(int, matches[-1])
        record.update(tests=n, passed=passed, failed=failed, skipped=skipped)
    records.append(record)
    (OUT / (a.mode + '-results.json')).write_text(json.dumps(records, indent=2) + '\n')
    print(json.dumps(record), flush=True)
    assert result.returncode == 0, record
    if matches:
        assert n > 0 and passed > 0 and failed == 0, record
    elif '/bin/unit/' in command[0]:
        raise AssertionError('Missing unit test results: ' + name)
