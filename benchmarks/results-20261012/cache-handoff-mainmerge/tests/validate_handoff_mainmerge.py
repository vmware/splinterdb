#!/usr/bin/env python3
"""Run merge correctness checks in separate real-disk scratch directories."""
import argparse, json, os, re, subprocess, time
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
TREE = ROOT / 'worktrees/cache-handoff'
OUT = ROOT / 'validation/cache-handoff-mainmerge'
parser = argparse.ArgumentParser()
parser.add_argument('mode', choices=['debug', 'modes', 'drivers'])
a = parser.parse_args()
B = TREE / 'build-mainmerge'
unit = lambda mode, suite, *args: [str(B / mode / 'bin/unit' / suite), *args]
driver = lambda *args: [str(B / 'debug/bin/driver_test'), *args, '--cache-capacity-mib', '64', '--db-capacity-gib', '2']
if a.mode == 'debug':
    cases = [('unit', [str(B / 'debug/bin/unit_test')], 600),
             ('splinter', unit('debug','splinter_test'), 360),
             ('prefetch', driver('cache_test','--prefetch-admission'), 180),
             ('async', driver('cache_test','--async'), 240),
             ('btree-functional', driver('btree_test'), 240),
             ('log-functional', driver('log_test'), 240),
             ('functionality', driver('splinter_test','--functionality','100000','100'), 180)]
elif a.mode == 'drivers':
    cases = [('btree-functional-512m', [str(B / 'debug/bin/driver_test'), 'btree_test', '--cache-capacity-mib', '512', '--db-capacity-gib', '2'], 360),
             ('log-functional', driver('log_test'), 240),
             ('functionality', driver('splinter_test','--functionality','100000','100'), 180)]
else:
    cases = [('release-writeback', unit('release','writeback_set_test'), 180),
             ('release-quick', unit('release','splinterdb_quick_test'), 240),
             ('asan-writeback', unit('debug-asan','writeback_set_test'), 180),
             ('asan-quick', unit('debug-asan','splinterdb_quick_test'), 240),
             ('asan-blob', unit('debug-asan','splinter_test','test_blob_'), 240)]
records = []
OUT.mkdir(parents=True, exist_ok=True)
for name, command, limit in cases:
    scratch = OUT / ('scratch-' + name)
    scratch.mkdir(exist_ok=True)
    build_link = scratch / 'build-mainmerge'
    if not build_link.exists():
        build_link.symlink_to(B, target_is_directory=True)
    record = dict(name=name, command=command, cwd=str(scratch), started=time.time())
    print('Running', name, flush=True)
    env = dict(os.environ, ASAN_OPTIONS='detect_leaks=0:abort_on_error=1')
    with (OUT / (name + '.log')).open('w') as log:
        result = subprocess.run(command, cwd=scratch, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=limit)
    record.update(returncode=result.returncode, finished=time.time())
    text = (OUT / (name + '.log')).read_text(errors='replace')
    matches = re.findall(r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)', text)
    if matches:
        n, passed, failed, skipped = map(int, matches[-1])
        record.update(tests=n, passed=passed, failed=failed, skipped=skipped)
        assert n > 0 and passed > 0 and failed == 0, record
    records.append(record)
    (OUT / (a.mode + '-results.json')).write_text(json.dumps(records, indent=2) + '\n')
    print(name, record, flush=True)
    assert result.returncode == 0, record
