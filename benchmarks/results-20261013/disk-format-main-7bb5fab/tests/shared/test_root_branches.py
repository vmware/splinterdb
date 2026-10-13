#!/usr/bin/env python3
"""Focused merge regression checks; invoke inside run_job.py test."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

project, mode = sys.argv[1:]
campaign = Path(__file__).resolve().parents[2]
tree = campaign / 'worktrees' / project
out = Path(__file__).resolve().parent / project
scratch = out / ('scratch-' + mode)
scratch.mkdir(parents=True, exist_ok=True)
build = tree / 'build-main-7bb5fab'
link = scratch / build.name
if not link.exists():
    link.symlink_to(build, target_is_directory=True)
base = build / mode / 'bin'
commands = []
unit = lambda name: str(base / 'unit' / (name + '_test'))
if project == 'cache-invalid-addresses':
    commands.append(('writeback', [unit('writeback_set')]))
else:
    commands.append(('merge', [unit('merge')]))
commands.append(('quick', [unit('splinterdb_quick')]))
core_options = ['--db-location', str(scratch / 'core.db'),
                '--cache-capacity-mib', '128', '--db-capacity-gib', '64']
commands += [('cache-core', [unit('splinter'), *core_options, 'test_cache_read_errors_']),
             ('blob-core', [unit('splinter'), *core_options, 'test_blob_'])]
if project == 'cache-invalid-addresses' and mode == 'debug':
    for flag in ['prefetch-admission', 'async']:
        commands.append((flag, [str(base / 'driver_test'), 'cache_test', '--' + flag,
                              '--db-location', str(scratch / (flag + '.db')),
                              '--cache-capacity-mib', '32', '--db-capacity-gib', '1']))
if project == 'filtered-ranges':
    commands.append(('filter', [str(base / 'driver_test'), 'filter_test',
                               '--db-location', str(scratch / 'filter.db'),
                               '--cache-capacity-mib', '64', '--db-capacity-gib', '4']))
validation = out / ('validation-' + mode + '.json')
old_records = json.loads(validation.read_text()) if validation.exists() else []
records = []
for name, cmd in commands:
    previous = next((r for r in old_records if r['name'] == name), None)
    if previous and previous['returncode'] == 0 and previous['command'] == cmd:
        records.append(previous)
        continue
    print(project, mode, name, flush=True)
    log = out / (mode + '-' + name + '.log')
    if log.exists():
        log.rename(log.with_suffix('.initial.log'))
        (out / (mode + '-' + name + '.initial.json')).write_text(json.dumps(previous, indent=2) + '\n')
    record = dict(name=name, command=cmd, started=time.time())
    with log.open('w') as stream:
        result = subprocess.run(cmd, cwd=scratch, stdout=stream, stderr=subprocess.STDOUT)
    content = log.read_text()
    record.update(returncode=result.returncode, finished=time.time(),
                  log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
                  summaries=[line for line in content.splitlines() if 'RESULTS:' in line])
    records.append(record)
    validation.write_text(json.dumps(records, indent=2) + '\n')
    if result.returncode:
        raise SystemExit(result.returncode)
    if name not in ['prefetch-admission', 'async', 'filter']:
        assert re.search(r'RESULTS: [1-9][0-9]* tests', content), (name, content)
