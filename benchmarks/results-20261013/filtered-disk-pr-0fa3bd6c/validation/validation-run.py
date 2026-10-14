#!/usr/bin/env python3
"""Combined predicate/disk-format validation; invoke under run_job.py."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

OUT = Path(__file__).resolve().parent
ROOT = Path('/home/rob/projects/splinterdb')
TREE = ROOT / '.cache/projects-20261011/worktrees/filtered-ranges'
BUILD = 'build-filtered-disk-pr'
phase, mode, *selected = sys.argv[1:]
base = TREE / BUILD / mode / 'bin'
scratch = ROOT / '.cache/projects-20261011/benchmarks/filtered-disk-pr-20261013/validation' / mode
scratch.mkdir(parents=True, exist_ok=True)
records = []

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def run(name, command, cwd=TREE, timeout=900, unit=False):
    if phase == 'test':
        cwd = scratch / name
        cwd.mkdir(exist_ok=True)
        link = cwd / BUILD
        if not link.exists():
            link.symlink_to(TREE / BUILD, target_is_directory=True)
    path = OUT / f'validation-{mode}-{name}.log'
    record = dict(name=name, command=list(map(str, command)), cwd=str(cwd), started=time.time())
    env = dict(os.environ)
    if mode.endswith('-asan'):
        env['ASAN_OPTIONS'] = 'detect_leaks=0:abort_on_error=1'
        record['ASAN_OPTIONS'] = env['ASAN_OPTIONS']
    print(mode, name, 'starting', flush=True)
    with path.open('w') as log:
        try:
            result = subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT,
                                    timeout=timeout, env=env)
            record['returncode'] = result.returncode
        except subprocess.TimeoutExpired:
            record['returncode'] = 124
    text = path.read_text(errors='replace')
    record.update(finished=time.time(), log_sha256=sha(path),
                  summaries=re.findall(r'RESULTS: [^\r\n]+', text))
    record['passed'] = record['returncode'] == 0
    if unit:
        counts = [tuple(map(int, result)) for result in re.findall(
            r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)', text)]
        record['counts'] = counts
        record['passed'] &= bool(counts) and all(n == ok and n > 0 and failed == skipped == 0
                                              for n, ok, failed, skipped in counts)
    if phase == 'test' and name == 'filter':
        record['basic_cases'] = text.count('routing filter basic test: false positive rate')
        record['saved_seed_regression'] = 'six saved seeds, seven hash calls' in text
        record['passed'] &= record['basic_cases'] == 4 and record['saved_seed_regression']
    records.append(record)
    suffix = '-'.join(selected) if selected else phase
    (OUT / f'validation-{mode}-{suffix}.json').write_text(json.dumps(records, indent=2) + '\n')
    print(mode, name, record['returncode'], record['summaries'], flush=True)
    if not record['passed']:
        print(text[-12000:], flush=True)
        raise SystemExit(record['returncode'] or 1)

units = ['merge_test', 'splinterdb_quick_test', 'splinter_test', 'superblock_test',
         'trunk_async_test', 'btree_test']
if phase == 'build':
    targets = ['libs']
    chosen = ['merge_test', 'splinterdb_quick_test'] if mode.endswith('-asan') else (selected or units)
    targets += [f'{BUILD}/{mode}/bin/unit/{name}' for name in chosen]
    if not mode.endswith('-asan'):
        targets += [f'{BUILD}/{mode}/bin/driver_test']
    command = ['make', '--jobserver-style=pipe', '-j4', f'BUILD_ROOT={BUILD}',
               'BUILD_MODE=' + mode.removesuffix('-asan'), 'CC=gcc', 'LD=gcc']
    if mode.endswith('-asan'):
        command += ['BUILD_ASAN=1']
    run('build', command + targets)
    sources = subprocess.check_output(['git', 'ls-files', 'src', 'include', 'tests', 'Makefile'],
                                      cwd=TREE, text=True).splitlines()
    provenance = dict(head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=TREE, text=True).strip(),
                      source_sha256={name: sha(TREE / name) for name in sources},
                      binaries={str(file.relative_to(TREE)): sha(file) for file in base.rglob('*') if file.is_file()},
                      build_configs={str(file.relative_to(TREE)): file.read_text()
                                     for file in (TREE / BUILD / mode).glob('build-config.*')})
    (OUT / f'validation-{mode}-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
elif phase == 'test':
    fs = subprocess.check_output(['findmnt', '-n', '-o', 'FSTYPE,SOURCE', '-T', str(scratch)], text=True).strip()
    assert fs.split()[0] not in ('tmpfs', 'ramfs') and fs.split()[1].startswith('/dev/'), fs
    commands = [('merge', [base / 'unit/merge_test'], True),
                ('quick', [base / 'unit/splinterdb_quick_test'] +
                 (['test_filtered_range'] if mode.endswith('-asan') else []), True)]
    if not mode.endswith('-asan'):
        commands += [
            ('splinter', [base / 'unit/splinter_test', '--db-location', scratch / 'core.db',
                          '--cache-capacity-mib', '128', '--db-capacity-gib', '64'], True),
            ('superblock', [base / 'unit/superblock_test'], True),
            ('trunk-async', [base / 'unit/trunk_async_test'], True),
            ('btree', [base / 'unit/btree_test'], True),
            ('filter', [base / 'driver_test', 'filter_test', '--db-location', scratch / 'filter.db',
                        '--cache-capacity-mib', '64', '--db-capacity-gib', '4'], False),
            ('log', [base / 'driver_test', 'log_test', '--db-location', scratch / 'log.db',
                     '--cache-capacity-mib', '64', '--db-capacity-gib', '4', '--num-inserts', '10000'], False),
        ]
    for name, command, unit in commands:
        if not selected or name in selected:
            run(name, command, unit=unit)
else:
    raise SystemExit('phase must be build or test')
