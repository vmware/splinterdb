#!/usr/bin/env python3
"""Paired O_DIRECT public API runs; invoke through run_job.py benchmark."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--project', required=True)
p.add_argument('--regime', choices=['resident', 'disk'], required=True)
p.add_argument('--seconds', type=int, default=5)
p.add_argument('--log', type=int, default=0)
p.add_argument('--baseline', type=Path)
p.add_argument('--campaign', default='')
p.add_argument('--fixture', type=Path)
p.add_argument('--candidate-binary', type=Path)
p.add_argument('--baseline-binary', type=Path)
p.add_argument('--phases', help='Comma-separated phase override')
a = p.parse_args()
root = Path(__file__).resolve().parent.parent
out = root / 'benchmarks' / a.project / (a.regime + '-log' + str(a.log) + ('-' + a.campaign if a.campaign else ''))
out.mkdir(parents=True, exist_ok=False)
trees = {'baseline': a.baseline or root / 'baseline', 'candidate': root / 'worktrees' / a.project}
phases = ['load:random', 'warm', 'point:uniform@1', 'point:uniform@8']
phases += [f'range:uniform:{length}@{threads}' for threads in [1, 8] for length in [1, 2, 10, 100, 10000]]
if a.phases:
    phases = a.phases.split(',')
manifest = {'project': a.project, 'regime': a.regime, 'io': 'O_DIRECT', 'runs': []}
if a.fixture:
    a.fixture = a.fixture.resolve(strict=True)
    if 'load:random' in phases:
        phases.remove('load:random')
    # Reuse the identical immutable tree in each sample. Concurrent loading can
    # produce different branch shapes, obscuring CPU-only query changes.
    manifest['fixture'] = str(a.fixture)
    manifest['fixture_sha256'] = subprocess.check_output(['sha256sum', str(a.fixture)], text=True).split()[0]
for index, version in enumerate(['baseline', 'candidate', 'candidate', 'baseline']):
    binary = (a.baseline_binary if version == 'baseline' else a.candidate_binary) or trees[version] / 'build/release/bin/kvbench'
    db = out / f'{index}-{version}.db'
    if a.fixture:
        subprocess.run(['cp', '--reflink=auto', '--sparse=always', str(a.fixture), str(db)], check=True)
    argv = ['taskset', '-c', '0-11', str(binary), '--db', str(db), '--open' if a.fixture else '--create',
            '--n', '200000' if a.regime == 'resident' else '2000000',
            '--cache-mib', '128' if a.regime == 'resident' else '64',
            '--disk-gib', '8', '--log', str(a.log), '--threads', '8',
            '--mt-bg', '1', '--normal-bg', '3', '--memtable-mib', '4',
            '--secs', str(a.seconds), '--warmup', '1', '--csv', str(out / 'results.csv'),
            '--vtag', version, '--stag', a.regime, '--otag', str(index),
            '--rep', str(index // 2), '--seed', '42', '--hang-secs', '60',
            '--hang-dir', str(out), *phases]
    record = dict(index=index, version=version, command=argv, started=time.time(),
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    print(f'{a.project} {a.regime}: run {index + 1}/4 {version}', flush=True)
    with (out / f'{index}-{version}.log').open('w') as log:
        result = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT)
    record.update(returncode=result.returncode, finished=time.time())
    if db.exists():
        record.update(db_allocated_bytes=db.stat().st_blocks * 512, db_apparent_bytes=db.stat().st_size)
    manifest['runs'].append(record)
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    if result.returncode:
        raise SystemExit(result.returncode)
with (out / 'results.csv').open() as stream:
    rows = list(csv.DictReader(stream))
for row in rows:
    for name in ['violations', 'errors']:
        if name in row and int(row[name]) != 0:
            raise RuntimeError('Correctness failures: ' + repr(row))
print('Completed', out, flush=True)
