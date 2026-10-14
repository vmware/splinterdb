#!/usr/bin/env python3
"""Bounded P7 disk control: fsync copied fixture before any O_DIRECT access."""
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

root = Path(__file__).resolve().parent.parent
out = root / 'benchmarks/branch-prefix/disk-common-fixed-final-fsynced'
out.mkdir(exist_ok=False)
old = json.loads((root / 'benchmarks/branch-prefix/disk-common-fixed-final/manifest.json').read_text())
profiles = {
    version: next(row for row in old['runs'] if row['version'] == version)
    for version in ['baseline', 'candidate']
}
manifest = dict(io='O_DIRECT', fixture_copy_sync='fsync destination before warm/open/timing',
                note='Frozen final binaries and the same incompatible per-format images as the earlier fixed-final comparison; only point8 and range10000@8 are repeated.',
                runs=[])
for index, version in enumerate(['baseline', 'candidate', 'candidate', 'baseline']):
    profile = profiles[version]
    binary = Path(profile['command'][3])
    source = Path(profile['fixture'])
    source_stat = source.stat()
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    assert digest == profile['binary_sha256']
    db = out / 'scratch.db'
    copy_started = time.time()
    subprocess.run(['cp', '--reflink=auto', '--sparse=always', str(source), str(db)], check=True)
    copied_at = time.time()
    with db.open('rb') as stream:
        os.fsync(stream.fileno())
    synced_at = time.time()
    argv = ['taskset', '-c', '0-11', str(binary), '--db', str(db), '--open',
            '--n', '2000000', '--cache-mib', '64', '--disk-gib', '8', '--log', '0',
            '--threads', '8', '--mt-bg', '0', '--normal-bg', '0', '--memtable-mib', '4',
            '--secs', '7', '--warmup', '1', '--csv', str(out / 'results.csv'),
            '--vtag', version, '--stag', 'disk', '--otag', str(index), '--rep', str(index // 2),
            '--seed', '42', '--hang-secs', '60', '--hang-dir', str(out),
            'warm', 'point:uniform@8', 'range:uniform:10000@8']
    record = dict(index=index, version=version, command=argv, fixture=str(source),
                  fixture_size=source_stat.st_size, fixture_mtime_ns=source_stat.st_mtime_ns,
                  binary_sha256=digest, copy_started=copy_started, copied_at=copied_at,
                  synced_at=synced_at, started=time.time(),
                  fixture_copy_allocated_bytes=db.stat().st_blocks * 512)
    print('Prefix fsynced disk control', index, version, flush=True)
    with (out / f'{index}-{version}.log').open('w') as stream:
        result = subprocess.run(argv, env=dict(os.environ, PREFIX_PATTERN='common'),
                                stdout=stream, stderr=subprocess.STDOUT, timeout=90)
    record.update(returncode=result.returncode, finished=time.time())
    manifest['runs'].append(record)
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    assert result.returncode == 0, record
    rows = list(csv.DictReader((out / 'results.csv').open()))
    assert all(int(row[key]) == 0 for row in rows for key in ['violations', 'errors'])
    assert source.stat().st_mtime_ns == source_stat.st_mtime_ns
    db.unlink()
print('Completed', out, flush=True)
