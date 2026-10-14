#!/usr/bin/env python3
"""Run under run_job.py's exclusive benchmark lock on verified physical disk."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--workload', choices=['barrier', 'optimize'], required=True)
p.add_argument('--campaign', default='sentinels-20261012')
a = p.parse_args()
root = Path(__file__).resolve().parent.parent
out = root / 'benchmarks/async-durable-barrier' / a.campaign / a.workload
out.mkdir(parents=True, exist_ok=False)
binaries = {
    'old': root / 'coordination/notification-sentinels-baseline/build/release/bin',
    'new': root / 'worktrees/async-durable-barrier/build-sentinels/release/bin',
}
manifest = dict(workload=a.workload, io='O_DIRECT', cpu_affinity='0-11', runs=[])

def run(name, mode, log, workers, count, index):
    binary = binaries[name] / ('barrier_bench' if a.workload == 'barrier'
                               else 'optimize_notification_bench')
    db = out / 'scratch.db'
    assert not db.exists()
    csv_path = out / 'results.csv'
    command = ['taskset', '-c', '0-11', str(binary), str(db), str(csv_path), name]
    if a.workload == 'barrier':
        command += [mode, str(log), str(workers), '256', str(count)]
    else:
        command += [str(workers), str(count)]
    build = json.loads(binary.with_suffix('.json').read_text())
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == build['binary_sha256']
    library = next(Path(arg) for arg in build['command'] if arg.endswith('/libsplinterdb.a'))
    assert hashlib.sha256(library.read_bytes()).hexdigest() == build['library_sha256']
    record = dict(version=name, index=index, command=command,
                  started=time.time(), build=build)
    print(f'{a.workload} {index}: {name} {mode} log={log} workers={workers}', flush=True)
    with (out / f'{index}-{name}.log').open('w') as f:
        result = subprocess.run(command, env=dict(os.environ, BARRIER_DEPTH='1'),
                                stdout=f, stderr=subprocess.STDOUT, timeout=300)
    record.update(returncode=result.returncode, finished=time.time())
    if db.exists():
        record['db_allocated_bytes'] = db.stat().st_blocks * 512
    manifest['runs'].append(record)
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    if result.returncode:
        raise SystemExit(result.returncode)
    with csv_path.open() as f:
        row = list(csv.DictReader(f))[-1]
    assert int(row['verified']) == 1
    # Delete only this successfully verified scratch image; create must start
    # with fresh superblock slots and cannot reuse a previous database file.
    db.unlink()

index = 0
for workers in (0, 1):
    if a.workload == 'barrier':
        for log in (0, 1):
            for mode in ('sync', 'async'):
                for name in ('old', 'new', 'new', 'old'):
                    run(name, mode, log, workers, 2048 if log else 512, index)
                    index += 1
    else:
        for name in ('old', 'new', 'new', 'old'):
            run(name, 'blocking', 0, workers, 20000, index)
            index += 1
print('Completed', out, flush=True)
