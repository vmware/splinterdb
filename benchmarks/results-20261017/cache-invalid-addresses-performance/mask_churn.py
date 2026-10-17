#!/usr/bin/env python3
"""Repeat the bounded real-I/O admission smoke for the exact async-mask source."""
import csv
import fcntl
import hashlib
import json
from pathlib import Path

import job

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / 'mask-churn-validation.json'


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    assert not OUTPUT.exists()
    previous = json.loads((ROOT / 'churn-validation.json').read_text())
    validation = json.loads((ROOT / 'validation-async-mask/validation.json').read_text())
    assert validation['passed'] and validation['sources_unchanged']
    library = ROOT / 'builds/async-mask/release/lib/libsplinterdb.so'
    assert digest(library) == validation['libraries']['release']
    result = dict(scope=previous['scope'], profile='async-mask',
                  driver_sha256=previous['driver_sha256'],
                  library_sha256=digest(library), runs=[])
    print('WAIT churn machine.lock', flush=True)
    with job.LOCK.open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for old in previous['runs']:
            name = old['name'].replace('async-load', 'async-mask')
            scratch = ROOT / 'scratch' / name
            scratch.mkdir(exist_ok=False)
            command = [word.replace('async-load', 'async-mask') for word in old['command']]
            assert digest(command[0]) == result['driver_sha256']
            env = {key: value.replace('async-load', 'async-mask')
                   for key, value in old['environment_overrides'].items()}
            record = job.run(name, command, scratch, env_extra=env,
                             timeout=180, lock_held=True)
            log = (ROOT / 'jobs' / name / 'output.log').read_text()
            lines = log.splitlines()
            fields = next(csv.reader([next(line for line in lines if line.startswith('mode,api,'))]))
            values = next(csv.reader([next(line for line in lines if line.startswith('churn,async,'))]))
            row = dict(zip(fields, values))
            assert len(fields) == len(values)
            assert row['mode'] == 'churn' and row['api'] == 'async'
            assert int(row['errors']) == int(row['write_bytes']) == int(row['major_faults']) == 0
            assert int(row['read_bytes']) > 0 and int(row['async_waits']) > 0 and int(row['ops']) > 0
            assert int(row['working_pages']) == 32768 and int(row['cache_mib']) == 64
            record['row'] = row
            result['runs'].append(record)
            OUTPUT.write_text(json.dumps(result, indent=2) + '\n')
            Path(command[1]).unlink()
        result['passed'] = True
        OUTPUT.write_text(json.dumps(result, indent=2) + '\n')
    print('CHURN VALIDATION COMPLETE', len(result['runs']), flush=True)


if __name__ == '__main__':
    main()
