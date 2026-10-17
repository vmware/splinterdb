#!/usr/bin/env python3
"""Audit completed bounded async-mask churn evidence; never launch workloads."""
import csv
import hashlib
import json
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / 'review/mask-churn-audited.json'
EXPECTED_SOURCE = '7f370079f60ccb1b86744a209c20dd09d0b729d512bdf3e022e04b51d23a777a'
EXPECTED_LIBRARY = 'c7b04dabd8a600500ead9095a04e0a572baf904e256f077043c7191abdd8d228'
EXPECTED_RUNNER = '2df6bd334a8c80ae2cd98f9c7a316242ddc8089033f029b522b830542c34eb45'
EXPECTED_DRIVER = '44e670cb89560c5f226633b6486da992d9cd4c8581806df5a701a3a7a3f38349'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    # Do not manufacture a result while the queue or a partial run remains active.
    raw_path = ROOT / 'mask-churn-validation.json'
    raw = json.loads(raw_path.read_text())
    assert raw.get('passed') is True, 'Churn is not recorded complete'
    assert raw['profile'] == 'async-mask'
    assert len(raw['runs']) == 2
    assert digest(ROOT / 'mask_churn.py') == EXPECTED_RUNNER
    validation_path = ROOT / 'validation-async-mask/validation.json'
    validation = json.loads(validation_path.read_text())
    assert validation['passed'] and validation['sources_unchanged']
    assert validation['passed_unit_executions'] == 124
    assert validation['profile']['clockcache_sha256'] == EXPECTED_SOURCE
    assert validation['source_sha256_before'] == validation['source_sha256_after']
    source = ROOT / 'sources/async-mask'
    assert digest(source / 'src/clockcache.c') == EXPECTED_SOURCE
    for relative, expected in validation['source_sha256_after'].items():
        assert digest(source / relative) == expected, relative
    library = ROOT / 'builds/async-mask/release/lib/libsplinterdb.so'
    assert digest(library) == validation['libraries']['release'] == EXPECTED_LIBRARY
    assert raw['library_sha256'] == EXPECTED_LIBRARY
    assert raw['driver_sha256'] == EXPECTED_DRIVER
    reports = []
    observed_threads = set()
    for run in raw['runs']:
        command = run['command']
        threads = int(command[3])
        assert threads in (1, 6) and threads not in observed_threads
        observed_threads.add(threads)
        name = 'async-mask-churn-' + str(threads)
        assert run['name'] == name and run['returncode'] == 0
        assert command[2:] == ['64', str(threads), '5', 'churn', 'async']
        assert command[0] == str(ROOT / 'harness/controlled-final/cache_api_probe')
        assert command[1] == str(ROOT / 'scratch' / name / 'cache.db')
        assert run['cwd'] == str(ROOT / 'scratch' / name)
        assert digest(Path(command[0])) == EXPECTED_DRIVER
        env = run['environment_overrides']
        expected_cpus = '2' if threads == 1 else '0,2,4,6,8,10'
        assert env['LD_LIBRARY_PATH'] == str(library.parent)
        assert env['CACHE_PROBE_CONTROL_CPU'] == '12'
        assert env['CACHE_PROBE_WORKER_CPUS'] == expected_cpus
        assert env['CACHE_PROBE_WARMUP_SECONDS'] == '2'
        assert env['CACHE_PROBE_PERF'] == '0'
        log_path = ROOT / 'jobs' / name / 'output.log'
        job_path = log_path.with_name('run.json')
        job = json.loads(job_path.read_text())
        for key, value in job.items():
            assert run[key] == value, (name, key)
        assert digest(log_path) == job['log_sha256']
        assert job['queued'] <= job['started'] <= job['finished']
        assert 0 < job['finished'] - job['started'] < 180
        lines = log_path.read_text().splitlines()
        headers = [line for line in lines if line.startswith('mode,api,')]
        values = [line for line in lines if line.startswith('churn,async,')]
        assert len(headers) == len(values) == 1
        fields = next(csv.reader(headers))
        cells = next(csv.reader(values))
        assert len(fields) == len(cells) == len(set(fields))
        row = dict(zip(fields, cells))
        assert row == run['row']
        assert row['mode'] == 'churn' and row['api'] == 'async'
        for key, value in {'cache_mib':64, 'threads':threads, 'working_pages':32768,
                           'warmup_seconds':2, 'control_cpu':12, 'perf_enabled':0,
                           'errors':0, 'write_bytes':0, 'major_faults':0}.items():
            assert int(row[key]) == value, (name, key, row[key])
        for key in ['ops', 'read_bytes', 'async_waits']:
            assert int(row[key]) > 0
        assert math.isfinite(float(row['seconds'])) and 5 <= float(row['seconds']) < 180
        setup = [line for line in lines if line.startswith('O_DIRECT address_layout=')]
        assert len(setup) == 1
        geometry = dict(re.findall(r'(\w+)=([^ ]+)', setup[0]))
        assert geometry['address_layout'] == 'spread'
        for key, value in {'cache_mib':64, 'disk_ratio':16, 'resident_pages':32768,
                           'disk_bytes':128 * 1024**2}.items():
            assert int(geometry[key]) == value, (name, key)
        control = [line for line in lines if line.startswith('CONTROL worker_cpus=')]
        assert len(control) == 1
        settings = dict(re.findall(r'(\w+)=([^ ]+)', control[0]))
        assert settings['worker_cpus'] == expected_cpus
        assert settings['control_cpu'] == '12' and settings['warmup_seconds'] == '2'
        worker_lines = [line for line in lines if line.startswith('WORKER worker=')]
        assert len(worker_lines) == threads
        reports.append({'name':name, 'row':row, 'effective_setup':geometry,
                        'effective_control':settings, 'started':job['started'],
                        'finished':job['finished'], 'log_sha256':digest(log_path),
                        'job_manifest_sha256':digest(job_path), 'returncode':0})
    assert observed_threads == {1, 6}
    intervals = sorted((run['started'],run['finished']) for run in reports)
    assert intervals[0][1] <= intervals[1][0]
    result = {'passed':True,
              'scope':'Read-only post-run audit of bounded real-I/O async miss correctness/progress smoke; not a disk-performance comparison.',
              'auditor_sha256':digest(Path(__file__)),
              'queued_runner_sha256':EXPECTED_RUNNER,
              'raw_manifest_sha256':digest(raw_path),
              'validation_manifest_sha256':digest(validation_path),
              'source_clockcache_sha256':EXPECTED_SOURCE,
              'all_validation_source_hashes_match':True,
              'library_sha256':EXPECTED_LIBRARY,
              'driver_sha256':EXPECTED_DRIVER,
              'exact_threads':[1,6], 'runs':reports,
              'run_intervals_nonoverlapping':True,
              'effective_geometry_from_logs_verified':True,
              'timing_scope':'read_bytes counts the measured interval; async_waits includes warmup and measurement.',
              'identity_limit':'The queued runner verified library before waiting and driver immediately before launch; post-run hashes match, and root reports no intervening source/library mutation. This is not a live process mapping capture.',
              'limitation':'Successful five-second churn and final eviction do not establish general cache admission progress or clear the inherited batch-reservation limitation.'}
    assert not OUTPUT.exists(), str(OUTPUT) + ' already exists'
    OUTPUT.write_text(json.dumps(result,indent=2)+'\n')
    print('CHURN AUDIT PASSED', len(reports), 'runs')


if __name__ == '__main__':
    main()
