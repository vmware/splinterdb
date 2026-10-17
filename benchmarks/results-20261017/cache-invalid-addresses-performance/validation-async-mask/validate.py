#!/usr/bin/env python3
"""Validate the exact async-mask candidate while holding the campaign lock."""
import fcntl
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent
sys.path.insert(0, str(ROOT))
import job

job.ROOT = OUT
SOURCE = ROOT / 'sources/async-mask'
BUILD = ROOT / 'builds/async-mask'
SCRATCH = ROOT / 'scratch/validation-async-mask'
SCRATCH.mkdir(exist_ok=False)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sources():
    paths = [SOURCE / 'Makefile', SOURCE / 'format-check.sh', SOURCE / '.clang-format']
    for directory in ['src', 'include', 'tests', 'examples', 'tools']:
        paths += [p for p in (SOURCE / directory).rglob('*') if p.suffix in ['.c', '.h']]
    return {str(p.relative_to(SOURCE)): digest(p) for p in sorted(paths)}


manifest = {
    'profile': json.loads((ROOT / 'async-mask-source.json').read_text()),
    'source': str(SOURCE),
    'build': str(BUILD),
    'machine_lock': str(job.LOCK),
    'lock_scope': 'One exclusive outer lock for all format/build/test commands; job.run(lock_held=True).',
    'storage': subprocess.check_output(['findmnt', '-T', str(SCRATCH), '-no', 'SOURCE,FSTYPE,TARGET'], text=True).strip(),
    'source_sha256_before': sources(),
    'compiler_versions': {cc: subprocess.check_output([cc, '--version'], text=True).splitlines()[0] for cc in ['cc', 'clang-19']},
    'commands': [],
    'queued': time.time(),
    'expected_unit_executions': 124,
    'validation_scope': 'Prior 118 selected unit executions plus two existing invalid-page-size configuration tests in each mode, and debug prefetch admission.',
    'format_scope': 'All C and header files in exported maintained src/include/tests/examples/tools; benchmark archives are absent from source exports.',
}
assert 'tmpfs' not in manifest['storage']
assert manifest['source_sha256_before']['src/clockcache.c'] == manifest['profile']['clockcache_sha256']
assert all(manifest['profile']['hashes'][path] == value for path, value in manifest['source_sha256_before'].items())


def save():
    (OUT / 'validation.json').write_text(json.dumps(manifest, indent=2) + '\n')


def run(label, command, mode=None, expected_passes=None):
    if shutil.disk_usage(ROOT).free < 4 * 1024**3:
        raise RuntimeError('Less than 4 GiB free before ' + label)
    cwd = SOURCE
    env = None
    if mode:
        cwd = SCRATCH / label
        cwd.mkdir(exist_ok=False)
        env = {'LD_LIBRARY_PATH': str(BUILD / mode / 'lib'),
               'MSAN_OPTIONS': 'halt_on_error=1:exit_code=86'}
    record = job.run(label, command, cwd=cwd, env_extra=env, timeout=1200, lock_held=True)
    if mode:
        record['binary_sha256'] = digest(Path(command[0]))
    log = (OUT / 'jobs' / label / 'output.log').read_text(errors='replace')
    counts = re.findall(r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)', log)
    if counts:
        record.update(zip(['tests', 'passed', 'failed', 'skipped'], map(int, counts[-1])))
    manifest['commands'].append(record)
    save()
    if expected_passes is not None:
        assert record.get('passed') == expected_passes and record.get('failed') == 0, record
    if mode:
        shutil.rmtree(cwd)
        record['disposable_test_directory_removed'] = True
        save()


save()
try:
    print('WAIT outer validation machine lock', flush=True)
    with job.LOCK.open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        manifest['lock_acquired'] = time.time()
        save()
        print('START outer validation machine lock', flush=True)
        format_files = [str(SOURCE / path) for path in manifest['source_sha256_before'] if path.endswith(('.c', '.h'))]
        run('format', ['clang-format-19', '--Werror', '--dry-run'] + format_files)
        for mode in ['release', 'debug', 'release-msan']:
            flags = ['make', '-j6', 'BUILD_ROOT=' + str(BUILD),
                     'BUILD_MODE=' + ('release' if mode == 'release-msan' else mode),
                     'GIT_VERSION=invalid-address-perf-async-mask']
            if mode == 'release-msan':
                flags += ['CC=clang-19', 'LD=clang-19', 'BUILD_MSAN=1']
            else:
                flags += ['CC=cc', 'LD=cc', 'BUILD_MSAN=0']
            targets = ['libs', 'unit/writeback_set_test', 'unit/limitations_test']
            if mode == 'release':
                targets += ['unit/splinterdb_quick_test']
            if mode != 'release-msan':
                targets += ['unit/splinter_test']
            if mode == 'debug':
                targets += [str(BUILD / mode / 'bin/driver_test')]
            run(mode + '-build', flags + targets)
            binary = BUILD / mode / 'bin'
            run(mode + '-writeback', [str(binary / 'unit/writeback_set_test')], mode=mode, expected_passes=18)
            for test in ['test_io_init_invalid_page_size', 'test_splinterdb_create_invalid_page_size']:
                run(mode + '-' + test, [str(binary / 'unit/limitations_test'), test], mode=mode, expected_passes=1)
            if mode == 'release':
                run(mode + '-quick', [str(binary / 'unit/splinterdb_quick_test')], mode=mode, expected_passes=62)
            if mode != 'release-msan':
                run(mode + '-core-read-errors', [str(binary / 'unit/splinter_test'), 'test_cache_read_errors_reach_core_operations'], mode=mode, expected_passes=1)
            if mode == 'debug':
                run('debug-prefetch-admission', [str(binary / 'driver_test'), 'cache_test', '--prefetch-admission', '--cache-capacity-mib', '64', '--db-capacity-gib', '2'], mode=mode)
        manifest['source_sha256_after'] = sources()
        assert manifest['source_sha256_before'] == manifest['source_sha256_after']
        manifest['sources_unchanged'] = True
        manifest['libraries'] = {mode: digest(BUILD / mode / 'lib/libsplinterdb.so') for mode in ['release', 'debug', 'release-msan']}
        manifest['passed_unit_executions'] = sum(r.get('passed', 0) for r in manifest['commands'])
        assert manifest['passed_unit_executions'] == manifest['expected_unit_executions']
        manifest['passed'] = True
        manifest['lock_released'] = time.time()
        save()
        print('VALIDATION COMPLETE', manifest['passed_unit_executions'], flush=True)
except BaseException as error:
    manifest['passed'] = False
    manifest['error'] = repr(error)
    manifest['failed_at'] = time.time()
    save()
    raise
