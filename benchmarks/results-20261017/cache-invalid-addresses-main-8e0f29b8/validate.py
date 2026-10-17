#!/usr/bin/env python3
"""Serialized integration checks for the authorized active project update."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time

TREE = Path('/home/rob/projects/splinterdb/.cache/projects-20261011/worktrees/cache-invalid-addresses')
MAIN = '8e0f29b89598db1d52aa7090404b475001dbf2eb'
BASE = 'c15f1452b752dd57a505f766b0d2a029348b94e9'
OLD_MAIN = 'fc0bc8f7e39566f14822bb598376458bdf1e28b3'
LOCK = Path('/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/machine.lock')
OUT = TREE / 'benchmarks/results-20261017/cache-invalid-addresses-main-8e0f29b8'
SCRATCH = TREE / '.cache/main-8e0f29b8-validation'
BUILD = 'build-main-8e0f29b8'
OUT.mkdir(parents=True, exist_ok=False)
SCRATCH.mkdir(parents=True, exist_ok=False)
shutil.copy2(__file__, OUT / 'validate.py')

def git(*args):
    return subprocess.check_output(['git', *args], cwd=TREE)

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def sources():
    paths = [TREE / 'Makefile', TREE / 'format-check.sh']
    for directory in ['src', 'include', 'tests']:
        paths += [p for p in (TREE / directory).rglob('*') if p.suffix in ['.c', '.h']]
    return {str(p.relative_to(TREE)): digest(p) for p in sorted(paths)}

def patch_id(patch):
    return subprocess.check_output(['git', 'patch-id', '--stable'], input=patch, cwd=TREE).decode().split()[0]

extension = ['src/cache.h', 'src/clockcache.c', 'tests/unit/writeback_set_test.c']
before_patch = git('diff', OLD_MAIN, BASE, '--', *extension)
after_patch = git('diff', MAIN, '--', *extension)
assert patch_id(before_patch) == patch_id(after_patch)
(OUT / 'invalid-address-extension.patch').write_bytes(after_patch)
manifest = {
    'base_commit': BASE, 'merged_main': MAIN,
    'merge_conflicts': [],
    'extension_patch_id_before': patch_id(before_patch),
    'extension_patch_id_after': patch_id(after_patch),
    'source_sha256_before': sources(),
    'storage': subprocess.check_output(['findmnt', '-T', str(SCRATCH), '-no', 'SOURCE,FSTYPE,TARGET']).decode().strip(),
    'machine_lock': str(LOCK),
    'commands': [],
    'performance_scope': 'Integration validation only; prior benchmark claims remain version-specific.',
}
assert 'tmpfs' not in manifest['storage']
assert git('rev-parse', 'HEAD').decode().strip() == BASE
assert git('rev-parse', 'MERGE_HEAD').decode().strip() == MAIN

def save():
    (OUT / 'validation.json').write_text(json.dumps(manifest, indent=2) + '\n')

def run(label, command, *, mode=None, unit=False, timeout=600):
    record = {'label': label, 'command': command, 'requested_at': time.time()}
    manifest['commands'].append(record)
    save()
    print('WAIT', label, flush=True)
    with LOCK.open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if shutil.disk_usage(TREE).free < 4 * 1024**3:
            raise RuntimeError('Less than 4 GiB free; stopping before the next job.')
        env = os.environ.copy()
        for key in ['MAKEFLAGS', 'MFLAGS', 'MAKELEVEL', 'CFLAGS', 'LDFLAGS', 'CPPFLAGS', 'CC', 'LD', 'BUILD_ROOT', 'BUILD_MODE', 'BUILD_ASAN', 'BUILD_MSAN', 'BUILD_MEMORY_FAULT_INJECTION']:
            env.pop(key, None)
        temp = SCRATCH / ('tmp-' + label)
        temp.mkdir()
        env['TMPDIR'] = str(temp)
        env['MSAN_OPTIONS'] = 'halt_on_error=1:exit_code=86'
        cwd = TREE
        if mode is not None:
            cwd = SCRATCH / ('db-' + label)
            cwd.mkdir()
            (cwd / BUILD).symlink_to(TREE / BUILD, target_is_directory=True)
            env['LD_LIBRARY_PATH'] = str(TREE / BUILD / mode / 'lib')
        record['cwd'] = str(cwd)
        record['tmpdir'] = str(temp)
        record['started_at'] = time.time()
        if mode is not None:
            record['binary_sha256'] = digest(Path(command[0]))
        save()
        print('START', label, flush=True)
        with (OUT / (label + '.log')).open('w') as log:
            proc = subprocess.Popen(command, cwd=cwd, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                record['returncode'] = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                record['timeout'] = True
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
                record['returncode'] = proc.returncode
        record['finished_at'] = time.time()
        log_text = (OUT / (label + '.log')).read_text(errors='replace')
        counts = re.findall(r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)', log_text)
        if counts:
            record.update(zip(['tests', 'passed', 'failed', 'skipped'], map(int, counts[-1])))
        save()
        print('END', label, record['returncode'], record.get('passed'), flush=True)
        assert record['returncode'] == 0, record
        if unit:
            assert record.get('passed', 0) > 0 and record.get('failed') == 0, record
        if mode is not None:
            shutil.rmtree(cwd)
        shutil.rmtree(temp)

try:
    for mode in ['debug', 'release', 'release-msan']:
        make_args = ['make', '-j6', 'BUILD_ROOT=' + BUILD, 'BUILD_MODE=' + ('release' if mode == 'release-msan' else mode)]
        if mode == 'release-msan':
            make_args += ['CC=clang-19', 'LD=clang-19', 'BUILD_MSAN=1']
        targets = ['libs', 'unit/writeback_set_test', 'unit/splinterdb_quick_test', BUILD + '/' + mode + '/bin/unit/platform_apis_test']
        if mode != 'release-msan':
            targets += ['unit/btree_test', 'unit/splinter_test']
        if mode == 'debug':
            targets += [BUILD + '/' + mode + '/bin/driver_test']
        run(mode + '-build', make_args + targets, timeout=1200)
        binary = TREE / BUILD / mode / 'bin'
        cases = [('writeback', [str(binary / 'unit/writeback_set_test')]),
                 ('quick', [str(binary / 'unit/splinterdb_quick_test')]),
                 ('platform', [str(binary / 'unit/platform_apis_test')])]
        if mode != 'release-msan':
            cases += [('btree', [str(binary / 'unit/btree_test')])]
            for name in ['test_cache_read_errors_reach_core_operations', 'test_invalid_branch_reference_rejected', 'test_persisted_old_branch_version_rejected']:
                cases += [(name, [str(binary / 'unit/splinter_test'), name])]
        for label, command in cases:
            run(mode + '-' + label, command, mode=mode, unit=True)
        if mode == 'debug':
            run('debug-prefetch-admission', [str(binary / 'driver_test'), 'cache_test', '--prefetch-admission', '--cache-capacity-mib', '64', '--db-capacity-gib', '2'], mode=mode)
    run('format', ['bash', 'format-check.sh'])
    run('whitespace', ['git', 'diff', '--check'])
    run('staged-whitespace', ['git', 'diff', '--cached', '--check'])
    manifest['source_sha256_after'] = sources()
    assert manifest['source_sha256_before'] == manifest['source_sha256_after']
    manifest['source_files_unchanged'] = True
    manifest['passed'] = True
    manifest['completed_at'] = time.time()
    save()
    print('VALIDATION COMPLETE', sum(x.get('passed', 0) for x in manifest['commands']), flush=True)
except BaseException as error:
    manifest['error'] = repr(error)
    manifest['passed'] = False
    save()
    raise
