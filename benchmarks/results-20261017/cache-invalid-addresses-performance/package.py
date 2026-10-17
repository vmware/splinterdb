#!/usr/bin/env python3
"""Package textual evidence after all campaign work stops; never run benchmarks.

Usage: python3 package.py --selected-profile candidate
The default output is archive/. Existing output is never overwritten. The shared
machine lock is acquired nonblocking and held throughout packaging. Raw evidence
and original absolute commands remain unchanged. No final performance claims are
generated. Unknown/incomplete records remain explicitly unknown.
"""

import argparse
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parent
LOCK = Path('/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/machine.lock')
TEXT_SUFFIXES = {'.py', '.sh', '.c', '.h', '.md', '.json', '.csv', '.log',
                 '.asm', '.patch', '.txt', '.cfg'}
EVIDENCE_DIRS = {'jobs', 'database', 'harness', 'probe-results', 'review', 'codegen'}
PRUNE_DIRS = {'__pycache__', 'tmp', 'scratch', 'bin', 'builds', 'sources', '.git'}
COORDINATORS = {'probe_run.py', 'run_controlled.py', 'validate.py', 'run.py',
                'run-v1.py', 'run-v2.py', 'prepare.py', 'prepare_diagnostics.py',
                'build_probe.py', 'run_quota.py', 'prepare_quota.py'}


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def inventory(tree):
    return {str(p.relative_to(tree)): sha256(p)
            for p in sorted(tree.rglob('*')) if p.is_file()}


def evidence_files():
    """Whitelist evidence trees and text, excluding all data/build directories."""
    for child in sorted(ROOT.iterdir()):
        if child.is_file() and child.suffix in TEXT_SUFFIXES:
            yield child
        elif child.is_dir() and (child.name in EVIDENCE_DIRS
                                 or child.name.startswith('validation-')):
            for directory, dirs, files in os.walk(child, followlinks=False):
                dirs[:] = sorted(d for d in dirs if d not in PRUNE_DIRS
                                 and not d.endswith('-tmp')
                                 and not (Path(directory) / d).is_symlink())
                for name in sorted(files):
                    path = Path(directory) / name
                    if path.is_symlink():
                        raise RuntimeError(f'Unexpected evidence symlink: {path}')
                    if path.suffix in TEXT_SUFFIXES:
                        with path.open('rb') as stream:
                            if b'\0' in stream.read(8192):
                                raise RuntimeError(f'Binary file in text evidence: {path}')
                        yield path


def make_patch(left, right):
    command = ['git', '-c', 'core.quotePath=false', 'diff', '--no-index',
               '--binary', '--no-renames', '--', left, right]
    result = subprocess.run(command, cwd=ROOT / 'sources', capture_output=True)
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.decode(errors='replace'))
    output = []
    for line in result.stdout.decode().splitlines(keepends=True):
        if line.startswith(('diff --git ', '--- ', '+++ ', 'Binary files ')):
            line = line.replace(f'a/{left}/', 'a/').replace(f'b/{right}/', 'b/')
        output.append(line)
    return ''.join(output), command


def source_provenance(out, selected):
    sources = {}
    for source in sorted((ROOT / 'sources').iterdir()):
        if not source.is_dir():
            continue
        hashes = inventory(source)
        metadata_path = ROOT / f'{source.name}-source.json'
        metadata = json.loads(metadata_path.read_text()) if metadata_path.exists() else {}
        expected = metadata.get('hashes', {})
        mismatches = [p for p, digest in expected.items() if hashes.get(p) != digest]
        expected_clockcache = metadata.get('clockcache_sha256')
        if expected_clockcache and hashes.get('src/clockcache.c') != expected_clockcache:
            mismatches.append('src/clockcache.c')
        if mismatches:
            raise RuntimeError(f'Source differs from recorded provenance: {source.name}: {mismatches}')
        source_inventory = f'provenance/source-hashes/{source.name}.json'
        write_json(out / source_inventory, hashes)
        sources[source.name] = {
            'original_absolute_path': str(source),
            'commit': metadata.get('commit'),
            'base_commit': metadata.get('base_commit'),
            'commit_is_variant_identity': bool(metadata.get('commit')),
            'metadata': str(metadata_path.relative_to(ROOT)) if metadata else None,
            'source_inventory': source_inventory,
            'source_inventory_sha256': sha256(out / source_inventory),
            'clockcache_sha256': hashes.get('src/clockcache.c'),
            'file_count': len(hashes),
        }
    if selected not in sources:
        raise RuntimeError(f'Unknown selected profile: {selected}')
    original_commit = sources['original']['commit']
    patches = []
    pairs = [('main', 'original')] + [('original', name) for name in sources
                                             if name not in ('main', 'original')]
    for left, right in pairs:
        text, command = make_patch(left, right)
        path = f'patches/{left}-to-{right}.patch'
        (out / path).parent.mkdir(exist_ok=True)
        (out / path).write_text(text)
        patches.append({'from': left, 'to': right, 'path': path,
                        'sha256': sha256(out / path), 'empty': not bool(text),
                        'generation_command': command,
                        'generation_cwd': str(ROOT / 'sources')})
        sources[right]['patch_from'] = left
        sources[right]['patch'] = path
        if not sources[right]['commit']:
            sources[right]['base_commit'] = sources[right]['base_commit'] or original_commit
    for name, source in sources.items():
        recorded = json.loads((out / source['source_inventory']).read_text())
        if inventory(ROOT / 'sources' / name) != recorded:
            raise RuntimeError(f'Source changed during packaging: {name}')
    libraries = {}
    for library in sorted((ROOT / 'builds').glob('**/lib/libsplinterdb.so')):
        relative = str(library.relative_to(ROOT / 'builds'))
        libraries[relative] = {'original_absolute_path': str(library),
                               'sha256': sha256(library),
                               'bytes': library.stat().st_size,
                               'copied': False}
    result = {'selected_profile': selected, 'baseline_profile': 'main',
              'unoptimized_branch_profile': 'original', 'sources': sources,
              'libraries': libraries, 'patches': patches,
              'identity_note': 'An uncommitted variant has no commit identity. Its '
                               'base commit, normalized patch and complete source '
                               'hash inventory identify the exact source. Library '
                               'hashes describe files present when packaging; raw '
                               'run manifests retain historical measured hashes.'}
    write_json(out / 'provenance/profiles.json', result)
    return result


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def walk_json(value, pointer=''):
    if isinstance(value, dict):
        yield pointer, value
        for key, child in value.items():
            yield from walk_json(child, pointer + '/' + str(key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_json(child, pointer + '/' + str(index))


def workload_audit(out, copied, intentional):
    """Deduplicate repeated records, then compare actual leaf-job intervals."""
    deduplicated, lock_scopes, suites, malformed = {}, [], [], []
    for relative in copied:
        path = out / relative
        if path.suffix != '.json':
            continue
        try:
            document = json.loads(path.read_text())
        except (ValueError, UnicodeError) as error:
            malformed.append({'path': relative, 'error': str(error)})
            continue
        if isinstance(document, dict) and (path.name == 'manifest.json'
                                           or path.name == 'validation.json'):
            suites.append({'path': relative, 'finished': document.get('finished'),
                           'lock_released': document.get('lock_released'),
                           'complete': document.get('complete'),
                           'passed': document.get('passed'),
                           'recorded_runs': len(document.get('runs', [])),
                           'intentional_partial_reason': intentional.get(relative),
                           'finish_record_absent': not any(number(document.get(k))
                               for k in ('finished', 'lock_released'))})
        for pointer, entry in walk_json(document):
            origin = relative + '#' + pointer
            start = entry.get('started')
            command = entry.get('command')
            if number(start) and command:
                key = (start, json.dumps(command, sort_keys=True))
                finish = entry.get('finished')
                if key not in deduplicated:
                    deduplicated[key] = {
                        'name': entry.get('name'), 'command': command,
                        'started': start, 'finished': finish,
                        'returncode': entry.get('returncode'),
                        'origins': [], 'conflicting_records': [],
                        'excluded_precondition': bool(entry.get('excluded_from_results')),
                    }
                record = deduplicated[key]
                record['origins'].append(origin)
                record['excluded_precondition'] |= bool(entry.get('excluded_from_results'))
                for field in ('finished', 'returncode'):
                    value = entry.get(field)
                    if record[field] is None:
                        record[field] = value
                    elif value is not None and value != record[field]:
                        record['conflicting_records'].append(
                            {'origin': origin, 'field': field, 'value': value})
            for start_key, finish_key in [('lock_acquired', 'lock_released'),
                                          ('suite_lock_acquired', 'finished'),
                                          ('acquired', 'released')]:
                if number(entry.get(start_key)):
                    end = entry.get(finish_key)
                    if start_key == 'lock_acquired' and end is None:
                        end = entry.get('finished')
                    lock_scopes.append({'origin': origin, 'started': entry[start_key],
                                        'finished': end, 'kind': 'lock-container'})
    records = sorted(deduplicated.values(), key=lambda r: r['started'])
    for index, record in enumerate(records):
        record['id'] = index
        words = record['command'] if isinstance(record['command'], list) else []
        coordinator = any(Path(word).name in COORDINATORS for word in words
                          if isinstance(word, str))
        contained = [other_index for other_index, other in enumerate(records)
                     if index != other_index and number(record['finished'])
                     and number(other['finished'])
                     and record['started'] <= other['started']
                     and other['finished'] <= record['finished']]
        record['container_children'] = contained if coordinator else []
        record['kind'] = 'coordinator-container' if coordinator and contained else 'leaf-workload'
        names = [Path(word).name for word in words if isinstance(word, str)]
        if 'cache_api_probe' in names:
            record['workload_type'] = 'cache-probe'
        elif any(name in names for name in ('kvbench', 'insert_benchmark',
                                             'insert_benchmark_diagnostics',
                                             'insert_benchmark_quota')):
            record['workload_type'] = 'database'
        elif any(name in names for name in ('make', 'gcc', 'cc', 'clang', 'clang-19')):
            record['workload_type'] = 'build'
        elif any('test' in name for name in names):
            record['workload_type'] = 'test'
        elif any('clang-format' in name for name in names):
            record['workload_type'] = 'format-check'
        else:
            record['workload_type'] = 'other-or-coordinator'
        record['finish_unknown'] = not number(record['finished'])
        record['intentional_partial_reasons'] = sorted({intentional[o.split('#')[0]]
            for o in record['origins'] if o.split('#')[0] in intentional})
    invalid_intervals = [r['id'] for r in records if number(r['finished'])
                         and r['finished'] < r['started']]
    conflicts = [r['id'] for r in records if r['conflicting_records']]
    finite = [r for r in records if r['kind'] == 'leaf-workload'
              and number(r['finished']) and r['id'] not in invalid_intervals]
    overlaps = []
    active = []
    for record in finite:
        active = [previous for previous in active
                  if previous['finished'] > record['started']]
        for previous in active:
            overlaps.append({'left': previous['id'], 'right': record['id'],
                             'seconds': min(previous['finished'], record['finished'])
                                        - record['started']})
        active.append(record)
    missing_runs = []
    for relative in copied:
        path = Path(relative)
        if path.name == 'output.log' and (path.parent / 'run.json').as_posix() not in copied:
            missing_runs.append({'log': relative,
                                 'status': 'no run.json; start and finish unknown',
                                 'restart_partial_name': 'interrupted' in str(path.parent),
                                 'intentional_partial_reason': intentional.get(relative)})
    failed = [r['id'] for r in records if r['returncode'] not in (None, 0)]
    result = {
        'scope': 'Recorded campaign subprocesses only; not unrelated system activity.',
        'method': 'Exact command/start duplicates coalesced across manifests and jobs. '
                  'Known coordinator commands containing recorded children and explicit '
                  'lock scopes excluded from leaf overlap comparison. Precondition, '
                  'failed and interrupted work retained. Unknown endpoints are never '
                  'treated as evidence of non-overlap.',
        'records': records, 'lock_containers': lock_scopes, 'suites': suites,
        'finite_leaf_count': len(finite), 'finite_leaf_overlaps': overlaps,
        'unknown_finish_ids': [r['id'] for r in records if r['finish_unknown']],
        'missing_run_records': missing_runs, 'failed_record_ids': failed,
        'malformed_json': malformed, 'intentional_partial_annotations': intentional,
        'invalid_interval_ids': invalid_intervals, 'conflicting_record_ids': conflicts,
        'finite_recorded_workloads_serial': not (overlaps or invalid_intervals or conflicts or malformed),
        'incomplete_evidence_exists': bool(missing_runs or any(r['finish_unknown'] for r in records)),
    }
    write_json(out / 'audit/workload-intervals.json', result)
    with (out / 'audit/workload-intervals.csv').open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['id', 'name', 'kind', 'started', 'finished', 'returncode',
                         'excluded_precondition', 'origins', 'command'])
        for r in records:
            writer.writerow([r[k] for k in ['id', 'name', 'kind', 'started', 'finished',
                                           'returncode', 'excluded_precondition']]
                            + [json.dumps(r['origins']), json.dumps(r['command'])])
    return result


SETUP_SCRIPT = r'''#!/usr/bin/env python3
"""Reconstruct source exports from local git objects plus archived patches.

Usage: python3 setup_sources.py --repo /path/to/splinterdb --dest /new/path
No network or compilation. Destination must not exist. Original build/run
commands remain in raw manifests; adapt their absolute paths before rerunning.
"""
import argparse, hashlib, io, json, os, shutil, subprocess, tarfile
from pathlib import Path

def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file()}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--dest', type=Path, required=True)
    args = parser.parse_args()
    archive = Path(__file__).resolve().parent
    profiles = json.loads((archive / 'provenance/profiles.json').read_text())
    destination = args.dest.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    for name in ('main', 'original'):
        metadata = json.loads((archive / f'{name}-source.json').read_text())
        data = subprocess.check_output(['git', '-C', str(args.repo.resolve()),
            'archive', metadata['commit'], '--', *metadata['exported_paths']])
        target = destination / name
        target.mkdir()
        with tarfile.open(fileobj=io.BytesIO(data)) as tar:
            for member in tar.getmembers():
                path = Path(member.name)
                if path.is_absolute() or '..' in path.parts or not (member.isfile() or member.isdir()):
                    raise RuntimeError(f'Unsupported/unsafe archive member: {member.name}')
            tar.extractall(target)
    for name, profile in profiles['sources'].items():
        if name not in ('main', 'original'):
            target = destination / name
            shutil.copytree(destination / 'original', target)
            patch = archive / profile['patch']
            if patch.stat().st_size:
                environment = os.environ.copy()
                for key in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR'):
                    environment.pop(key, None)
                environment['GIT_CEILING_DIRECTORIES'] = str(target.parent)
                subprocess.run(['git', 'apply', '--no-index', str(patch)],
                               cwd=target, env=environment, check=True)
        expected = json.loads((archive / profile['source_inventory']).read_text())
        actual = hashes(destination / name)
        if expected != actual:
            raise RuntimeError(f'Reconstructed source hash mismatch: {name}')
    print(f'Verified {len(profiles["sources"])} source profiles in {destination}')

if __name__ == '__main__':
    main()
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--selected-profile', default='candidate')
    parser.add_argument('--output', type=Path, default=ROOT / 'archive')
    parser.add_argument('--intentional-partial', action='append', default=[],
                        metavar='RELATIVE_PATH=REASON',
                        help='Explicitly annotate retained intentionally stopped evidence.')
    args = parser.parse_args()
    intentional = dict(value.split('=', 1) for value in args.intentional_partial)
    output = args.output.resolve()
    if output.exists():
        parser.error(f'Refusing existing archive: {output}')
    if output.parent != ROOT:
        parser.error('Output must be an immediate child of this campaign directory.')
    with LOCK.open('rb') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error('Machine lock is held. Stop/wait for campaign workloads before packaging.')
        files = list(evidence_files())
        selected_paths = {str(path.relative_to(ROOT)) for path in files}
        for path in intentional:
            if path not in selected_paths:
                parser.error(f'Annotation must name existing campaign evidence: {path}')
        output.mkdir()
        copied = []
        for path in files:
            relative = str(path.relative_to(ROOT))
            target = output / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            if sha256(path) != sha256(target):
                raise RuntimeError(f'Evidence changed while copying: {path}')
            copied.append(relative)
        profiles = source_provenance(output, args.selected_profile)
        audit = workload_audit(output, copied, intentional)
        (output / 'setup_sources.py').write_text(SETUP_SCRIPT)
        (output / '.gitattributes').write_text(
            '# Preserve blank context lines in archived unified patches.\n'
            '*.patch -whitespace\n'
            '# Keep the raw CSV writer CRLF bytes and check other whitespace.\n'
            '*.csv -text whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol\n')
        issues = [
            {'evidence': 'jobs/controlled-smoke/output.log',
             'status': 'failed harness smoke; excluded performance evidence',
             'reason': 'v3 worker diagnostics were written before the final I/O snapshot; '
                       'the redirected log caused the measured 4096-byte write rejection. '
                       'Frozen v3b moves diagnostics after the snapshot.',
             'source': 'harness/controlled-v3b/README.md'},
            {'evidence': 'database/results/insert-fixed20m/REJECTED.txt',
             'status': 'rejected suite after excluded precondition; no measured samples',
             'reason': 'Diagnostics parser failed after successful precondition execution. '
                       'The fixed20m-retry directory is the separate retry.'},
            {'evidence': 'jobs/initial-blocking-1-05-main-interrupted/output.log',
             'status': 'restart partial; missing run.json; exact endpoint unknown'},
            {'evidence': 'jobs/build-async-mask/REJECTED.txt',
             'status': 'launcher argument error; no compiler or workload created',
             'reason': 'A positional REMAINDER consumed --cwd as the executable. '
                       'The corrected build-async-mask-retry is recorded separately.'},
            {'evidence': 'jobs/build-cache-write-probe/output.log',
             'status': 'launcher path error; no compiler or probe created',
             'reason': 'A workspace-relative script path was resolved against the '
                       'job runner campaign directory. The absolute-path retry '
                       'build-cache-write-probe-retry succeeded separately.'},
            {'evidence': 'database/results/insert-final-fixed20m/REJECTED.txt',
             'status': 'canceled while queued; no workload launched',
             'reason': 'Its proposed timed precondition could exhaust its record cap. '
                       'The completed retry used a shorter precondition; later quota '
                       'experiments use an independent larger cap.'},
        ]
        write_json(output / 'audit/known-harness-issues.json',
                   [issue for issue in issues if (output / issue['evidence']).exists()])
        write_json(output / 'package-manifest.json', {
            'packaged_at_unix': time.time(), 'campaign_original_path': str(ROOT),
            'selected_profile': args.selected_profile, 'machine_lock': str(LOCK),
            'package_command': sys.argv, 'copied_file_count': len(copied),
            'profiles': 'provenance/profiles.json',
            'workload_audit': 'audit/workload-intervals.json',
            'known_harness_issues': 'audit/known-harness-issues.json',
            'reconstruction': 'setup_sources.py',
            'exclusions': ['database files', 'build directories and compiled libraries',
                           'executables and objects', 'temporary directories', '__pycache__',
                           'scratch directories', 'full source exports'],
            'raw_commands_rewritten': False, 'performance_claims_generated': False,
            'profile_count': len(profiles['sources']),
            'finite_leaf_overlaps': len(audit['finite_leaf_overlaps']),
            'unknown_finish_records': len(audit['unknown_finish_ids']),
            'missing_run_records': len(audit['missing_run_records']),
            'inventory_note': 'SHA256SUMS covers all archived files except itself; '
                              'its own digest is printed by this packaging command.',
        })
        archive_hashes = inventory(output)
        checksum_file = output / 'SHA256SUMS'
        checksum_file.write_text(''.join(f'{digest}  {path}\n'
                                         for path, digest in archive_hashes.items()))
        size = sum(p.stat().st_size for p in output.rglob('*') if p.is_file())
        print(json.dumps({'archive': str(output), 'files': len(archive_hashes) + 1,
                          'bytes': size, 'SHA256SUMS_sha256': sha256(checksum_file),
                          'finite_leaf_overlaps': len(audit['finite_leaf_overlaps']),
                          'missing_run_records': len(audit['missing_run_records'])}, indent=2))


if __name__ == '__main__':
    main()
