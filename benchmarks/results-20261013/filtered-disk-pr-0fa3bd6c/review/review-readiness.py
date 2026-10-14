#!/usr/bin/env python3
"""Read-only evidence audit; invoke only after all benchmark jobs have finished.

This never builds or executes SplinterDB/tests/benchmarks. It runs read-only git
and findmnt commands, hashes artifacts, parses raw evidence, and writes one audit
JSON. It verifies evidence integrity and scope, not statistical significance.
"""
import argparse
import csv
import hashlib
import io
import json
import math
import re
import subprocess
import time
from collections import Counter
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path('/home/rob/projects/splinterdb'))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    repo = args.repo.resolve()
    campaign = repo / '.cache/projects-20261011'
    coord = campaign / 'coordination/filtered-disk-pr-20261013'
    bench = coord / 'benchmarks'
    results = campaign / 'benchmarks/filtered-disk-pr-20261013'
    tree = campaign / 'worktrees/filtered-ranges'
    output = args.output or coord / 'review-readiness.json'
    checks, artifacts, facts = [], {}, {}
    hash_cache = {}

    def check(name, passed, detail=None):
        item = {'check': name, 'passed': bool(passed)}
        if detail is not None:
            item['detail'] = detail
        checks.append(item)

    def sha(path):
        path = Path(path)
        key = (str(path), path.stat().st_size, path.stat().st_mtime_ns)
        if key not in hash_cache:
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            hash_cache[key] = digest.hexdigest()
        artifacts[str(path)] = hash_cache[key]
        return hash_cache[key]

    def read_json(path):
        sha(path)
        return json.loads(Path(path).read_text())

    def git(*command):
        return subprocess.check_output(['git', '-C', str(tree), *command], text=True).strip()

    def digest_matches(name, path, expected):
        path = Path(path)
        check(name + ': exists', path.is_file(), str(path))
        if path.is_file():
            check(name + ': sha256', sha(path) == expected, str(path))

    def filesystem(path):
        description = subprocess.check_output(
            ['findmnt', '-n', '-o', 'FSTYPE,SOURCE', '-T', str(path)], text=True
        ).strip()
        fields = description.split()
        return description, len(fields) >= 2 and fields[0] not in ('tmpfs', 'ramfs') and fields[1].startswith('/dev/')

    def perf_values(path, include_kernel=False):
        sha(path)
        values = {}
        for fields in csv.reader(Path(path).read_text().splitlines()):
            if not fields or fields[0].startswith('#'):
                continue
            check(str(path) + ': counter row', len(fields) >= 5, fields)
            if len(fields) < 5:
                continue
            try:
                value, scheduled = float(fields[0]), float(fields[4])
            except ValueError:
                check(str(path) + ': numeric counter', False, fields)
                continue
            check(str(path) + ': counter scheduled ' + fields[2], scheduled >= 99.0, scheduled)
            check(str(path) + ': finite counter ' + fields[2], math.isfinite(value) and value >= 0)
            values[fields[2]] = value
        expected = ['cpu_core/cycles/u', 'cpu_core/ref-cycles/u', 'cpu_core/instructions/u',
                    'cpu_core/branches/u', 'cpu_core/branch-misses/u', 'task-clock',
                    'context-switches', 'cpu-migrations']
        if include_kernel:
            expected += ['cpu_core/cycles/k', 'cpu_core/instructions/k']
        check(str(path) + ': complete counter set', set(values) == set(expected))
        for key in expected[:4] + ['task-clock']:
            check(str(path) + ': positive ' + key, values.get(key, 0) > 0)
        return values

    def feature_checksum(rows, queries, limit):
        # Independent fixed-count reproduction of the documented query stream.
        seed, checksum = 42, 0
        for query in range(queries):
            seed = (seed * 6364136223846793005 + 1442695040888963407) % (1 << 64)
            start = (seed >> 32) % (rows - limit)
            checksum += sum(start + offset + query % 32 for offset in range(limit))
        return checksum

    try:
        facts['head'] = git('rev-parse', 'HEAD')
        for ancestor in ['origin/main', '7a0c8171', 'd9893674', '36183807']:
            resolved = git('rev-parse', ancestor)
            rc = subprocess.run(['git', '-C', str(tree), 'merge-base', '--is-ancestor', resolved, 'HEAD']).returncode
            check('required ancestor ' + ancestor, rc == 0, resolved)
        runtime_paths = ['src', 'include', 'tests', 'Makefile', '.github']
        difference = git('diff', '36183807', '--', *runtime_paths)
        check('runtime/test/build/CI source identical to validated integration', not difference,
              {'reference': '36183807', 'paths': runtime_paths})
        check('no staged runtime changes', not git('diff', '--cached', '--', *runtime_paths))
        check('no untracked runtime sources', not git('ls-files', '--others', '--exclude-standard', '--', *runtime_paths))
        facts['tree_tracked_status'] = git('status', '--short', '--untracked-files=no')

        build_records = {}
        harnesses = {}
        for name in ['main', 'disk-format', 'combined']:
            record = read_json(bench / (name + '-build.json'))
            build_records[name] = record
            source_root = Path(record['tree'])
            digest_matches(name + ' library', record['library'], record['library_sha256'])
            recorded_sources = record['source_files_sha256']
            tracked_sources = subprocess.check_output(
                ['git', '-C', str(source_root), 'ls-files', 'src', 'include'], text=True
            ).splitlines()
            check(name + ' complete source inventory', set(recorded_sources) == set(tracked_sources))
            for relative, digest in recorded_sources.items():
                digest_matches(name + ' source ' + relative, source_root / relative, digest)
            for binary_name, binary in record['binaries'].items():
                digest_matches(name + ' ' + binary_name, binary['binary'], binary['binary_sha256'])
                digest_matches(name + ' harness ' + binary_name, binary['source'], binary['source_sha256'])
                harnesses.setdefault(binary_name, set()).add(binary['source_sha256'])
            check(name + ' benchmark optimization flags', all(
                flag in record['binaries']['kvbench_controlled']['command']
                for flag in ['-O3', '-flto', '-march=native']))
        check('identical controlled harness across profiles', len(harnesses['kvbench_controlled']) == 1)
        digest_matches('frozen controlled harness', bench / 'sources/kvbench_controlled.c',
                       '59041437fcdc9267d850c33749738c1211af0017a39513363b657dcc4569ee38')
        runner_hashes = read_json(bench / 'runner-source-sha256.json')
        for relative, digest in runner_hashes.items():
            digest_matches('benchmark runner ' + relative, bench / relative, digest)

        completed_jobs = [json.loads(line) for line in (campaign / 'coordination/jobs.jsonl').read_text().splitlines() if line.strip()]
        sha(campaign / 'coordination/jobs.jsonl')
        completed_jobs = [job for job in completed_jobs if 'started' in job and 'finished' in job]
        overlaps = []
        for index, left in enumerate(completed_jobs):
            for right in completed_jobs[index + 1:]:
                if 'benchmark' not in (left.get('mode'), right.get('mode')):
                    continue
                if min(left['finished'], right['finished']) > max(left['started'], right['started']):
                    overlaps.append([left['name'], right['name']])
        check('no completed benchmark overlaps any completed campaign job', not overlaps, overlaps)
        facts['completed_jobs'] = len(completed_jobs)
        facts['completed_benchmark_jobs'] = sum(job['mode'] == 'benchmark' for job in completed_jobs)

        phases = ['point:uniform@8', 'range:uniform:1@8', 'range:uniform:10000@8']
        order = ['main', 'disk-format', 'combined', 'combined', 'disk-format', 'main']
        all_runs = []
        fixture_sets = {}
        expected_checksums = {limit: feature_checksum(16384, 10000, limit) for limit in [1, 2, 10, 100]}
        modes = ['smoke', 'resident', 'disk', 'repeat-disk-range1', 'sampled-disk-range1', 'repeat-disk-range10000', 'feature-v2']
        process_migrations = {}
        for mode in modes:
            focused = mode in ('repeat-disk-range1', 'sampled-disk-range1', 'repeat-disk-range10000')
            feature = mode == 'feature-v2'
            folder = results / mode
            manifest = read_json(folder / 'manifest.json')
            check(mode + ' complete', manifest.get('complete') is True)
            storage = manifest['storage'].split()
            check(mode + ' recorded real disk', len(storage) >= 2 and storage[0] not in ('tmpfs', 'ramfs') and storage[1].startswith('/dev/'), manifest['storage'])
            live_storage, real_disk = filesystem(folder)
            check(mode + ' current real disk', real_disk, live_storage)
            check(mode + ' O_DIRECT', manifest.get('io') == 'O_DIRECT')
            digest_matches(mode + ' preserved runner', folder / 'runner.py', manifest['runner_sha256'])
            # Earlier runners are intentionally retained: later targeted runs
            # add kernel counters/profiling without rebuilding the binaries.
            required_runs = {'smoke': 2, 'resident': 18, 'disk': 18,
                             'repeat-disk-range1': 6, 'sampled-disk-range1': 4,
                             'repeat-disk-range10000': 6, 'feature-v2': 12}[mode]
            check(mode + ' expected run count', len(manifest['runs']) == required_runs)
            if focused:
                expected_order = ['disk-format', 'combined', 'combined', 'disk-format']
                if mode in ('repeat-disk-range1', 'repeat-disk-range10000'):
                    expected_order += ['disk-format', 'combined']
                check(mode + ' expected measured duration', manifest.get('seconds') == (15 if mode == 'repeat-disk-range10000' else 12))
            else:
                expected_order = ['combined'] * 12 if feature else ['disk-format', 'combined'] if mode == 'smoke' else order * 3
            check(mode + ' profile order', [r['profile'] for r in manifest['runs']] == expected_order)
            mode_argument = 'feature' if feature else mode
            mode_jobs = [j for j in completed_jobs if j['mode'] == 'benchmark' and j.get('db_dir') == str(results) and j.get('command', [])[-1:] == [mode_argument] and j.get('returncode') == 0]
            check(mode + ' completed exclusive wrapper', bool(mode_jobs))
            for run in manifest['runs']:
                label = mode + '/' + run['label']
                location = folder / run['label']
                all_runs.append(run)
                check(label + ' passed', run.get('verified') is True and run.get('returncode') == 0)
                check(label + ' inside wrapper interval', any(j['started'] <= run['started'] <= run['finished'] <= j['finished'] for j in mode_jobs))
                build_record = build_records[run['profile']]
                binary_name = 'hash_range_benchmark' if feature else 'kvbench_controlled'
                check(label + ' binary identity', run['binary_sha256'] == build_record['binaries'][binary_name]['binary_sha256'])
                check(label + ' library identity', run['library_sha256'] == build_record['library_sha256'])
                digest_matches(label + ' build manifest', run['build_manifest'], run['build_manifest_sha256'])
                command = run['command']
                check(label + ' delayed perf enable', command[command.index('-D') + 1] == '-1' and '--control' in command)
                environment = run['environment']
                check(label + ' acknowledged phase control', bool(environment.get('KVBENCH_PERF_CONTROL')) and bool(environment.get('KVBENCH_PERF_ACK')))
                values = perf_values(location / 'perf-stat.csv', include_kernel=focused)
                check(label + ' raw counter fidelity', values == run['counters'])
                # The perf process also includes the unpinned coordinator and
                # AIO cleaner. Their migrations do not violate worker pinning.
                process_migrations[label] = values.get('cpu-migrations')
                if mode == 'sampled-disk-range1':
                    digest_matches(label + ' recorded profile', location / 'perf.data', run['perf_data_sha256'])
                    commands = run.get('perf_proxy_commands', [])
                    check(label + ' profile phase boundaries', [c.get('command') for c in commands] == ['enable', 'disable'])
                    for control in commands:
                        check(label + ' both instrument acknowledgements ' + control.get('command', '?'),
                              control['received_mono_ns'] <= control['stat_ack_mono_ns'] <= control['record_ack_mono_ns'])
                    if len(commands) == 2:
                        check(label + ' measured profile duration',
                              11.9 <= (commands[1]['received_mono_ns'] - commands[0]['record_ack_mono_ns']) / 1e9 <= 13)
                    check(label + ' two gated instruments', command.count('--control') == 2 and command.count('-D') == 2)
                log_path = location / 'output.log'
                sha(log_path)
                log = log_path.read_text()
                check(label + ' no close failure', 'FINDING: splinterdb_close' not in log)
                if not feature:
                    sha(location / 'results.csv')
                    rows = list(csv.DictReader(io.StringIO((location / 'results.csv').read_text())))
                    check(label + ' raw CSV fidelity', rows == run['rows'])
                    check(label + ' warm and measured rows', len(rows) == 2 and rows[0]['phase'] == 'warm@1' and rows[1]['phase'] in phases)
                    for row in rows:
                        check(label + ' correct ' + row['phase'], int(row['errors']) == 0 and int(row['violations']) == 0 and int(row['ops']) > 0)
                    count = 2000000 if mode == 'disk' or focused else 200000
                    check(label + ' full validating warm scan', int(rows[0]['ops']) == count)
                    if mode in ('smoke', 'resident'):
                        check(label + ' no timed physical reads', int(rows[1]['rd_bytes']) == 0)
                    recorded_regs = run['registrations']
                    parsed_regs = [list(map(int, values)) for values in re.findall(r'CONTROL worker=(\d+) tid=(\d+) assigned_cpu=(\d+) actual_cpu=(\d+)', log)]
                    check(label + ' raw registration fidelity', parsed_regs == recorded_regs)
                    check(label + ' deterministic registration', recorded_regs == manifest['runs'][0]['registrations'])
                    cpus = [0, 2, 4, 6, 8, 10, 1, 3]
                    check(label + ' fixed worker placement', len(parsed_regs) == 9 and [r[0] for r in parsed_regs] == [0] + list(range(8)) and all(r[2] == cpus[r[0]] == r[3] for r in parsed_regs))
                    check(label + ' serialized worker registration requested', environment.get('KVBENCH_SERIAL_REGISTER') == '1')
                    check(label + ' fixture fsynced', run.get('fixture_copy_fsynced') is True)
                    fixture = run['fixture']
                    digest_matches(label + ' preserved fixture', fixture['fixture'], fixture['fixture_sha256'])
                    fixture_description, fixture_disk = filesystem(fixture['fixture'])
                    check(label + ' fixture real disk', fixture_disk, fixture_description)
                    fixture_sets.setdefault((mode, run['profile']), set()).add(fixture['fixture_sha256'])
                else:
                    lines = log.splitlines()
                    headers = [i for i, line in enumerate(lines) if line.startswith('mode,collisions,')]
                    check(label + ' feature CSV header', len(headers) == 1)
                    # perf's FIFO control acknowledgements share stdout/stderr
                    # with the CSV stream, so admit only genuine CSV data lines.
                    csv_lines = [lines[headers[0]]] + [line for line in lines[headers[0] + 1:]
                        if re.match(r'^(application|predicate|pruned),', line)] if headers else []
                    rows = list(csv.DictReader(csv_lines))
                    check(label + ' raw feature fidelity', rows == run['rows'])
                    check(label + ' exact feature limits', [int(row['limit']) for row in rows] == [1, 2, 10, 100])
                    check(label + ' full feature load', sum(line.startswith('loaded=524288 ') for line in lines) == 1)
                    for row in rows:
                        limit = int(row['limit'])
                        check(label + ' fixed query count ' + str(limit), int(row['queries']) == 10000 and int(row['keys_per_tenant']) == 16384 and int(row['warmup_queries']) == 500)
                        check(label + ' independent checksum ' + str(limit), int(row['checksum']) == expected_checksums[limit])
                        check(label + ' valid latency ' + str(limit), float(row['ns_per_query']) > 0 and float(row['cpu_s']) > 0)
                        if row['mode'] != 'application':
                            check(label + ' exact result count ' + str(limit), int(row['visited']) == limit * 10000)
                        else:
                            check(label + ' application predicate count ' + str(limit), row['visited'] == row['predicate_calls'])
                    check(label + ' no feature correctness failures', run.get('errors') == 0 and run.get('violations') == 0)
                    check(label + ' fixture disk allocation', run.get('allocated_bytes', 0) > 0)
            if feature:
                cases = Counter((int(r['rows'][0]['collisions']), r['rows'][0]['mode']) for r in manifest['runs'])
                check('feature collision/mode matrix', cases == Counter({(c, m): 2 for c in [0, 1] for m in ['application', 'predicate', 'pruned']}))
            elif mode != 'smoke':
                focused_phase = 'range:uniform:10000@8' if mode == 'repeat-disk-range10000' else 'range:uniform:1@8'
                expected_phases = [focused_phase] * required_runs if focused else [p for p in phases for unused in order]
                check(mode + ' phase order', [r['rows'][1]['phase'] for r in manifest['runs']] == expected_phases)
                check(mode + ' identical format8 fixtures', fixture_sets[(mode, 'combined')] == fixture_sets[(mode, 'disk-format')] and len(fixture_sets[(mode, 'combined')]) == 1)

        profile_reports = read_json(bench / 'profile-reports.json')
        sampled = read_json(results / 'sampled-disk-range1/manifest.json')
        check('profile report coverage', {r['label'] for r in profile_reports} == {r['label'] for r in sampled['runs']})
        for profile in profile_reports:
            check(profile['label'] + ' leaf and callgraph reports', len(profile['reports']) == 2)
            for report in profile['reports']:
                digest_matches(profile['label'] + ' report', report['file'], report['sha256'])

        failed_feature = read_json(results / 'feature/manifest.json')
        check('initial feature parser failure retained', not failed_feature.get('complete') and len(failed_feature['runs']) == 1)
        for run in failed_feature['runs']:
            location = results / 'feature' / run['label']
            sha(location / 'output.log')
            sha(location / 'perf-stat.csv')
        facts['initial_feature_attempt'] = {'path': str(results / 'feature'),
            'included_in_performance_comparisons': False,
            'reason': 'Perf enable/disable messages interleaved with CSV; corrected parser and full feature-v2 rerun.'}

        summary = read_json(coord / 'validation-summary.json')
        for section in ['fresh_release', 'fresh_gcc_debug_asan', 'static']:
            check('validation ' + section, summary[section].get('all_passed') is True)
        check('validation source unchanged', summary.get('no_runtime_source_changed') is True)
        check('validation benchmark library matches', summary['release_library_sha256'] == build_records['combined']['library_sha256'])
        identity = read_json(coord / 'validation-source-identity.json')
        for relative, digest in identity['prior_evidence_sha256'].items():
            digest_matches('reused validation ' + relative, Path(identity['prior_archive']) / relative, digest)

        header = (tree / 'include/splinterdb/splinterdb.h').read_text()
        for name in ['splinterdb_iterator_init_with_filter', 'splinterdb_iterator_init_with_bounds_and_filter']:
            declaration = re.search(r'\b' + name + r'\s*\((.*?)\);', header, re.S)
            check(name + ' separate API arguments', bool(declaration) and all(re.search(pattern, declaration.group(1)) for pattern in [r'splinterdb_key_predicate\s+predicate', r'void\s*\*\s*predicate_context', r'slice\s+routing_filter_key']))
        check('no data_config predicate callback', 'key_filter_equal' not in (tree / 'include/splinterdb/data.h').read_text())
        check('header disallows pruning without predicate', 'routing_filter_key without a predicate is invalid' in header)
        check('header all-seeds requirement', 'For EVERY seed' in header)
        doc = (tree / 'docs/hash-filtered-ranges.md').read_text()
        integration = (tree / 'docs/predicate-filter-format-integration.md').read_text()
        check('docs describe current combined implementation', 'PR candidate combines' in doc and 'current `robj/filtered-ranges` PR candidate includes disk-format' in integration)
        check('docs no stale global seed guidance', 'This standalone branch uses the existing global' not in doc)
        check('docs historical evidence labeled', 'Historical standalone predicate validation and performance' in doc and 'Historical single-hash validation' in doc)
        check('docs all possible seeds', 'every possible\nseed' in integration)
        check('docs no data_config callback', 'No `data_config` callback is added' in integration)
        for path in [tree / 'docs/hash-filtered-ranges.md', tree / 'docs/predicate-filter-format-integration.md', tree / 'docs/filtered-ranges-pr.md']:
            sha(path)
        check('documentation/working whitespace', subprocess.run(['git', '-C', str(tree), 'diff', '--check'], capture_output=True).returncode == 0)
        facts['benchmark_processes'] = len(all_runs)
        facts['whole_process_cpu_migrations'] = process_migrations
        facts['scope'] = 'Source/ancestry/evidence integrity and existing validation; no new runtime tests or statistical no-regression assertion.'
    except Exception as error:
        check('audit completed without input/schema error', False, repr(error))

    report = {'timestamp': time.time(), 'script_sha256': sha(__file__), 'facts': facts,
              'all_passed': all(item['passed'] for item in checks),
              'checks': checks, 'artifact_sha256': artifacts}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + '\n')
    failed = [item for item in checks if not item['passed']]
    print(json.dumps({'output': str(output), 'all_passed': not failed, 'checks': len(checks), 'failures': failed}, indent=2))
    return int(bool(failed))


if __name__ == '__main__':
    raise SystemExit(main())
