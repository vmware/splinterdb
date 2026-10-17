#!/usr/bin/env python3
"""Scenario-major pinned cache measurements, optionally with a small hot set."""
import argparse
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from job import LOCK, run


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_text(path):
    try:
        return path.read_text().strip()
    except OSError:
        return None


def snapshot(cpus):
    """Diagnostic before/after-process snapshots; no system settings changed."""
    values = {}
    for cpu in cpus:
        base = Path(f'/sys/devices/system/cpu/cpu{cpu}')
        for part in ('cpufreq/scaling_cur_freq', 'cpufreq/scaling_governor',
                     'cpufreq/scaling_min_freq', 'cpufreq/scaling_max_freq',
                     'thermal_throttle/core_throttle_count',
                     'thermal_throttle/package_throttle_count'):
            path = base / part
            values[str(path)] = read_text(path)
    for hwmon in sorted(Path('/sys/class/hwmon').glob('hwmon*')):
        name = read_text(hwmon / 'name')
        if name not in ('coretemp', 'acpitz'):
            continue
        values[str(hwmon / 'name')] = name
        for path in sorted(hwmon.glob('temp*_input')):
            values[str(path)] = read_text(path)
            values[str(path.with_name(path.name.replace('_input', '_label')))] = read_text(path.with_name(path.name.replace('_input', '_label')))
    return {'wall_time': time.time(), 'monotonic_ns': time.monotonic_ns(), 'values': values}


def csv_rows(path):
    lines = path.read_text().splitlines()
    headers = [line for line in lines if line.startswith('mode,api,')]
    assert len(headers) == 1, headers
    fields = next(csv.reader(headers))
    result = []
    for line in lines:
        if line.startswith(('disjoint,', 'shared,')):
            values = next(csv.reader([line]))
            assert len(values) == len(fields), line
            result.append(dict(zip(fields, values)))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('label')
    parser.add_argument('--profiles', nargs='+', default=['main', 'original', 'async-mask'])
    parser.add_argument('--apis', nargs='+', choices=['write'], default=['write'])
    parser.add_argument('--threads', nargs='+', type=int, choices=[1, 6, 8], default=[1, 8])
    parser.add_argument('--modes', nargs='+', choices=['disjoint', 'shared'], default=['disjoint', 'shared'])
    parser.add_argument('--blocks', type=int, default=3)
    parser.add_argument('--seconds', type=int, default=4)
    parser.add_argument('--warmup', type=int, default=2)
    parser.add_argument('--cache-mib', type=int, default=64)
    parser.add_argument('--working-pages', type=int, default=32,
                        help='exact resident write working set, >=32 pages; default 32')
    parser.add_argument('--control-cpu', type=int, default=12)
    parser.add_argument('--cpus-1', default='2')
    parser.add_argument('--cpus-6', default='0,2,4,6,8,10')
    parser.add_argument('--cpus-8', default='0,2,4,6,8,10,1,3')
    parser.add_argument('--perf', choices=['single', 'all', 'off'], default='single')
    parser.add_argument('--precondition-warmup', type=int, default=30)
    parser.add_argument('--precondition-seconds', type=int, default=1)
    parser.add_argument('--precondition-profile', default='main')
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    assert args.blocks > 0 and args.seconds > 0 and args.warmup > 0
    assert args.working_pages >= 32
    assert args.working_pages >= max(args.threads)
    assert len(set(args.profiles)) == len(args.profiles)
    assert args.precondition_warmup > 0 and args.precondition_seconds > 0
    assert args.precondition_profile in args.profiles
    cpu_map = {n: [int(s) for s in getattr(args, f'cpus_{n}').split(',')] for n in args.threads}
    for n, cpus in cpu_map.items():
        assert len(cpus) == n and len(set(cpus)) == n
        assert args.control_cpu not in cpus
        for cpu in cpus:
            assert Path(f'/sys/devices/system/cpu/cpu{cpu}').exists()
    all_cpus = sorted({args.control_cpu, *(cpu for cpus in cpu_map.values() for cpu in cpus)})
    binary = Path(__file__).resolve().with_name('cache_api_probe')
    libraries = {name: ROOT / 'builds' / name / 'release/lib/libsplinterdb.so' for name in args.profiles}
    out = ROOT / 'probe-results' / args.label
    out.mkdir(parents=True, exist_ok=args.resume)
    arguments = vars(args).copy()
    arguments.pop('resume')
    metadata = {
        'arguments': arguments,
        'runner_sha256': digest(Path(__file__)),
        'driver_sha256': digest(binary),
        'libraries': {name: digest(lib) for name, lib in libraries.items()},
        'cpu_map': cpu_map,
        'topology': {str(cpu): {name: read_text(Path(f'/sys/devices/system/cpu/cpu{cpu}/topology') / name)
                               for name in ('physical_package_id', 'core_id', 'thread_siblings_list')}
                     for cpu in all_cpus},
        'workload_scope': 'Resident mutable MEMTABLE pages; lock marks dirty; writeback/verification outside measurement.',
        'perf_scope': 'Per-worker user-mode events; single enables only one-worker cases.',
        'telemetry_scope': 'Before/after process snapshots, not continuously sampled or exact phase frequency.',
        'runs': [],
        'precondition_runs': [],
        'schedule': 'scenario-major; each case has one excluded predetermined precondition, then all rotating mirrored blocks',
    }
    rows = []
    if args.resume:
        old = json.loads((out / 'manifest.json').read_text())
        for key in ('arguments', 'driver_sha256', 'libraries', 'runner_sha256'):
            assert old[key] == metadata[key], key
        metadata = old
        if (out / 'results.csv').exists():
            rows = list(csv.DictReader((out / 'results.csv').open()))
    scenarios = [(api, n, mode) for api in args.apis for n in args.threads for mode in args.modes]
    metadata['scenario_order'] = [dict(api=api, threads=n, mode=mode) for api, n, mode in scenarios]

    def save():
        (out / 'manifest.json').write_text(json.dumps(metadata, indent=2) + '\n')
        if rows:
            with (out / 'results.csv').open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

    def execute_job(name, profile, api, threads, mode, warmup, seconds):
        assert digest(libraries[profile]) == metadata['libraries'][profile]
        scratch = ROOT / 'scratch' / name
        scratch.mkdir(parents=True, exist_ok=False)
        db = scratch / 'cache.db'
        perf = args.perf == 'all' or (args.perf == 'single' and threads == 1)
        env = {
            'LD_LIBRARY_PATH': str(libraries[profile].parent),
            'CACHE_PROBE_CONTROL_CPU': str(args.control_cpu),
            'CACHE_PROBE_WORKER_CPUS': ','.join(map(str, cpu_map[threads])),
            'CACHE_PROBE_WARMUP_SECONDS': str(warmup),
            'CACHE_PROBE_PERF': '1' if perf else '0',
            'CACHE_PROBE_DISK_RATIO': '16',
            'CACHE_PROBE_ADDRESS_LAYOUT': 'spread',
        }
        if args.working_pages is not None:
            env['CACHE_PROBE_WORKING_PAGES'] = str(args.working_pages)
        command = [str(binary), str(db), str(args.cache_mib), str(threads), str(seconds), mode, api]
        before = snapshot(all_cpus)
        info = run(name, command, scratch, timeout=seconds + warmup + 300, env_extra=env, lock_held=True)
        after = snapshot(all_cpus)
        worker_lines = [line for line in (ROOT / 'jobs' / name / 'output.log').read_text().splitlines()
                        if line.startswith('WORKER ')]
        workers = [{key: int(value) for key, value in
                    (part.split('=', 1) for part in line.split()[1:])}
                   for line in worker_lines]
        assert len(workers) == threads
        assert [worker['worker'] for worker in workers] == list(range(threads))
        assert [worker['cpu'] for worker in workers] == cpu_map[threads]
        tids = [worker['tid'] for worker in workers]
        assert len(set(tids)) == threads
        expected = metadata.setdefault('expected_worker_tids', {})
        expected.setdefault(str(threads), tids)
        assert expected[str(threads)] == tids, (expected, workers)
        parsed = csv_rows(ROOT / 'jobs' / name / 'output.log')
        assert len(parsed) == 1
        row = parsed[0]
        assert row['mode'] == mode and row['api'] == api
        assert int(row['cache_mib']) == args.cache_mib
        assert int(row['threads']) == threads and int(row['warmup_seconds']) == warmup
        if args.working_pages is not None:
            assert int(row['working_pages']) == args.working_pages
        assert int(row['errors']) == int(row['read_bytes']) == int(row['write_bytes']) == int(row['major_faults']) == 0
        assert int(row['perf_enabled']) == int(perf)
        assert int(row['async_waits']) == 0
        assert int(row['ops']) > 0 and int(row['all_ops']) >= int(row['ops'])
        assert sum(w['ops'] for w in workers) == int(row['ops'])
        assert int(row['verified_updates']) == int(row['all_ops'])
        assert int(row['warmup_ops']) == int(row['all_ops']) - int(row['ops'])
        assert int(row['dirty_pages']) > 0
        assert sum(w['all_ops'] for w in workers) == int(row['all_ops'])
        assert sum(w['claim_failures'] for w in workers) == int(row['claim_failures'])
        assert sum(w['all_claim_failures'] for w in workers) == int(row['all_claim_failures'])
        assert int(row['all_claim_failures']) >= int(row['claim_failures'])
        if mode == 'disjoint':
            assert int(row['all_claim_failures']) == 0
        if perf:
            assert int(row['perf_enabled_ns']) == int(row['perf_running_ns']) > 0
        increases = {key: int(value) - int(before['values'][key])
                     for key, value in after['values'].items()
                     if key.endswith('_throttle_count') and value is not None
                     and before['values'].get(key) is not None
                     and int(value) > int(before['values'][key])}
        info.update(before_telemetry=before, after_telemetry=after,
                    throttle_increases=increases, workers=workers, profile=profile)
        return info, row, db

    print('WAIT suite machine.lock', flush=True)
    with LOCK.open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        metadata.setdefault('lock_intervals', []).append({'acquired': time.time()})
        try:
            for name, lib in libraries.items():
                env = os.environ.copy()
                env['LD_LIBRARY_PATH'] = str(lib.parent)
                loaded = subprocess.check_output(['ldd', str(binary)], env=env, text=True)
                assert str(lib) in loaded, (name, loaded)
            save()
            for scenario_index, (api, threads, mode) in enumerate(scenarios):
                planned = []
                for block in range(args.blocks):
                    rotated = args.profiles[block % len(args.profiles):] + args.profiles[:block % len(args.profiles)]
                    order = rotated + list(reversed(rotated))
                    for slot, profile in enumerate(order):
                        name = f'{args.label}-b{block:02d}-{api}-{threads}-{mode}-s{slot:02d}-{profile}'
                        planned.append((name, block, slot, profile))
                completed = {row['job'] for row in rows}
                pending = [job for job in planned if job[0] not in completed]
                if not pending:
                    print('RESUMED completed scenario', api, threads, mode, flush=True)
                    continue
                # Predetermined exclusion: this is never used in comparisons.
                # Resume a partial scenario with a new precondition, since the
                # earlier one may have run long before this process restarted.
                previous = [item for item in metadata['precondition_runs']
                            if item['scenario_index'] == scenario_index]
                name = (f'{args.label}-precondition-s{scenario_index:02d}-{api}-{threads}-{mode}'
                        f'-r{len(previous):02d}-{args.precondition_profile}')
                info, row, db = execute_job(name, args.precondition_profile, api, threads, mode,
                                            args.precondition_warmup, args.precondition_seconds)
                info.update(scenario_index=scenario_index,
                            scenario=dict(api=api, threads=threads, mode=mode),
                            excluded_from_results=True, row=row)
                metadata['precondition_runs'].append(info)
                save()
                print('PRECONDITION', api, threads, mode, 'excluded', row['ops_s'], flush=True)
                db.unlink()
                for name, block, slot, profile in pending:
                    info, row, db = execute_job(name, profile, api, threads, mode, args.warmup, args.seconds)
                    row.update(profile=profile, block=block, slot=slot, job=name,
                               worker_cpus=':'.join(map(str, cpu_map[threads])),
                               thermal_counter_increased=int(bool(info['throttle_increases'])))
                    rows.append(row)
                    info.update(block=block, slot=slot)
                    metadata['runs'].append(info)
                    save()
                    print('RESULT', profile, api, threads, mode, row['ops_s'],
                          'throttle=' + str(bool(info['throttle_increases'])), flush=True)
                    db.unlink()
            metadata['finished'] = time.time()
        finally:
            metadata['lock_intervals'][-1]['released'] = time.time()
            save()
    print('COMPLETE', args.label, len(rows), 'rows', flush=True)


if __name__ == '__main__':
    main()
