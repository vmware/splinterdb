#!/usr/bin/env python3
"""Run serialized, balanced public-API comparisons using one frozen driver.

Examples:
  python3 run.py resident baseline --profiles main original --seconds 5 --blocks 2
  python3 run.py insert candidate --profiles main original candidate --seconds 5

Each block uses profiles + reverse(profiles): ABBA or ABCCBA. Resident tests
use 200k 24/100-byte tuples, a full warm scan, and a 128 MiB cache. Insert
tests use 16/16-byte tuples, eight writers, 256 MiB cache, and both WAL modes.
This runner takes machine.lock itself; do not wrap it in another lock holder.
Only newly created scratch images from successful, validated runs are deleted.
"""
import argparse
import csv
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import signal
import statistics
import subprocess
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
LOCK = Path('/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/machine.lock')
PHASES = ['warm@1', 'point:uniform@1', 'point:uniform@8',
          'range:uniform:1@1', 'range:uniform:1@8',
          'range:uniform:10000@1', 'range:uniform:10000@8']


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for data in iter(lambda: stream.read(1 << 20), b''):
            h.update(data)
    return h.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def read_csv(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


def write_csv(path, rows):
    with path.open('w') as stream:
        writer = csv.DictWriter(stream, list(rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def available(path):
    s = os.statvfs(path)
    return s.f_bavail * s.f_frsize


def hardware_snapshot():
    """Read-only context, not a frequency normalization or stability proof."""
    patterns = ['/sys/class/thermal/thermal_zone*/temp',
                '/sys/devices/system/cpu/cpufreq/policy*/scaling_cur_freq',
                '/sys/devices/system/cpu/cpufreq/policy*/scaling_governor',
                '/sys/class/power_supply/*/online']
    result = {'time': time.time(), 'readings': {}}
    for pattern in patterns:
        for path in Path('/').glob(pattern.lstrip('/')):
            try:
                result['readings'][str(path)] = path.read_text().strip()
            except OSError:
                pass
    return result


def execute(out, manifest, name, command, profile, db, minimum_free_gib):
    assert available(out) >= minimum_free_gib * 2**30, 'free-space floor reached'
    env = os.environ.copy()
    for key in ['LD_LIBRARY_PATH', 'LD_PRELOAD', 'TMPDIR']:
        env.pop(key, None)
    tmp = out / (name + '-tmp')
    tmp.mkdir()
    libdir = ROOT / 'builds' / profile / 'release/lib'
    env.update(LD_LIBRARY_PATH=str(libdir), TMPDIR=str(tmp))
    record = dict(name=name, profile=profile, command=command,
                  environment_overrides={key: env[key] for key in ['LD_LIBRARY_PATH', 'TMPDIR']},
                  started=time.time(), free_bytes_before=available(out),
                  library_sha256=digest(libdir / 'libsplinterdb.so'),
                  correctness_passed=False, hardware_before=hardware_snapshot())
    manifest['runs'].append(record)
    save(out / 'manifest.json', manifest)
    print('START', name, flush=True)
    child_cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    with (out / (name + '.log')).open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   env=env, cwd=out, start_new_session=True)
        try:
            record['returncode'] = process.wait(timeout=300)
        except BaseException:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            record['returncode'] = 'timeout-or-interruption'
            raise
        finally:
            child_cpu_after = resource.getrusage(resource.RUSAGE_CHILDREN)
            record.update(finished=time.time(), free_bytes_after=available(out),
                          log_sha256=digest(out / (name + '.log')),
                          hardware_after=hardware_snapshot(),
                          process_user_cpu_s=child_cpu_after.ru_utime - child_cpu_before.ru_utime,
                          process_system_cpu_s=child_cpu_after.ru_stime - child_cpu_before.ru_stime)
            if db.exists():
                stat = db.stat()
                record.update(db_allocated_bytes=stat.st_blocks * 512,
                              db_apparent_bytes=stat.st_size)
            save(out / 'manifest.json', manifest)
    assert record['returncode'] == 0, record
    print('END', name, round(record['finished'] - record['started'], 2), flush=True)
    return record


def query_base(binary, db, args, csv_path, profile, index):
    return ['taskset', '-c', args.cpus, str(binary), '--db', str(db),
            '--n', '200000', '--cache-mib', '128', '--disk-gib', '8',
            '--log', '0', '--threads', '8', '--mt-bg', '0', '--normal-bg', '0',
            '--memtable-mib', '4', '--secs', str(args.seconds), '--warmup', str(args.warmup),
            '--csv', str(csv_path), '--vtag', profile, '--stag', 'resident',
            '--otag', str(index), '--rep', str(index), '--seed', '42',
            '--hang-secs', '60', '--hang-dir', str(csv_path.parent)]


def checked_query_rows(path, expected, resident=True):
    rows = read_csv(path)
    assert [r['phase'] for r in rows] == expected, rows
    assert all(int(r['ops']) > 0 and int(r['errors']) == 0 and
               int(r['violations']) == 0 for r in rows), rows
    if resident:
        assert all(int(r['rd_bytes']) == 0 for r in rows if not r['phase'].startswith('warm')), rows
    assert all(int(r['swapin']) == 0 and int(r['swapout']) == 0 for r in rows), rows
    return rows


def resident(out, manifest, args, order, binary):
    phases = ['warm@1', *args.phases]
    fixture = out / 'fixture.db'
    csv_path = out / 'fixture.csv'
    cmd = query_base(binary, fixture, args, csv_path, 'main', -1)
    record = execute(out, manifest, 'fixture', cmd + ['--create', 'load:random@1'],
                     'main', fixture, args.min_free_gib)
    rows = checked_query_rows(csv_path, ['load:random@1'], False)
    assert int(rows[0]['ops']) == 200000, rows
    with fixture.open('r+b') as stream:
        os.fsync(stream.fileno())
    record['correctness_passed'] = True
    manifest['fixture_sha256'] = digest(fixture)
    save(out / 'manifest.json', manifest)
    all_rows = []
    runs = list(enumerate(order))
    if args.precondition_seconds:
        runs.insert(0, (-1, args.profiles[0]))
    for index, profile in runs:
        precondition = index == -1
        name = 'precondition' if precondition else f'{index:02d}-{profile}'
        db = out / (name + '.db')
        assert not db.exists()
        subprocess.run(['cp', '--reflink=auto', '--sparse=always', str(fixture), str(db)], check=True)
        with db.open('r+b') as stream:
            os.fsync(stream.fileno())
        csv_path = out / (name + '.csv')
        cmd = query_base(binary, db, args, csv_path, profile, index)
        if precondition:
            cmd[cmd.index('--secs') + 1] = str(args.precondition_seconds)
        record = execute(out, manifest, name, cmd + ['--open', *phases],
                         profile, db, args.min_free_gib)
        rows = checked_query_rows(csv_path, phases)
        record['correctness_passed'] = True
        record['csv_sha256'] = digest(csv_path)
        record['validated_rows'] = len(rows)
        record['excluded_precondition'] = precondition
        if not precondition:
            all_rows.extend(rows)
        db.unlink()
        record['own_successful_scratch_removed'] = True
        save(out / 'manifest.json', manifest)
    fixture.unlink()
    manifest['fixture_removed_after_all_queries_passed'] = True
    write_csv(out / 'results.csv', all_rows)
    return [dict(profile=r['version'], phase=r['phase'], ops_s=float(r['ops_s']),
                 block=int(r['order']) // (2 * len(args.profiles)),
                 cpu_us_op=float(r['cpu_s']) * 1e6 / int(r['ops']),
                 p99_us=float(r['p99_us']), read_B_op=float(r['rd_B_op']))
            for r in all_rows if not r['phase'].startswith('warm')]


def inserts(out, manifest, args, order, binary):
    rows = []
    for wal in args.wal:
        runs = list(enumerate(order))
        if args.precondition_seconds:
            runs.insert(0, (-1, args.profiles[0]))
        for index, profile in runs:
            precondition = index == -1
            name = f'wal{wal}-precondition' if precondition else f'wal{wal}-{index:02d}-{profile}'
            db = out / (name + '.db')
            csv_path = out / (name + '.csv')
            summary_path = out / (name + '.summary.csv')
            cmd = ['taskset', '-c', args.cpus, str(binary), '--db', str(db),
                   '--csv', str(csv_path), '--summary', str(summary_path),
                   '--records', str(args.records), '--seconds',
                   str(args.precondition_seconds if precondition else 0 if args.fixed_records else args.seconds),
                   '--report-ms', '100', '--cache-mib', '256', '--disk-gib', '8',
                   '--direct', '--inserters', '8', '--readers', '0',
                   '--memtable-bg', '1', '--normal-bg', '3', '--memtable-mib', '8',
                   '--key-size', '16', '--value-size', '16',
                   '--min-free-gib', str(args.min_free_gib), '--seed', '42']
            if wal == 0:
                cmd.append('--no-log')
            record = execute(out, manifest, name, cmd, profile, db, args.min_free_gib)
            log = (out / (name + '.log')).read_text()
            expected_reason = 'complete' if args.fixed_records and not precondition else 'time_limit'
            assert re.search(r'load_end reason=' + expected_reason + r' .* error=0(?: |\n)', log), log[-2000:]
            close = re.search(r'close_s=([\d.]+) close_rc=0', log)
            assert close, log[-2000:]
            summaries = read_csv(summary_path)
            assert len(summaries) == 1
            summary = summaries[0]
            inserted = int(summary['inserted'])
            if expected_reason == 'complete':
                assert inserted == args.records, summary
            else:
                assert 0 < inserted < args.records, summary
            intervals = read_csv(csv_path)
            assert intervals, csv_path
            last = intervals[-1]
            sampled_s = float(last['elapsed_s'])
            sampled_inserted = int(last['inserted'])
            elapsed_s = float(summary['elapsed_s'])
            cpu_s = sum(float(r['cpu_cores']) * float(r['interval_s']) for r in intervals)
            result = dict(profile=profile, phase=f'insert-wal{wal}@8',
                             block=index // (2 * len(args.profiles)),
                             ops_s=float(summary['achieved_insert_ops_s']),
                             cpu_us_op=cpu_s * 1e6 / sampled_inserted,
                             sampled_cpu_s=cpu_s, sampled_s=sampled_s,
                             sampled_inserted=sampled_inserted,
                             sampled_ops_s=sampled_inserted / sampled_s,
                             elapsed_s=elapsed_s,
                             post_sample_join_s=elapsed_s - sampled_s,
                             process_cpu_s=record['process_user_cpu_s'] + record['process_system_cpu_s'],
                             process_cpu_us_op=(record['process_user_cpu_s'] + record['process_system_cpu_s']) * 1e6 / inserted,
                             p99_us=float(summary['insert_service_p99_ms']) * 1000,
                             read_B_op=sum(float(r['read_mib_s']) * float(r['interval_s'])
                                           for r in intervals) * 2**20 / inserted,
                             inserted=inserted, close_s=float(close[1]),
                             total_elapsed_s=elapsed_s + float(close[1]),
                             drain_ops_s=inserted / (elapsed_s + float(close[1])))
            if args.insert_diagnostics:
                stages = {}
                workers = []
                for line in log.splitlines():
                    if line.startswith('diagnostic_stage '):
                        fields = dict(item.split('=', 1) for item in line.split()[1:])
                        stage_name = fields.pop('name')
                        stages[stage_name] = {k: int(v) for k, v in fields.items()}
                    elif line.startswith('diagnostic_worker '):
                        fields = {k: int(v) for k, v in (item.split('=', 1) for item in line.split()[1:])}
                        workers.append(fields)
                expected_stages = ['worker_start', 'stop_request', 'workers_joined', 'before_close', 'after_close']
                assert list(stages) == expected_stages, stages
                assert len(workers) == 8 and sum(w['completed'] for w in workers) == inserted, workers
                for previous, following in zip(expected_stages, expected_stages[1:]):
                    assert all(stages[following][key] >= stages[previous][key] for key in ['wall_ns', 'cpu_ns']), stages
                for phase, start, end in [('window', 'worker_start', 'stop_request'),
                                          ('join', 'stop_request', 'workers_joined'),
                                          ('body', 'worker_start', 'workers_joined'),
                                          ('close', 'before_close', 'after_close'),
                                          ('drain', 'worker_start', 'after_close')]:
                    for counter in ['wall', 'cpu']:
                        result[f'diagnostic_{phase}_{counter}_s'] = (stages[end][counter + '_ns'] - stages[start][counter + '_ns']) / 1e9
                result['body_cpu_us_op'] = result['diagnostic_body_cpu_s'] * 1e6 / inserted
                result['drain_cpu_us_op'] = result['diagnostic_drain_cpu_s'] * 1e6 / inserted
                stop_ns = stages['stop_request']['wall_ns']
                for worker in workers:
                    worker['last_insert_ms'] = (worker['last_end_ns'] - worker['last_start_ns']) / 1e6
                    worker['in_flight_at_stop'] = worker['last_start_ns'] <= stop_ns < worker['last_end_ns']
                    worker['last_insert_after_stop_ms'] = max(0, worker['last_end_ns'] - stop_ns) / 1e6
                    worker['deregister_ms'] = (worker['deregister_end_ns'] - worker['deregister_start_ns']) / 1e6
                record['diagnostic_stages'] = stages
                record['diagnostic_workers'] = workers
            if not precondition:
                rows.append(result)
            record.update(correctness_passed=True, summary_sha256=digest(summary_path),
                          csv_sha256=digest(csv_path), inserted=inserted,
                          excluded_precondition=precondition, metrics=result,
                          validation=f'Normal {expected_reason} stop, checked insert count, error=0, close_rc=0; no exhaustive readback')
            db.unlink()
            record['own_successful_scratch_removed'] = True
            save(out / 'manifest.json', manifest)
    write_csv(out / 'results.csv', rows)
    return rows


def summarize(out, rows, profiles):
    summaries = []
    block_rows = []
    for phase in dict.fromkeys(row['phase'] for row in rows):
        group = {p: [r for r in rows if r['phase'] == phase and r['profile'] == p]
                 for p in profiles}
        baseline = statistics.fmean(r['ops_s'] for r in group[profiles[0]])
        for profile, samples in group.items():
            rates = [r['ops_s'] for r in samples]
            mean = statistics.fmean(rates)
            summary = dict(phase=phase, profile=profile, samples=len(samples),
                           ops_s_mean=mean, ops_s_min=min(rates), ops_s_max=max(rates),
                           ops_s_stdev=statistics.stdev(rates) if len(rates) > 1 else 0,
                           delta_baseline_pct=100 * (mean / baseline - 1),
                           cpu_us_op_mean=statistics.fmean(r['cpu_us_op'] for r in samples),
                           p99_us_mean=statistics.fmean(r['p99_us'] for r in samples),
                           read_B_op_mean=statistics.fmean(r['read_B_op'] for r in samples))
            if 'drain_ops_s' in samples[0]:
                for metric in ['sampled_ops_s', 'drain_ops_s', 'sampled_cpu_s', 'sampled_s',
                               'elapsed_s', 'post_sample_join_s', 'close_s', 'total_elapsed_s',
                               'process_cpu_s', 'process_cpu_us_op']:
                    summary[metric + '_mean'] = statistics.fmean(r[metric] for r in samples)
                for metric in ['sampled_ops_s', 'drain_ops_s']:
                    baseline_metric = statistics.fmean(r[metric] for r in group[profiles[0]])
                    summary[metric + '_delta_baseline_pct'] = 100 * (summary[metric + '_mean'] / baseline_metric - 1)
            if 'body_cpu_us_op' in samples[0]:
                for metric in [k for k in samples[0] if k.startswith('diagnostic_')] + ['body_cpu_us_op', 'drain_cpu_us_op']:
                    summary[metric + '_mean'] = statistics.fmean(r[metric] for r in samples)
            block_ratios = []
            for block in sorted({r['block'] for r in samples}):
                candidate_rate = statistics.fmean(r['ops_s'] for r in samples if r['block'] == block)
                baseline_rate = statistics.fmean(r['ops_s'] for r in group[profiles[0]] if r['block'] == block)
                ratio = candidate_rate / baseline_rate
                block_ratios.append(math.log(ratio))
                block_rows.append(dict(phase=phase, profile=profile, block=block,
                                       candidate_ops_s=candidate_rate, baseline_ops_s=baseline_rate,
                                       delta_baseline_pct=100 * (ratio - 1)))
            nblocks = len(block_ratios)
            mean_log = statistics.fmean(block_ratios)
            # Two-sided 95% Student t cutoffs, degrees of freedom 1..30.
            t95 = [None, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365,
                   2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131,
                   2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069,
                   2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042]
            half_width = (t95[min(nblocks - 1, 30)] * statistics.stdev(block_ratios)
                          / math.sqrt(nblocks)) if nblocks > 1 else None
            summary.update(blocks=nblocks,
                           block_geomean_delta_pct=100 * (math.exp(mean_log) - 1),
                           block_ci95_low_pct=100 * (math.exp(mean_log - half_width) - 1) if half_width is not None else '',
                           block_ci95_high_pct=100 * (math.exp(mean_log + half_width) - 1) if half_width is not None else '')
            summaries.append(summary)
            print(phase, profile, f'{mean:.1f} ops/s {summary["delta_baseline_pct"]:+.2f}%', flush=True)
    write_csv(out / 'summary.csv', summaries)
    write_csv(out / 'block-ratios.csv', block_rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['resident', 'insert'])
    parser.add_argument('name', help='new result directory name')
    parser.add_argument('--profiles', nargs='+', default=['main', 'original'])
    parser.add_argument('--seconds', type=int, default=5)
    parser.add_argument('--records', type=int, default=20000000, help='insert target; identical across all profiles')
    parser.add_argument('--fixed-records', action='store_true', help='insert exactly --records instead of stopping on time; --seconds is ignored')
    parser.add_argument('--insert-diagnostics', action='store_true', help='use separately built driver with exact CPU/wall phase boundaries and worker-exit diagnostics')
    parser.add_argument('--blocks', type=int, default=2)
    parser.add_argument('--rotate-blocks', action='store_true', help='rotate profile starting position each mirrored block')
    parser.add_argument('--phases', nargs='+', choices=PHASES[1:], default=PHASES[1:], help='resident phases; a full warm scan is always prepended')
    parser.add_argument('--warmup', type=float, default=1, help='resident warmup before each timed phase, seconds')
    parser.add_argument('--precondition-seconds', type=int, default=0, help='excluded startup run before measurement; per selected phase or WAL mode')
    parser.add_argument('--cpus', default='0-11')
    parser.add_argument('--min-free-gib', type=int, default=6)
    parser.add_argument('--wal', nargs='+', type=int, choices=[0, 1], default=[0, 1])
    args = parser.parse_args()
    assert re.fullmatch(r'[A-Za-z0-9_-]+', args.name), 'unsafe result name'
    assert all(re.fullmatch(r'[A-Za-z0-9_-]+', p) for p in args.profiles)
    assert (args.seconds >= 0 if args.fixed_records else args.seconds > 0)
    assert args.blocks > 0 and len(set(args.profiles)) == len(args.profiles)
    assert args.warmup >= 0 and args.precondition_seconds >= 0
    assert args.records > 0 and (not args.fixed_records or args.mode == 'insert')
    assert not args.insert_diagnostics or args.mode == 'insert'
    out = HERE / 'results' / (args.mode + '-' + args.name)
    out.mkdir(parents=True, exist_ok=False)
    filesystem = subprocess.check_output(['findmnt', '-n', '-o', 'FSTYPE,SOURCE', '-T', str(out)], text=True).strip()
    fstype, source = filesystem.split()[:2]
    assert fstype not in {'tmpfs', 'ramfs'} and source.startswith('/dev/'), filesystem
    build = json.loads((HERE / ('diagnostics-build.json' if args.insert_diagnostics else 'dynamic-build.json')).read_text())
    binary_name = 'kvbench' if args.mode == 'resident' else 'insert_benchmark'
    driver = build['binaries'][binary_name]
    binary = Path(driver['binary'])
    assert digest(binary) == driver['binary_sha256']
    assert digest(driver['source']) == driver['source_sha256']
    order = []
    for block in range(args.blocks):
        shift = block % len(args.profiles) if args.rotate_blocks else 0
        profiles = args.profiles[shift:] + args.profiles[:shift]
        order.extend(profiles + list(reversed(profiles)))
    provenance = {}
    for profile in set(args.profiles + ['main']):
        source_path = ROOT / (profile + '-source.json')
        provenance[profile] = dict(library_sha256=digest(ROOT / 'builds' / profile / 'release/lib/libsplinterdb.so'),
                                   clockcache_source_sha256=digest(ROOT / 'sources' / profile / 'src/clockcache.c'))
        if source_path.exists():
            provenance[profile]['source_manifest_sha256'] = digest(source_path)
            provenance[profile]['source_manifest'] = json.loads(source_path.read_text())
    manifest = dict(mode=args.mode, arguments=vars(args), order=order,
                    filesystem=filesystem, io='O_DIRECT', driver=driver,
                    provenance=provenance, runner_sha256=digest(__file__),
                    machine_lock=str(LOCK), queued=time.time(), runs=[], complete=False,
                    limitations=['Bounded startup insertion throughput is not steady-state or tail-neutrality evidence.',
                                 'Dynamic library swapping controls driver layout; library code layout still differs.',
                                 'Block confidence intervals assume independent approximately normal log ratios; few blocks and thermal carryover limit certainty.',
                                 'Hardware snapshots are observational before/after context, not timed frequency measurements or normalization.',
                                 'Insert cpu_us_op covers the sampled reporting window only; it excludes post-sample joins and close. process_cpu_us_op includes setup, all joins, and close.',
                                 'Resident background thread count is zero, differing from historical 1+3 background configuration.'])
    save(out / 'manifest.json', manifest)
    print('WAIT machine.lock', flush=True)
    with LOCK.open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        manifest['lock_acquired'] = time.time()
        try:
            rows = (resident if args.mode == 'resident' else inserts)(out, manifest, args, order, binary)
            summarize(out, rows, args.profiles)
            manifest['complete'] = True
        finally:
            manifest['finished'] = time.time()
            save(out / 'manifest.json', manifest)
    print('Completed', out, flush=True)


if __name__ == '__main__':
    main()
