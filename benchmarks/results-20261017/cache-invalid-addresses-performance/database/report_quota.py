#!/usr/bin/env python3
"""Audit quota-runner evidence without launching workloads or changing inputs.

Pass one or more result directories (or names below database/results). By
default each matrix must be complete. --allow-incomplete reports only complete
blocks in comparisons and lists pending runs. --output creates a new JSON file;
an existing output is never replaced. Both quota modes remain separate cohorts.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
T95 = [None, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365,
       2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131,
       2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069,
       2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042]
STAGES = ['worker_start', 'stop_request', 'workers_joined', 'before_close', 'after_close']
PHASES = [('window', 'worker_start', 'stop_request'),
          ('join', 'stop_request', 'workers_joined'),
          ('body', 'worker_start', 'workers_joined'),
          ('close', 'before_close', 'after_close'),
          ('drain', 'worker_start', 'after_close')]
RATIO_METRICS = ['exact_body_ops_s', 'exact_drain_ops_s', 'productive_ops_s',
                 'body_cpu_us_op', 'drain_cpu_us_op', 'worker_completion_spread_s']
DIFFERENCE_METRICS = ['worker_completion_spread_s', 'productive_end_to_stop_s',
                      'diagnostic_join_wall_s', 'diagnostic_close_wall_s']


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_csv(path):
    with Path(path).open() as stream:
        return list(csv.DictReader(stream))


def close_number(actual, expected, label):
    assert math.isfinite(float(actual)) and math.isfinite(float(expected)), label
    assert math.isclose(float(actual), float(expected), rel_tol=1e-12, abs_tol=1e-12), (label, actual, expected)


def parse_diagnostics(log):
    stages, workers, quotas = {}, [], []
    for line in log.splitlines():
        if line.startswith('diagnostic_stage '):
            fields = dict(part.split('=', 1) for part in line.split()[1:])
            name = fields.pop('name')
            assert name not in stages, 'duplicate diagnostic stage'
            stages[name] = {key: int(value) for key, value in fields.items()}
        elif line.startswith('diagnostic_worker '):
            workers.append({key: int(value) for key, value in
                            (part.split('=', 1) for part in line.split()[1:])})
        elif line.startswith('diagnostic_quota '):
            fields = dict(part.split('=', 1) for part in line.split()[1:])
            mode = fields.pop('mode')
            quotas.append(dict(mode=mode, **{key: int(value) for key, value in fields.items()}))
    assert list(stages) == STAGES, 'missing, reordered or extra diagnostic stages'
    assert len(quotas) == 1, 'expected exactly one quota diagnostic'
    assert [worker['index'] for worker in workers] == list(range(8)), 'worker index/count mismatch'
    return stages, workers, quotas[0]


def diagnostic_metrics(stages, workers, quota, records, mode, precondition=False):
    """Validate raw boundaries, including workers which received no records."""
    for before, after in zip(STAGES, STAGES[1:]):
        assert set(stages[before]) == set(stages[after]) == {'wall_ns', 'cpu_ns'}
        assert all(stages[after][key] >= stages[before][key] >= 0 for key in ['wall_ns', 'cpu_ns'])
    assert quota['mode'] == mode
    completed = sum(worker['completed'] for worker in workers)
    assert completed == quota['completed_records'] > 0
    assert quota['target_records'] == records
    if precondition:
        assert mode == 'fixed' and completed < records, 'precondition reached cap instead of timer'
    else:
        assert completed == records, 'incomplete measured record set'
    if mode == 'shared':
        assert quota['batch'] == 1024 and not precondition
        # One successful reservation per batch, then one empty fetch per worker.
        assert quota['next_record'] == ((records + 1023) // 1024 + 8) * 1024
    else:
        assert mode == 'fixed' and quota['batch'] == quota['next_record'] == 0
        if not precondition:
            assert all(worker['completed'] == (records + 7 - worker['index']) // 8 for worker in workers)
    start = stages['worker_start']['wall_ns']
    stop = stages['stop_request']['wall_ns']
    joined = stages['workers_joined']['wall_ns']
    for worker in workers:
        assert worker['completed'] >= 0
        exit_times = [start, worker['loop_exit_ns'], worker['deregister_start_ns'],
                      worker['deregister_end_ns'], joined]
        assert exit_times == sorted(exit_times), worker
        if worker['completed']:
            assert start <= worker['last_start_ns'] <= worker['last_end_ns'] <= worker['loop_exit_ns'], worker
        else:
            assert worker['last_start_ns'] == worker['last_end_ns'] == 0, worker
        if not precondition:
            assert worker['deregister_end_ns'] <= stop, 'fixed-count controller stopped before worker completion'
    finishes = [worker['last_end_ns'] for worker in workers if worker['completed']]
    productive_end = max(finishes)
    assert quota['productive_end_ns'] == productive_end > start
    result = {}
    for phase, begin, end in PHASES:
        for counter in ['wall', 'cpu']:
            result[f'diagnostic_{phase}_{counter}_s'] = (stages[end][counter + '_ns'] - stages[begin][counter + '_ns']) / 1e9
    result.update(body_cpu_us_op=result['diagnostic_body_cpu_s'] * 1e6 / completed,
                  drain_cpu_us_op=result['diagnostic_drain_cpu_s'] * 1e6 / completed,
                  productive_wall_s=(productive_end - start) / 1e9,
                  productive_end_to_stop_s=(stop - productive_end) / 1e9,
                  worker_completion_spread_s=(productive_end - min(finishes)) / 1e9)
    result.update(exact_body_ops_s=completed / result['diagnostic_body_wall_s'],
                  exact_drain_ops_s=completed / result['diagnostic_drain_wall_s'],
                  productive_ops_s=completed / result['productive_wall_s'])
    return result


def expected_command(directory, record, args, driver, wal, precondition):
    name = record['name']
    mode = 'fixed' if precondition else args['quota_mode']
    command = ['taskset', '-c', args['cpus'], driver['binary'], '--db', str(directory / (name + '.db')),
               '--csv', str(directory / (name + '.csv')), '--summary', str(directory / (name + '.summary.csv')),
               '--records', str(args['precondition_records'] if precondition else args['records']),
               '--seconds', str(args['precondition_seconds'] if precondition else 0),
               '--report-ms', '100', '--cache-mib', '256', '--disk-gib', '8', '--direct',
               '--inserters', '8', '--readers', '0', '--memtable-bg', '1', '--normal-bg', '3',
               '--memtable-mib', '8', '--key-size', '16', '--value-size', '16',
               '--min-free-gib', str(args['min_free_gib']), '--seed', '42']
    if wal == 0:
        command.append('--no-log')
    if mode == 'shared':
        command.append('--shared-quota')
    return command


def audit_provenance(manifest):
    driver = manifest['driver']
    for kind in ['source', 'binary']:
        assert digest(driver[kind]) == driver[kind + '_sha256'], f'driver {kind} changed'
    assert digest(HERE / 'run_quota.py') == manifest['runner_sha256'], 'quota runner changed'
    build_path = HERE / 'quota-build.json'
    build = json.loads(build_path.read_text())
    assert build['built'] and build['binaries']['insert_benchmark'] == driver
    assert build['runner_sha256'] == manifest['runner_sha256']
    assert digest(HERE / 'prepare_quota.py') == build['prepare_sha256']
    assert digest(build['parent_build_manifest']) == build['parent_build_manifest_sha256']
    for kind in ['source', 'binary']:
        assert digest(build['parent_driver'][kind]) == build['parent_driver'][kind + '_sha256']
    source_counts = {}
    for profile, provenance in manifest['provenance'].items():
        assert digest(ROOT / 'builds' / profile / 'release/lib/libsplinterdb.so') == provenance['library_sha256'], profile
        assert digest(ROOT / 'sources' / profile / 'src/clockcache.c') == provenance['clockcache_source_sha256'], profile
        source_counts[profile] = 1
        if 'source_manifest' in provenance:
            source_path = ROOT / (profile + '-source.json')
            assert digest(source_path) == provenance['source_manifest_sha256']
            assert json.loads(source_path.read_text()) == provenance['source_manifest']
            hashes = provenance['source_manifest']['hashes']
            for path, expected in hashes.items():
                assert digest(ROOT / 'sources' / profile / path) == expected, (profile, path)
            source_counts[profile] = len(hashes)
    return dict(driver_sha256=driver['binary_sha256'], driver_source_sha256=driver['source_sha256'],
                runner_sha256=manifest['runner_sha256'], build_manifest_sha256=digest(build_path),
                source_file_hashes_checked=source_counts,
                library_hashes={profile: entry['library_sha256'] for profile, entry in manifest['provenance'].items()})


def audit_campaign(directory, allow_incomplete=False):
    manifest_path = directory / 'manifest.json'
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    args = manifest['arguments']
    assert manifest['mode'] == args['mode'] == 'insert'
    assert args['fixed_records'] and args['insert_diagnostics']
    assert args['quota_mode'] in ['fixed', 'shared'] and args['records'] > 0 and args['blocks'] > 0
    profiles = args['profiles']
    assert len(profiles) == len(set(profiles)) and profiles
    assert len(args['wal']) == len(set(args['wal'])) and set(args['wal']) <= {0, 1}
    order = []
    for block in range(args['blocks']):
        shift = block % len(profiles) if args['rotate_blocks'] else 0
        block_profiles = profiles[shift:] + profiles[:shift]
        order.extend(block_profiles + block_profiles[::-1])
    assert order == manifest['order']
    expected = {}
    for wal in args['wal']:
        if args['precondition_seconds']:
            expected[f'wal{wal}-precondition'] = (wal, -1, profiles[0], True)
        for index, profile in enumerate(order):
            expected[f'wal{wal}-{index:02d}-{profile}'] = (wal, index, profile, False)
    runs = manifest['runs']
    names = [record['name'] for record in runs]
    assert len(names) == len(set(names))
    assert names == list(expected)[:len(names)], 'run order/name mismatch'
    validated = [record for record in runs if record.get('correctness_passed')]
    assert validated == runs[:len(validated)], 'validated runs do not form a prefix'
    complete = manifest['complete'] and len(validated) == len(expected)
    assert allow_incomplete or complete, f'incomplete matrix: {len(expected) - len(validated)} runs pending'
    provenance = audit_provenance(manifest)
    assert manifest['filesystem'].startswith('ext4 /dev/') and manifest['io'] == 'O_DIRECT'
    assert manifest['machine_lock'] == '/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/machine.lock'
    samples, preconditions, aggregate = [], [], []
    previous_end = manifest['lock_acquired']
    for record in validated:
        name = record['name']
        wal, index, profile, precondition = expected[name]
        assert record['returncode'] == 0 and record['profile'] == profile
        assert record['excluded_precondition'] == precondition
        assert record['command'] == expected_command(directory, record, args, manifest['driver'], wal, precondition)
        assert record['environment_overrides'] == dict(LD_LIBRARY_PATH=str(ROOT / 'builds' / profile / 'release/lib'),
                                                       TMPDIR=str(directory / (name + '-tmp')))
        assert previous_end <= record['started'] <= record['finished'] <= manifest.get('finished', math.inf), 'overlapping/outside-lock runs'
        previous_end = record['finished']
        assert record['free_bytes_before'] >= args['min_free_gib'] * 2**30
        assert record['library_sha256'] == provenance['library_hashes'][profile]
        for ending, field in [('.log', 'log_sha256'), ('.csv', 'csv_sha256'), ('.summary.csv', 'summary_sha256')]:
            assert digest(directory / (name + ending)) == record[field], (name, field)
        assert record['own_successful_scratch_removed'] and not (directory / (name + '.db')).exists()
        log = (directory / (name + '.log')).read_text()
        endings = re.findall(r'load_end reason=(\w+) inserted=(\d+) elapsed_s=([\d.]+) error=(\d+)', log)
        assert len(endings) == 1 and endings[0][0] == ('time_limit' if precondition else 'complete') and endings[0][3] == '0'
        assert len(re.findall(r'close_s=[\d.]+ close_rc=0(?:\n|$)', log)) == 1
        stages, workers, quota = parse_diagnostics(log)
        assert stages == record['diagnostic_stages'] and quota == record['diagnostic_quota']
        assert len(record['diagnostic_workers']) == len(workers)
        for worker, stored in zip(workers, record['diagnostic_workers']):
            assert all(stored[key] == value for key, value in worker.items())
            close_number(stored['last_insert_ms'], (worker['last_end_ns'] - worker['last_start_ns']) / 1e6, name)
            close_number(stored['deregister_ms'], (worker['deregister_end_ns'] - worker['deregister_start_ns']) / 1e6, name)
            stop = stages['stop_request']['wall_ns']
            assert stored['in_flight_at_stop'] == (worker['last_start_ns'] <= stop < worker['last_end_ns'])
            close_number(stored['last_insert_after_stop_ms'], max(0, worker['last_end_ns'] - stop) / 1e6, name)
        target = args['precondition_records'] if precondition else args['records']
        mode = 'fixed' if precondition else args['quota_mode']
        metrics = diagnostic_metrics(stages, workers, quota, target, mode, precondition)
        summaries = read_csv(directory / (name + '.summary.csv'))
        assert len(summaries) == 1
        summary = summaries[0]
        count = int(summary['inserted'])
        assert count == record['inserted'] == record['metrics']['inserted'] == int(endings[0][1]) == quota['completed_records']
        intervals = read_csv(directory / (name + '.csv'))
        assert intervals and [int(row['inserted']) for row in intervals] == sorted(int(row['inserted']) for row in intervals)
        assert 0 < int(intervals[-1]['inserted']) <= count
        assert all(float(row['interval_s']) > 0 and float(row['elapsed_s']) > 0 for row in intervals)
        assert [float(row['elapsed_s']) for row in intervals] == sorted(float(row['elapsed_s']) for row in intervals)
        assert record['metrics']['profile'] == profile and record['metrics']['quota_mode'] == mode
        phase = f'insert-wal{wal}-{mode}@8'
        assert record['metrics']['phase'] == phase
        block = index // (2 * len(profiles))
        assert record['metrics']['block'] == block
        for key, value in metrics.items():
            close_number(record['metrics'][key], value, (name, key))
        close_number(record['metrics']['ops_s'], summary['achieved_insert_ops_s'], name)
        close_number(record['metrics']['elapsed_s'], summary['elapsed_s'], name)
        row = dict(name=name, profile=profile, phase=phase, wal=wal, quota_mode=mode,
                   block=block, inserted=count, **metrics,
                   worker_completed=[worker['completed'] for worker in workers],
                   worker_productive_finish_s=[(worker['last_end_ns'] - stages['worker_start']['wall_ns']) / 1e9
                                                if worker['completed'] else None for worker in workers],
                   zero_work_workers=sum(worker['completed'] == 0 for worker in workers),
                   max_last_insert_ms=max((worker['last_end_ns'] - worker['last_start_ns']) / 1e6 for worker in workers),
                   max_deregister_ms=max((worker['deregister_end_ns'] - worker['deregister_start_ns']) / 1e6 for worker in workers),
                   quota_next_record=quota['next_record'], log_sha256=record['log_sha256'],
                   service_max_ms=float(summary['insert_service_max_ms']))
        if precondition:
            preconditions.append(row)
        else:
            samples.append(row)
            aggregate.append({key: str(value) for key, value in record['metrics'].items()})
    if complete:
        assert read_csv(directory / 'results.csv') == aggregate, 'aggregate CSV differs from measured records'
    complete_blocks = {}
    for wal in args['wal']:
        complete_blocks[str(wal)] = [block for block in range(args['blocks'])
                                    if all(sum(row['wal'] == wal and row['block'] == block and row['profile'] == profile
                                               for row in samples) == 2 for profile in profiles)]
    return dict(campaign=directory.name, directory=str(directory), arguments=args,
                validation=dict(complete=complete, expected_measured_runs=len(order) * len(args['wal']),
                                measured_runs=len(samples), excluded_preconditions=len(preconditions),
                                total_measured_inserts=sum(row['inserted'] for row in samples),
                                pending_runs=list(expected)[len(validated):], complete_blocks=complete_blocks,
                                manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(), **provenance,
                                lock_acquired=manifest['lock_acquired'], finished=manifest.get('finished'),
                                checks=['Exact command/matrix/order and fixed-quota preconditions',
                                        'Harness, runner, build, parent driver, source and library hashes',
                                        'Raw log/CSV hashes, successful count/error/close, aggregate CSV identities',
                                        'Shared reservation identity or per-worker fixed count identity',
                                        'Monotonic raw boundaries, zero-work workers, reconstructed wall/CPU metrics',
                                        'Serial processes inside recorded shared-lock interval']),
                samples=samples, excluded_precondition_samples=preconditions)


def interval(values, logarithmic=False):
    if logarithmic:
        assert all(value > 0 for value in values)
        values = [math.log(value) for value in values]
    mean = statistics.fmean(values)
    n = len(values)
    half = T95[min(n - 1, 30)] * statistics.stdev(values) / math.sqrt(n) if n > 1 else None
    if logarithmic:
        convert = lambda x: 100 * math.expm1(x)
        return dict(blocks=n, geometric_delta_pct=convert(mean),
                    ci95_low_pct=convert(mean - half) if half is not None else None,
                    ci95_high_pct=convert(mean + half) if half is not None else None,
                    block_delta_pct=[convert(value) for value in values])
    return dict(blocks=n, mean_difference_s=mean,
                ci95_low_s=mean - half if half is not None else None,
                ci95_high_s=mean + half if half is not None else None,
                block_difference_s=values)


def summarize(campaign):
    args = campaign['arguments']
    rows = campaign['samples']
    summaries, comparisons = [], []
    for wal in args['wal']:
        blocks = campaign['validation']['complete_blocks'][str(wal)]
        for profile in args['profiles']:
            group = [row for row in rows if row['wal'] == wal and row['profile'] == profile]
            if not group:
                continue
            count = sum(row['inserted'] for row in group)
            summary = dict(wal=wal, quota_mode=args['quota_mode'], profile=profile, samples=len(group),
                           all_expected_samples_present=len(group) == 2 * args['blocks'],
                           complete_comparison_blocks=blocks, inserted_total=count)
            for key in dict.fromkeys(RATIO_METRICS + DIFFERENCE_METRICS + ['productive_wall_s',
                    'diagnostic_body_wall_s', 'diagnostic_drain_wall_s', 'diagnostic_body_cpu_s', 'diagnostic_drain_cpu_s']):
                summary[key + '_mean'] = statistics.fmean(row[key] for row in group)
            for phase in ['body', 'drain']:
                summary['pooled_' + phase + '_ops_s'] = count / sum(row[f'diagnostic_{phase}_wall_s'] for row in group)
            summary['pooled_productive_ops_s'] = count / sum(row['productive_wall_s'] for row in group)
            summary['max_deregister_ms'] = max(row['max_deregister_ms'] for row in group)
            summary['zero_work_worker_observations'] = sum(row['zero_work_workers'] for row in group)
            summaries.append(summary)
        if not blocks:
            continue
        for baseline in [profile for profile in ['main', 'original'] if profile in args['profiles']]:
            for profile in args['profiles']:
                if profile == baseline or (baseline == 'original' and profile == 'main'):
                    continue
                comparison = dict(wal=wal, quota_mode=args['quota_mode'], profile=profile, baseline=baseline,
                                  complete_block_numbers=blocks, estimates={}, absolute_differences={})
                for metric in RATIO_METRICS:
                    pairs = []
                    for block in blocks:
                        means = [statistics.fmean(row[metric] for row in rows
                                                 if row['wal'] == wal and row['block'] == block and row['profile'] == p)
                                 for p in [profile, baseline]]
                        pairs.append(means)
                    if all(candidate > 0 and control > 0 for candidate, control in pairs):
                        comparison['estimates'][metric] = interval([candidate / control for candidate, control in pairs], True)
                    else:
                        comparison['estimates'][metric] = {'blocks': len(blocks), 'unavailable': 'zero values; see absolute differences'}
                for metric in DIFFERENCE_METRICS:
                    differences = []
                    for block in blocks:
                        candidate, control = [statistics.fmean(row[metric] for row in rows
                                                               if row['wal'] == wal and row['block'] == block and row['profile'] == p)
                                              for p in [profile, baseline]]
                        differences.append(candidate - control)
                    comparison['absolute_differences'][metric] = interval(differences)
                comparisons.append(comparison)
    campaign.update(summaries=summaries, comparisons=comparisons)
    return campaign


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaigns', nargs='+')
    parser.add_argument('--allow-incomplete', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    assert args.output is None or not args.output.exists(), 'will not replace an existing report'
    directories = []
    for value in args.campaigns:
        path = Path(value)
        if not path.is_dir():
            path = HERE / 'results' / value
        directories.append(path.resolve())
    assert len(directories) == len(set(directories)), 'duplicate campaign'
    campaigns = [summarize(audit_campaign(path, args.allow_incomplete)) for path in directories]
    by_start = sorted(campaigns, key=lambda c: c['validation']['lock_acquired'])
    for first, second in zip(by_start, by_start[1:]):
        assert first['validation']['finished'] is not None
        assert first['validation']['finished'] <= second['validation']['lock_acquired'], 'campaign lock intervals overlap'
    binaries = {campaign['validation']['driver_sha256'] for campaign in campaigns}
    assert len(binaries) == 1, 'quota modes must use the same frozen binary'
    report = dict(auditor_sha256=digest(__file__), same_binary_verified=True,
                  campaign_lock_intervals_nonoverlapping=True, campaigns=campaigns,
                  uncertainty_method='Arithmetic mirror-pair means within complete blocks; pointwise Student t intervals on block log ratios (or raw second differences). One block has no confidence interval. All samples retained; no multiple-comparison adjustment.',
                  limitations=['No exhaustive database readback; uniqueness follows disjoint ID reservation and unchanged bijective key generation, with successful exact-count checks.',
                               'Shared quotas change worker/key assignment and add reservation/control overhead. Compare profiles within each mode; a fixed/shared difference is not solely a quota-tail effect.',
                               'Productive throughput ends at the final insert completion and excludes polling, deregistration and close; it supplements exact body/drain rates.',
                               'Excluded timed preconditions use fixed quotas and may finish their last operation after stop request, producing a negative productive_end_to_stop_s.',
                               'Before/after hardware snapshots retained in the hashed source manifests are not timed frequency measurements. DVFS, variable SMT placement and background-task distribution remain confounders.',
                               'Three blocks and thermal carryover can give broad or optimistic uncertainty. Fixed-record startup tests do not establish steady-state or tail neutrality.',
                               'Small smoke runs validate bookkeeping and zero-work workers; their throughput is not performance evidence.',
                               'This audit checks recorded lock intervals and serial jobs, not unrecorded external machine activity.'])
    rendered = json.dumps(report, indent=2, allow_nan=False) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(rendered)
        print(args.output)
    else:
        print(rendered, end='')


if __name__ == '__main__':
    main()
