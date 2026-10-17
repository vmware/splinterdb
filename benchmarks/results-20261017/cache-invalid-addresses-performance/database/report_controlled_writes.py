#!/usr/bin/env python3
"""Audit a resident cache-write campaign and report paired block ratios.

Reads benchmark inputs only. With --output, creates one new analysis JSON;
otherwise prints JSON. Partial campaigns are rejected unless explicitly allowed.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parent.parent
T95 = [None, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365,
       2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131,
       2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069,
       2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def ratio_interval(ratios):
    assert ratios and all(math.isfinite(ratio) and ratio > 0 for ratio in ratios)
    logs = [math.log(ratio) for ratio in ratios]
    mean = statistics.fmean(logs)
    n = len(logs)
    half = T95[min(n - 1, 30)] * statistics.stdev(logs) / math.sqrt(n) if n > 1 else None
    pct = lambda x: 100 * (math.exp(x) - 1)
    return dict(blocks=n, geometric_delta_pct=pct(mean),
                ci95_low_pct=pct(mean - half) if half is not None else None,
                ci95_high_pct=pct(mean + half) if half is not None else None,
                block_delta_pct=[100 * (r - 1) for r in ratios])


def difference_interval(differences):
    """Absolute block differences preserve useful zero-valued retry counts."""
    assert differences and all(math.isfinite(value) for value in differences)
    mean = statistics.fmean(differences)
    n = len(differences)
    half = (T95[min(n - 1, 30)] * statistics.stdev(differences) / math.sqrt(n)
            if n > 1 else None)
    return dict(blocks=n, arithmetic_delta=mean,
                ci95_low=mean - half if half is not None else None,
                ci95_high=mean + half if half is not None else None,
                block_delta=differences)


def raw_rows(log):
    lines = log.read_text().splitlines()
    headers = [line for line in lines if line.startswith('mode,api,')]
    assert len(headers) == 1, log
    fields = next(csv.reader(headers))
    data = [next(csv.reader([line])) for line in lines
            if line.startswith(('disjoint,', 'shared,'))]
    assert len(data) == 1 and len(data[0]) == len(fields), log
    return dict(zip(fields, data[0]))


def metrics(row):
    ops = int(row['ops'])
    values = dict(ops_s=float(row['ops_s']),
                  worker_cpu_ns_op=int(row['worker_cpu_ns']) / ops,
                  claim_failures_op=int(row['claim_failures']) / ops)
    if int(row['perf_enabled']):
        for counter in ['cycles', 'instructions', 'branches', 'branch_misses']:
            values[counter + '_op'] = int(row['perf_' + counter]) / ops
        # This is context only, not a normalization of performance.
        values['user_cycles_per_worker_cpu_ns_proxy'] = int(row['perf_cycles']) / int(row['worker_cpu_ns'])
    return values


def audit_preconditions(metadata, binary, measured_names, complete):
    """Verify excluded warmup jobs with the same evidence checks as samples."""
    records = metadata.get('precondition_runs', [])
    assert len(records) == len({r['name'] for r in records}), 'duplicate precondition jobs'
    assert not set(r['name'] for r in records) & measured_names
    cases = metadata.get('scenario_order', [])
    validated = []
    for record in records:
        assert record['excluded_from_results'] is True
        index = record['scenario_index']
        assert 0 <= index < len(cases) and record['scenario'] == cases[index]
        scenario = record['scenario']
        profile = record['profile']
        assert profile == metadata['arguments']['precondition_profile']
        assert profile in metadata['libraries']
        threads = scenario['threads']
        row = record['row']
        assert (row['api'], int(row['threads']), row['mode']) == (scenario['api'], threads, scenario['mode'])
        assert record['returncode'] == 0
        log = ROOT / 'jobs' / record['name'] / 'output.log'
        assert digest(log) == record['log_sha256'], f'precondition log changed: {record["name"]}'
        assert raw_rows(log) == row, 'precondition CSV differs from raw log'
        persisted = json.loads(log.with_name('run.json').read_text())
        for key in ['name', 'command', 'returncode', 'started', 'finished', 'log_sha256', 'environment_overrides']:
            assert persisted[key] == record[key], (record['name'], key)
        command = record['command']
        env = record['environment_overrides']
        assert command[0] == str(binary)
        assert env['LD_LIBRARY_PATH'] == str(ROOT / 'builds' / profile / 'release/lib')
        assert int(command[2]) == int(row['cache_mib']) == metadata['arguments']['cache_mib']
        assert int(command[3]) == threads and command[5:] == [scenario['mode'], scenario['api']]
        assert float(row['seconds']) >= int(command[4]) > 0
        assert int(row['warmup_seconds']) == int(env['CACHE_PROBE_WARMUP_SECONDS']) > 0
        assert int(command[4]) == metadata['arguments']['precondition_seconds']
        assert int(row['warmup_seconds']) == metadata['arguments']['precondition_warmup']
        assert int(row['control_cpu']) == metadata['arguments']['control_cpu']
        assert int(row['ops']) > 0
        assert all(int(row[k]) == 0 for k in ['errors', 'read_bytes', 'write_bytes', 'major_faults'])
        if row['api'] == 'async':
            assert int(row['async_waits']) == 0
        workers = record['workers']
        cpus = metadata['cpu_map'][str(threads)]
        assert len(workers) == threads and [w['worker'] for w in workers] == list(range(threads))
        assert [w['tid'] for w in workers] == metadata['expected_worker_tids'][str(threads)]
        assert all(w['cpu'] == w['start_cpu'] == w['end_cpu'] == cpu for w, cpu in zip(workers, cpus))
        assert sum(w['ops'] for w in workers) == int(row['ops'])
        assert sum(w['cpu_ns'] for w in workers) == int(row['worker_cpu_ns'])
        assert int(row['perf_enabled']) == int(env['CACHE_PROBE_PERF'])
        for counter in ['cycles', 'instructions', 'branches', 'branch_misses']:
            assert sum(w[counter] for w in workers) == int(row['perf_' + counter])
        if int(row['perf_enabled']):
            assert int(row['perf_enabled_ns']) == int(row['perf_running_ns']) > 0
        validated.append(dict(name=record['name'], scenario_index=index, scenario=scenario,
                              profile=profile, warmup_seconds=int(row['warmup_seconds']),
                              measured_seconds=float(row['seconds']), excluded_from_results=True,
                              log_sha256=record['log_sha256']))
    if complete and cases:
        assert {r['scenario_index'] for r in records} == set(range(len(cases))), 'missing case precondition'
    return validated


def audit(campaign, allow_incomplete=False):
    manifest_path = campaign / 'manifest.json'
    results_path = campaign / 'results.csv'
    metadata = json.loads(manifest_path.read_text())
    args = metadata['arguments']
    rows = list(csv.DictReader(results_path.open()))
    profiles = args['profiles']
    scenarios = [(api, n, mode) for api in args['apis'] for n in args['threads'] for mode in args['modes']]
    expected = {}
    for block in range(args['blocks']):
        shift = block % len(profiles)
        order = profiles[shift:] + profiles[:shift]
        order += list(reversed(order))
        for api, threads, mode in scenarios:
            for slot, profile in enumerate(order):
                name = f'{args["label"]}-b{block:02d}-{api}-{threads}-{mode}-s{slot:02d}-{profile}'
                expected[name] = (block, api, threads, mode, slot, profile)
    assert len(rows) == len({r['job'] for r in rows}), 'duplicate result jobs'
    assert set(r['job'] for r in rows) <= set(expected), 'unexpected result jobs'
    runs = {r['name']: r for r in metadata['runs']}
    assert len(runs) == len(metadata['runs']), 'duplicate manifest jobs'
    assert set(runs) == {r['job'] for r in rows}, 'manifest/result job mismatch'
    missing = sorted(set(expected) - set(runs))
    complete = not missing and 'finished' in metadata
    assert allow_incomplete or complete, f'incomplete campaign: {len(missing)} missing jobs'
    assert rows, 'no results to analyze'
    binary = Path(next(iter(runs.values()))['command'][0])
    runner = binary.with_name('run_controlled.py')
    assert digest(binary) == metadata['driver_sha256'], 'driver changed'
    assert digest(runner) == metadata['runner_sha256'], 'runner changed'
    build_path = binary.with_suffix('.json')
    build = json.loads(build_path.read_text())
    assert build['binary_sha256'] == metadata['driver_sha256']
    assert digest(binary.with_suffix('.c')) == build['source_sha256'], 'driver source changed'
    assert digest(binary.with_suffix('.o')) == build['object_sha256'], 'driver object changed'
    assert build['dynamic'], 'expected shared driver'
    for profile, expected_hash in metadata['libraries'].items():
        assert digest(ROOT / 'builds' / profile / 'release/lib/libsplinterdb.so') == expected_hash, profile
    preconditions = metadata.get('precondition_runs', [])
    chronology = sorted([*runs.values(), *preconditions], key=lambda r: r['started'])
    assert all(a['finished'] <= b['started'] for a, b in zip(chronology, chronology[1:])), 'overlapping runs'
    lock_intervals = metadata['lock_intervals']
    if complete:
        assert all('released' in interval for interval in lock_intervals)
    for record in chronology:
        assert any(interval['acquired'] <= record['started'] and record['finished'] <= interval.get('released', float('inf'))
                   for interval in lock_intervals), 'run outside suite lock'
    if 'scenario_order' in metadata:
        case_order = [(case['api'], case['threads'], case['mode']) for case in metadata['scenario_order']]
        assert len(case_order) == len(scenarios) and set(case_order) == set(scenarios), 'incorrect scenario order'
        schedule = [(block, *case) for case in case_order for block in range(args['blocks'])]
    else:
        schedule = []
        for block in range(args['blocks']):
            shift = block % len(scenarios)
            schedule.extend((block, *case) for case in scenarios[shift:] + scenarios[:shift])
    scheduled_jobs = []
    for block, api, threads, mode in schedule:
        shift = block % len(profiles)
        order = profiles[shift:] + profiles[:shift]
        order += list(reversed(order))
        scheduled_jobs.extend(f'{args["label"]}-b{block:02d}-{api}-{threads}-{mode}-s{slot:02d}-{profile}'
                              for slot, profile in enumerate(order))
    measured_chronology = [r['name'] for r in chronology if r['name'] in runs]
    assert measured_chronology == [job for job in scheduled_jobs if job in runs], 'measured scenario/block order differs'
    validated_preconditions = audit_preconditions(metadata, binary, set(runs), complete)
    if validated_preconditions:
        case_by_job = {r['name']: r['scenario_index'] for r in preconditions}
        seen_preconditions = set()
        for record in chronology:
            if record['name'] in case_by_job:
                seen_preconditions.add(case_by_job[record['name']])
            else:
                _, api, threads, mode, _, _ = expected[record['name']]
                index = case_order.index((api, threads, mode))
                assert index in seen_preconditions, 'measurement preceded its case precondition'
    for row in rows:
        record = runs[row['job']]
        block, api, threads, mode, slot, profile = expected[row['job']]
        assert (int(row['block']), row['api'], int(row['threads']), row['mode'], int(row['slot']), row['profile']) == expected[row['job']]
        assert (record['block'], record['slot'], record['profile']) == (block, slot, profile)
        assert record['returncode'] == 0
        assert any(interval['acquired'] <= record['started'] and record['finished'] <= interval.get('released', float('inf'))
                   for interval in lock_intervals), 'run outside suite lock'
        log = ROOT / 'jobs' / row['job'] / 'output.log'
        assert digest(log) == record['log_sha256'], f'log changed: {row["job"]}'
        raw = raw_rows(log)
        assert all(row[k] == v for k, v in raw.items()), f'CSV differs from log: {row["job"]}'
        persisted = json.loads(log.with_name('run.json').read_text())
        for key in ['name', 'command', 'returncode', 'started', 'finished', 'log_sha256', 'environment_overrides']:
            assert persisted[key] == record[key], (row['job'], key)
        libdir = ROOT / 'builds' / profile / 'release/lib'
        assert record['environment_overrides']['LD_LIBRARY_PATH'] == str(libdir)
        assert record['command'][0] == str(binary)
        assert int(row['ops']) > 0 and float(row['ops_s']) > 0
        assert all(int(row[k]) == 0 for k in ['errors', 'read_bytes', 'write_bytes', 'major_faults'])
        assert int(row['cache_mib']) == args['cache_mib']
        assert int(row['warmup_seconds']) == args['warmup']
        assert int(row['control_cpu']) == args['control_cpu']
        assert float(row['seconds']) >= args['seconds']
        if api == 'async':
            assert int(row['async_waits']) == 0
        cpus = metadata['cpu_map'][str(threads)]
        workers = record['workers']
        assert len(workers) == threads and [w['worker'] for w in workers] == list(range(threads))
        assert [w['tid'] for w in workers] == metadata['expected_worker_tids'][str(threads)]
        assert all(w['cpu'] == w['start_cpu'] == w['end_cpu'] == cpu for w, cpu in zip(workers, cpus))
        assert row['worker_cpus'] == ':'.join(map(str, cpus))
        assert sum(w['ops'] for w in workers) == int(row['ops'])
        assert sum(w['cpu_ns'] for w in workers) == int(row['worker_cpu_ns'])
        for counter in ['cycles', 'instructions', 'branches', 'branch_misses']:
            assert sum(w[counter] for w in workers) == int(row['perf_' + counter])
        enabled = args['perf'] == 'all' or (args['perf'] == 'single' and threads == 1)
        assert bool(int(row['perf_enabled'])) == enabled
        if enabled:
            assert int(row['perf_enabled_ns']) == int(row['perf_running_ns']) > 0
        assert int(row['thermal_counter_increased']) == int(bool(record['throttle_increases']))
    return metadata, rows, dict(complete=complete, expected_rows=len(expected), observed_rows=len(rows),
                                missing_jobs=missing, manifest_sha256=digest(manifest_path),
                                results_sha256=digest(results_path), driver_build_sha256=digest(build_path),
                                driver_sha256=metadata['driver_sha256'], runner_sha256=metadata['runner_sha256'],
                                libraries=metadata['libraries'], all_present_raw_logs_and_job_records_verified=True,
                                scenario_order_verified=True, excluded_precondition_runs=validated_preconditions,
                                sample_exclusions=0, thermal_flagged_samples=sum(int(r['thermal_counter_increased']) for r in rows))


def report(metadata, rows, validation):
    args = metadata['arguments']
    comparisons = []
    summaries = []
    baselines = [p for p in ['main', 'original'] if p in args['profiles']]
    scenarios = [(api, n, mode) for api in args['apis'] for n in args['threads'] for mode in args['modes']]
    for api, threads, mode in scenarios:
        subset = [r for r in rows if (r['api'], int(r['threads']), r['mode']) == (api, threads, mode)]
        if not subset:
            continue
        for profile in args['profiles']:
            samples = [r for r in subset if r['profile'] == profile]
            if samples:
                values = [metrics(r) for r in samples]
                summaries.append(dict(api=api, threads=threads, mode=mode, profile=profile,
                                      samples=len(samples), means={k: statistics.fmean(v[k] for v in values) for k in values[0]}))
        for baseline in baselines:
            for profile in args['profiles']:
                if profile == baseline or (baseline == 'original' and profile == 'main'):
                    continue
                paired = []
                for block in range(args['blocks']):
                    whole_block = [r for r in subset if int(r['block']) == block]
                    if any(sum(r['profile'] == p for r in whole_block) != 2 for p in args['profiles']):
                        continue
                    candidates = [r for r in subset if int(r['block']) == block and r['profile'] == profile]
                    controls = [r for r in subset if int(r['block']) == block and r['profile'] == baseline]
                    if len(candidates) != 2 or len(controls) != 2:
                        continue
                    cvalues, bvalues = [metrics(r) for r in candidates], [metrics(r) for r in controls]
                    cmean = {k: statistics.fmean(r[k] for r in cvalues) for k in cvalues[0]}
                    bmean = {k: statistics.fmean(r[k] for r in bvalues) for k in bvalues[0]}
                    paired.append(dict(block=block, candidate_mean=cmean, baseline_mean=bmean,
                                       ratios={k: cmean[k] / bmean[k] for k in cmean
                                               if k != 'claim_failures_op'
                                               and cmean[k] > 0 and bmean[k] > 0},
                                       differences={k: cmean[k] - bmean[k] for k in cmean},
                                       thermal_flagged=any(int(r['thermal_counter_increased']) for r in candidates + controls)))
                if paired:
                    common_metrics = set.intersection(*(set(p['ratios']) for p in paired))
                    estimates = {metric: ratio_interval([p['ratios'][metric] for p in paired]) for metric in sorted(common_metrics)}
                    absolute_estimates = {metric: difference_interval([p['differences'][metric] for p in paired])
                                          for metric in sorted(paired[0]['differences'])}
                    comparisons.append(dict(api=api, threads=threads, mode=mode, profile=profile,
                                            baseline=baseline, paired_blocks=paired, estimates=estimates,
                                            absolute_estimates=absolute_estimates))
    return dict(schema_version=1, validation=validation, arguments=args, summaries=summaries, comparisons=comparisons,
                comparison_scope='Only complete scenario blocks containing both mirrored samples of every planned profile; final comparisons use all blocks. Partial summaries may include individual samples, but partial comparisons do not.',
                timing_scope='ops_s uses control phase-2 release through the last worker finish, including stopping batches. Worker CPU and hardware counters begin when each worker observes phase 2 and end after its final operation. Verification, logging, dirty writeback and eviction are outside these intervals.',
                absolute_metric_units={'ops_s': 'completed updates per second',
                                       'worker_cpu_ns_op': 'worker CPU nanoseconds per completed update',
                                       'claim_failures_op': 'explicit failed claims per completed update',
                                       'cycles_op': 'user cycles per completed update',
                                       'instructions_op': 'user retired instructions per completed update',
                                       'branches_op': 'user branches per completed update',
                                       'branch_misses_op': 'user branch misses per completed update',
                                       'user_cycles_per_worker_cpu_ns_proxy': 'user cycles per worker CPU nanosecond; not calibrated frequency'},
                metric_direction={'ops_s': 'higher is better', 'worker_cpu_ns_op': 'lower is better',
                                  'cycles_op': 'lower is better', 'instructions_op': 'lower is better',
                                  'claim_failures_op': 'descriptive contention measure only',
                                  'branches_op': 'descriptive only', 'branch_misses_op': 'descriptive only',
                                  'user_cycles_per_worker_cpu_ns_proxy': 'diagnostic only; no normalization'},
                uncertainty_method='Arithmetic mirror-pair means within each block; geometric mean and two-sided Student-t interval of block log ratios. Absolute differences use an arithmetic mean and Student-t interval of paired block differences. Blocks, not individual adjacent runs, are the replication units. Zero-valued metrics have no log ratio, and claim failures use only descriptive absolute differences.',
                limitations=['Few blocks give imprecise intervals; normal-log-ratio and independent-block assumptions may be imperfect under thermal carryover.',
                             'Intervals are pointwise, not adjusted for multiple scenarios/candidates.',
                             'No samples are silently excluded, including flagged thermal events; inspect all block ratios.',
                             'The user-cycle/thread-CPU-time proxy is not calibrated CPU frequency and is not used to normalize throughput.',
                             'Zero I/O and error checks apply to these cache probe cases; results are not database steady-state or durability benchmarks.',
                             'For degrees of freedom above 30, the t cutoff is conservatively held at 2.042.'])


def audit_working_set(metadata, rows):
    """Verify setup override and unchanged geometry in every included/excluded job."""
    binary = Path(metadata['runs'][0]['command'][0])
    driver_dir = binary.parent
    provenance = json.loads((driver_dir / 'provenance.json').read_text())
    for name, expected in provenance['parents']['controlled-final'].items():
        assert digest(driver_dir / 'baseline' / name) == expected
    for name, expected in provenance['files_sha256'].items():
        assert digest(driver_dir / name) == expected
    requested = metadata['arguments']['working_pages']
    assert isinstance(requested, int) and requested >= 32
    by_name = {row['job']: row for row in rows}
    geometries = set()
    checked = []
    for record in metadata['runs'] + metadata['precondition_runs']:
        row = record.get('row', by_name.get(record['name']))
        assert row is not None
        env = record['environment_overrides']
        assert int(env['CACHE_PROBE_WORKING_PAGES']) == requested
        assert int(row['working_pages']) == requested
        assert env['CACHE_PROBE_ADDRESS_LAYOUT'] == 'spread'
        assert env['CACHE_PROBE_DISK_RATIO'] == '16'
        log = ROOT / 'jobs' / record['name'] / 'output.log'
        lines = [line for line in log.read_text().splitlines()
                 if line.startswith('O_DIRECT address_layout=')]
        assert len(lines) == 1
        setup = dict(token.split('=', 1) for token in lines[0].split()[1:])
        assert setup['address_layout'] == env['CACHE_PROBE_ADDRESS_LAYOUT']
        assert setup['disk_ratio'] == env['CACHE_PROBE_DISK_RATIO']
        assert int(setup['cache_mib']) == int(row['cache_mib'])
        assert int(setup['resident_pages']) == requested
        # This frozen probe uses IO_DEFAULT_PAGE_SIZE=4096 and extent size=128KiB.
        assert int(setup['working_set_bytes']) == requested * 4096
        assert int(setup['disk_bytes']) == ((requested + 31) // 32) * 131072
        assert requested <= (int(row['data_bytes']) // 4096) * 3 // 4
        geometry = tuple(int(row[k]) for k in ('cache_mib', 'directory_bytes',
                         'entry_bytes', 'data_bytes', 'refcount_bytes', 'pin_bytes'))
        geometries.add(geometry)
        checked.append(dict(name=record['name'], profile=record['profile'],
                            working_pages=requested,
                            excluded_precondition=bool(record.get('excluded_from_results', False)),
                            setup=setup))
    assert len(geometries) == 1, 'cache geometry changed between compared processes'
    return dict(working_pages=requested, checked_jobs=len(checked),
                measured_jobs=len(metadata['runs']),
                excluded_preconditions=len(metadata['precondition_runs']),
                geometry_fields=['cache_mib', 'directory_bytes', 'entry_bytes',
                                 'data_bytes', 'refcount_bytes', 'pin_bytes'],
                identical_geometry=list(next(iter(geometries))), jobs=checked,
                interpretation='Small working set with identical cache/directory geometry and spread addresses; offset-zero page lines may conflict in L1. This measures resident cache mutation ownership and dirtying; it is not complete insertion evidence.')


def audit_write_counters(metadata, rows):
    args = metadata['arguments']
    binary = Path(metadata['runs'][0]['command'][0])
    by_name = {row['job']: row for row in rows}
    checked = []
    for record in metadata['runs'] + metadata['precondition_runs']:
        row = record.get('row', by_name.get(record['name']))
        assert row['api'] == 'write' and row['mode'] in ('disjoint', 'shared')
        threads = int(row['threads'])
        command = record['command']
        env = record['environment_overrides']
        cpus = metadata['cpu_map'][str(threads)]
        excluded = bool(record.get('excluded_from_results', False))
        seconds = args['precondition_seconds'] if excluded else args['seconds']
        warmup = args['precondition_warmup'] if excluded else args['warmup']
        assert len(command) == 7 and command[0] == str(binary)
        assert [int(command[2]), int(command[3]), int(command[4])] == [args['cache_mib'], threads, seconds]
        assert command[5:] == [row['mode'], 'write']
        assert env['CACHE_PROBE_WORKER_CPUS'] == ','.join(map(str, cpus))
        assert int(env['CACHE_PROBE_CONTROL_CPU']) == int(row['control_cpu']) == args['control_cpu']
        assert int(env['CACHE_PROBE_WARMUP_SECONDS']) == int(row['warmup_seconds']) == warmup
        enabled = args['perf'] == 'all' or (args['perf'] == 'single' and threads == 1)
        assert int(env['CACHE_PROBE_PERF']) == int(row['perf_enabled']) == int(enabled)
        elapsed = float(row['seconds'])
        rate = float(row['ops_s'])
        assert math.isfinite(elapsed) and elapsed >= seconds > 0
        assert math.isfinite(rate) and rate > 0
        # CSV prints elapsed to nine decimals and rate to one decimal.
        tolerance = 0.050001 + rate * 0.5e-9 / elapsed
        assert abs(int(row['ops']) / elapsed - rate) <= tolerance
        workers = record['workers']
        assert all(w['ops'] > 0 and w['cpu_ns'] > 0 for w in workers)
        assert all(w['all_ops'] >= w['ops'] and w['all_claim_failures'] >= w['claim_failures'] >= 0
                   for w in workers)
        for field in ('ops', 'all_ops', 'claim_failures', 'all_claim_failures'):
            assert sum(w[field] for w in workers) == int(row[field])
        assert int(row['all_ops']) == int(row['verified_updates']) > 0
        assert int(row['warmup_ops']) == int(row['all_ops']) - int(row['ops']) >= 0
        assert int(row['all_claim_failures']) >= int(row['claim_failures']) >= 0
        assert 0 < int(row['dirty_pages']) <= int(row['working_pages'])
        assert int(row['async_waits']) == 0
        if not enabled:
            assert all(int(row['perf_' + k]) == 0 for k in
                       ['cycles', 'instructions', 'branches', 'branch_misses', 'enabled_ns', 'running_ns'])
        if row['mode'] == 'disjoint':
            assert int(row['all_claim_failures']) == 0
        log = ROOT / 'jobs' / record['name'] / 'output.log'
        lines = log.read_text().splitlines()
        verifies = [line for line in lines if line.startswith('VERIFY ')]
        assert len(verifies) == 1
        v = {k: int(value) for k, value in
             (token.split('=', 1) for token in verifies[0].split()[1:])}
        assert v['pages'] == int(row['working_pages'])
        assert v['observed'] == v['expected'] == int(row['all_ops'])
        assert v['errors'] == 0
        raw_workers = [{k: int(value) for k, value in
                       (token.split('=', 1) for token in line.split()[1:])}
                       for line in lines if line.startswith('WORKER ')]
        assert raw_workers == workers
        checked.append(dict(name=record['name'], mode=row['mode'],
                            threads=int(row['threads']), all_ops=int(row['all_ops']),
                            measured_ops=int(row['ops']), warmup_ops=int(row['warmup_ops']),
                            claim_failures=int(row['claim_failures']),
                            all_claim_failures=int(row['all_claim_failures']),
                            dirty_pages=int(row['dirty_pages']), verification=v))
    return dict(checked_jobs=len(checked), all_counter_sums_exact=True,
                all_identity_and_complement_checks_passed=True, jobs=checked)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', help='result label or full result directory')
    parser.add_argument('--allow-incomplete', action='store_true')
    parser.add_argument('--output', type=Path, help='new JSON file; existing files are refused')
    args = parser.parse_args()
    campaign = Path(args.campaign)
    if not campaign.is_dir():
        campaign = ROOT / 'probe-results' / args.campaign
    metadata, rows, validation = audit(campaign, args.allow_incomplete)
    working_set_audit = audit_working_set(metadata, rows)
    write_counter_audit = audit_write_counters(metadata, rows)
    result = report(metadata, rows, validation)
    result['working_set_audit'] = working_set_audit
    result['write_counter_audit'] = write_counter_audit
    result['limitations'].append('Counters and CPU/op include address generation, identity/ref/complement checks, bookkeeping, get/claim/lock/update/unlock/unclaim/unget, and retries; they are not isolated function costs. Claim-failure counts exclude waiting inside blocking cache_get.')
    result['limitations'].append('Offset-zero page lines may conflict in L1 even for a 32-page working set. Cache/directory capacity and deterministic spread layout stay fixed across profiles.')
    result['limitations'].append('Pages are ordinarily already dirty after warmup. Disjoint/shared per-worker working sets differ; compare profiles within modes, not modes as a pure contention contrast. This cannot establish database insertion neutrality.')
    result['campaign'] = str(campaign.resolve())
    result['report_script_sha256'] = digest(__file__)
    serialized = json.dumps(result, indent=2) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(serialized)
        print(args.output)
    else:
        print(serialized, end='')


if __name__ == '__main__':
    main()
