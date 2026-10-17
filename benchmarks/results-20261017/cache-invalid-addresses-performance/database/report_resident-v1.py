#!/usr/bin/env python3
"""Read-only audit of one frozen resident-query campaign; never starts workloads.

By default require the full matrix. --allow-incomplete keeps all validated
samples but computes comparisons from complete mirrored blocks only. Output
must be a new file. Separate invocations keep different candidate cohorts apart.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

from report_quota import digest, read_csv, interval

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
METRICS = ['ops_s', 'cpu_us_op', 'p99_us']


def provenance(manifest):
    driver = manifest['driver']
    for field in ['source', 'binary']:
        assert digest(driver[field]) == driver[field + '_sha256'], field
    assert digest(HERE / 'run.py') == manifest['runner_sha256'], 'runner changed'
    build = json.loads((HERE / 'dynamic-build.json').read_text())
    assert build['built'] and build['linkage'] == 'dynamic'
    assert build['binaries']['kvbench'] == driver
    counts = {}
    for profile, entry in manifest['provenance'].items():
        tree = ROOT / 'sources' / profile
        assert digest(ROOT / 'builds' / profile / 'release/lib/libsplinterdb.so') == entry['library_sha256'], profile
        assert digest(tree / 'src/clockcache.c') == entry['clockcache_source_sha256'], profile
        counts[profile] = 1
        if 'source_manifest' in entry:
            source_path = ROOT / (profile + '-source.json')
            assert digest(source_path) == entry['source_manifest_sha256'], profile
            assert json.loads(source_path.read_text()) == entry['source_manifest']
            for name, sha in entry['source_manifest']['hashes'].items():
                assert digest(tree / name) == sha, (profile, name)
            counts[profile] = len(entry['source_manifest']['hashes'])
    return dict(driver_sha256=driver['binary_sha256'], driver_source_sha256=driver['source_sha256'],
                runner_sha256=manifest['runner_sha256'], build_manifest_sha256=digest(HERE / 'dynamic-build.json'),
                source_file_hashes_checked=counts,
                library_hashes={p: v['library_sha256'] for p, v in manifest['provenance'].items()})


def expected_command(directory, manifest, name, profile, index, kind):
    args = manifest['arguments']
    seconds = args['precondition_seconds'] if kind == 'precondition' else args['seconds']
    command = ['taskset', '-c', args['cpus'], manifest['driver']['binary'],
               '--db', str(directory / (name + '.db')), '--n', '200000', '--cache-mib', '128',
               '--disk-gib', '8', '--log', '0', '--threads', '8', '--mt-bg', '0', '--normal-bg', '0',
               '--memtable-mib', '4', '--secs', str(seconds), '--warmup', str(args['warmup']),
               '--csv', str(directory / (name + '.csv')), '--vtag', profile, '--stag', 'resident',
               '--otag', str(index), '--rep', str(index), '--seed', '42', '--hang-secs', '60',
               '--hang-dir', str(directory)]
    command += ['--create', 'load:random@1'] if kind == 'fixture' else ['--open', 'warm@1', *args['phases']]
    return command


def check_rows(rows, profile, index, phases, duration):
    assert [r['phase'] for r in rows] == phases, 'phase order/count'
    for row in rows:
        assert row['version'] == profile and row['size'] == 'resident'
        assert int(row['order']) == int(row['rep']) == index
        assert int(row['n']) == 200000 and row['log'] == '0'
        assert int(row['threads']) == int(row['phase'].split('@')[1])
        assert all(int(row[key]) == 0 for key in ['errors', 'violations', 'swapin', 'swapout', 'majflt'])
        ops, seconds, rate = int(row['ops']), float(row['secs']), float(row['ops_s'])
        assert ops > 0 and seconds > 0 and rate > 0
        # Wall seconds are printed to 3 decimals and rate to 1 decimal.
        assert abs(ops / rate - seconds) <= 0.000501 + ops / rate**2 * 0.051
        if row['phase'].startswith(('warm', 'load')):
            assert ops == 200000
            if row['phase'].startswith('warm'):
                assert int(row['tuples']) == 200000
        else:
            assert abs(seconds - duration) <= 0.001
            assert int(row['rd_bytes']) == 0, 'timed query performed physical reads'
            assert float(row['cpu_s']) > 0
            assert all(math.isfinite(float(row[key])) and float(row[key]) > 0 for key in ['p99_us', 'mean_us'])
            if row['phase'].startswith('range'):
                length = int(row['phase'].split(':')[2].split('@')[0])
                assert ops <= int(row['tuples']) <= ops * length


def audit(directory, allow_incomplete=False):
    manifest_bytes = (directory / 'manifest.json').read_bytes()
    m = json.loads(manifest_bytes)
    args = m['arguments']
    assert m['mode'] == args['mode'] == 'resident'
    assert not args['fixed_records'] and not args['insert_diagnostics']
    assert args['seconds'] > 0 and args['warmup'] >= 0 and args['blocks'] > 0
    profiles = args['profiles']
    assert profiles and len(profiles) == len(set(profiles))
    assert set(m['provenance']) == set(profiles + ['main'])
    order = []
    for block in range(args['blocks']):
        shift = block % len(profiles) if args['rotate_blocks'] else 0
        rotated = profiles[shift:] + profiles[:shift]
        order += rotated + rotated[::-1]
    assert order == m['order']
    expected = [('fixture', 'main', -1, 'fixture')]
    if args['precondition_seconds']:
        expected.append(('precondition', profiles[0], -1, 'precondition'))
    expected += [(f'{index:02d}-{profile}', profile, index, 'measured') for index, profile in enumerate(order)]
    assert [r['name'] for r in m['runs']] == [r[0] for r in expected[:len(m['runs'])]]
    validated = [r for r in m['runs'] if r.get('correctness_passed') and
                 (r['name'] == 'fixture' or r.get('own_successful_scratch_removed'))]
    assert validated == m['runs'][:len(validated)], 'completed runs are not a prefix'
    complete = m['complete'] and len(validated) == len(expected)
    assert allow_incomplete or complete, 'incomplete campaign'
    prov = provenance(m)
    assert m['filesystem'].startswith('ext4 /dev/') and m['io'] == 'O_DIRECT'
    assert m['machine_lock'] == '/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/machine.lock'
    prior_end = m['lock_acquired']
    samples, preconditions, raw_rows, records = [], [], [], []
    for record, (name, profile, index, kind) in zip(validated, expected):
        assert record['profile'] == profile and record['returncode'] == 0
        assert record['command'] == expected_command(directory, m, name, profile, index, kind)
        assert record['environment_overrides'] == dict(LD_LIBRARY_PATH=str(ROOT / 'builds' / profile / 'release/lib'),
                                                       TMPDIR=str(directory / (name + '-tmp')))
        assert record['library_sha256'] == prov['library_hashes'][profile]
        assert record['free_bytes_before'] >= args['min_free_gib'] * 2**30
        assert prior_end <= record['started'] <= record['finished'] <= m.get('finished', math.inf)
        prior_end = record['finished']
        assert record['started'] <= record['hardware_before']['time'] <= record['hardware_after']['time']
        assert digest(directory / (name + '.log')) == record['log_sha256']
        csv_sha = digest(directory / (name + '.csv'))
        if kind != 'fixture':
            assert csv_sha == record['csv_sha256']
            assert record['excluded_precondition'] == (kind == 'precondition')
            assert not (directory / (name + '.db')).exists()
        rows = read_csv(directory / (name + '.csv'))
        phases = ['load:random@1'] if kind == 'fixture' else ['warm@1', *args['phases']]
        duration = args['precondition_seconds'] if kind == 'precondition' else args['seconds']
        check_rows(rows, profile, index, phases, duration)
        if kind != 'fixture':
            assert record['validated_rows'] == len(rows)
            log = (directory / (name + '.log')).read_text()
            found_counts = re.findall(r'found=(\d+) notfound=(\d+)', log)
            assert len(found_counts) == sum(p.startswith('point') for p in phases)
            point_rows = [r for r in rows if r['phase'].startswith('point')]
            assert all(int(found) == int(row['ops']) and int(missing) == 0
                       for (found, missing), row in zip(found_counts, point_rows))
            if kind == 'measured':
                raw_rows.extend(rows)
            destination = samples if kind == 'measured' else preconditions
            for row in rows[1:]:
                destination.append(dict(name=name, profile=profile, phase=row['phase'],
                                        block=index // (2 * len(profiles)), ops=int(row['ops']),
                                        seconds=float(row['secs']), ops_s=float(row['ops_s']),
                                        cpu_us_op=float(row['cpu_s']) * 1e6 / int(row['ops']),
                                        p99_us=float(row['p99_us']), process_read_bytes=int(row['rd_bytes']),
                                        process_write_bytes=int(row['wr_bytes']), tuples=int(row['tuples'])))
        records.append(dict(name=name, profile=profile, kind=kind, command=record['command'],
                            started=record['started'], finished=record['finished'],
                            log_sha256=record['log_sha256'], csv_sha256=csv_sha,
                            csv_hash_recorded_by_runner=kind != 'fixture'))
    if complete:
        assert m['fixture_removed_after_all_queries_passed'] and not (directory / 'fixture.db').exists()
        assert re.fullmatch('[0-9a-f]{64}', m['fixture_sha256'])
        assert read_csv(directory / 'results.csv') == raw_rows
    blocks = [b for b in range(args['blocks']) if all(
        sum(row['block'] == b and row['profile'] == p and row['phase'] == phase for row in samples) == 2
        for p in profiles for phase in args['phases'])]
    return dict(campaign=directory.name, directory=str(directory), arguments=args,
                validation=dict(complete=complete, expected_measured_processes=len(order),
                                measured_processes=len({row['name'] for row in samples}), measured_phases=len(samples),
                                excluded_precondition_phases=len(preconditions), complete_blocks=blocks,
                                pending_runs=[r[0] for r in expected[len(validated):]],
                                manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(), **prov,
                                lock_acquired=m['lock_acquired'], finished=m.get('finished'),
                                fixture_hash_recorded=m.get('fixture_sha256'),
                                checks=['Commands, matrix, rotated mirror order and excluded precondition',
                                        'Frozen driver/build/runner/source/library/log/query CSV hashes',
                                        'Full warm count, point found count, errors/violations/swaps/faults, zero timed read bytes',
                                        'Aggregate CSV identity and serial processes within shared-lock interval']),
                samples=samples, excluded_precondition_samples=preconditions, records=records)


def summarize(campaign):
    args, rows = campaign['arguments'], campaign['samples']
    blocks = campaign['validation']['complete_blocks']
    summaries, comparisons = [], []
    for phase in args['phases']:
        for profile in args['profiles']:
            group = [r for r in rows if r['phase'] == phase and r['profile'] == profile]
            if group:
                summaries.append(dict(phase=phase, profile=profile, samples=len(group),
                    all_expected_samples_present=len(group) == 2 * args['blocks'],
                    **{metric + '_mean': statistics.fmean(row[metric] for row in group) for metric in METRICS},
                    ops=sum(r['ops'] for r in group), process_read_bytes=sum(r['process_read_bytes'] for r in group),
                    process_write_bytes=sum(r['process_write_bytes'] for r in group)))
        if not blocks:
            continue
        for baseline in [p for p in ['main', 'original'] if p in args['profiles']]:
            for profile in args['profiles']:
                if profile == baseline or (baseline == 'original' and profile == 'main'):
                    continue
                estimates = {}
                for metric in METRICS:
                    ratios = []
                    for block in blocks:
                        candidate, control = [statistics.fmean(r[metric] for r in rows if r['phase'] == phase
                                                               and r['block'] == block and r['profile'] == p)
                                              for p in [profile, baseline]]
                        ratios.append(candidate / control)
                    estimates[metric] = interval(ratios, True)
                comparisons.append(dict(phase=phase, profile=profile, baseline=baseline,
                                        complete_block_numbers=blocks, estimates=estimates))
    campaign.update(summaries=summaries, comparisons=comparisons)
    return campaign


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign')
    parser.add_argument('--allow-incomplete', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    assert args.output is None or not args.output.exists(), 'will not replace a report'
    directory = Path(args.campaign)
    if not directory.is_dir():
        directory = HERE / 'results' / args.campaign
    report = summarize(audit(directory.resolve(), args.allow_incomplete))
    report.update(auditor_sha256=digest(__file__), helper_sha256=digest(HERE / 'report_quota.py'),
                  uncertainty_method='Arithmetic mirror-pair means within each complete block; pointwise 95% Student t intervals on block log ratios. No samples excluded; one block has no interval. Separate cohorts are not pooled.',
                  limitations=['Only the named phases, worker counts and resident data size are covered.',
                               'Three blocks and serial phase order do not establish a universal 2-3% performance bound; laptop DVFS and thermal carryover remain possible confounders.',
                               'CPU/wall seconds in query CSV are rounded to milliseconds; printed throughput has separate rounding.',
                               'Process write bytes include watchdog log output. Zero timed read bytes does not imply zero total database I/O including warmup and close.',
                               'Hardware snapshots are whole-process observational context, not timed frequency/throttle measurements or normalization.',
                               'The deleted fixture cannot be rehashed. Its creation hash is recorded; the fixture CSV has no original runner-recorded hash, so this audit supplies a current hash only.',
                               'Recorded serial lock intervals do not exclude unrecorded external activity.'])
    rendered = json.dumps(report, indent=2, allow_nan=False) + '\n'
    if args.output:
        with args.output.open('x') as stream:
            stream.write(rendered)
        print(args.output)
    else:
        print(rendered, end='')


if __name__ == '__main__':
    main()
