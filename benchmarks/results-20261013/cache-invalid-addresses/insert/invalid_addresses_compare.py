#!/usr/bin/env python3
"""Isolate invalid-address validation overhead against compact cache-errors.

Invoke with run_job.py benchmark mode: it holds benchmark.lock and the exclusive
machine.lock for the whole campaign. Images live on verified physical storage;
every query fixture copy is fsynced before O_DIRECT open. Only this runner's
successfully closed scratch image may be removed between measured processes.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parent.parent
COORD = ROOT / 'coordination/cache-invalid-addresses'
ORDER = ['parent', 'validated', 'validated', 'parent']
PHASES = ['warm', 'point:uniform@1', 'point:uniform@8',
          'range:uniform:1@1', 'range:uniform:1@8',
          'range:uniform:10000@1', 'range:uniform:10000@8']


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def read_csv(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


def write_csv(path, rows):
    with path.open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)


def provenance():
    records = {label: json.loads((COORD / f'{label}-build.json').read_text()) for label in set(ORDER)}
    for label, record in records.items():
        tree = Path(record['tree'])
        for relative, expected in record['source_files_sha256'].items():
            assert digest(tree / relative) == expected, (label, relative)
        assert digest(record['library']) == record['library_sha256'], label
        for build in record['builds'].values():
            assert digest(build['binary']) == build['binary_sha256'], build['binary']
    assert records['parent']['flags'] == records['validated']['flags']
    return records


def prepare_scratch(out, manifest):
    db = out / 'probe.db'
    if db.exists():
        assert manifest['runs'] and manifest['runs'][-1]['correctness_passed'], db
        db.unlink()
    return db


def finish_run(out, manifest, record, process, db):
    record.update(returncode=process.returncode, finished=time.time(),
                  db_allocated_bytes=db.stat().st_blocks * 512, db_apparent_bytes=db.stat().st_size)
    manifest['runs'].append(record)
    write_json(out / 'manifest.json', manifest)
    assert process.returncode == 0, record


def query_summary(out, rows):
    summaries = []
    for phase in PHASES[1:]:
        groups = {label: [row for row in rows if row['phase'] == phase and row['version'] == label]
                  for label in set(ORDER)}
        assert all(len(group) == 2 for group in groups.values())
        mean = lambda label, key: statistics.fmean(float(row[key]) for row in groups[label])
        for label in ('parent', 'validated'):
            rates = [float(row['ops_s']) for row in groups[label]]
            record = dict(phase=phase, version=label, samples=len(rates),
                          ops_s_mean=statistics.fmean(rates), ops_s_min=min(rates), ops_s_max=max(rates),
                          throughput_delta_parent_pct=100 * (mean(label, 'ops_s') / mean('parent', 'ops_s') - 1),
                          cpu_us_op=statistics.fmean(1e6 * float(row['cpu_s']) / int(row['ops']) for row in groups[label]),
                          cpu_efficiency_delta_parent_pct=100 * (mean(label, 'ops_per_cpu_s') / mean('parent', 'ops_per_cpu_s') - 1),
                          read_B_op=mean(label, 'rd_B_op'), write_B_op=mean(label, 'wr_B_op'),
                          p99_us=mean(label, 'p99_us'), errors=0, violations=0)
            summaries.append(record)
            print(phase, label, f'{record["ops_s_mean"]:.1f} ops/s [{min(rates):.1f},{max(rates):.1f}], '
                  f'{record["throughput_delta_parent_pct"]:+.2f}%, CPUeff '
                  f'{record["cpu_efficiency_delta_parent_pct"]:+.2f}%', flush=True)
    write_csv(out / 'summary.csv', summaries)


def run_queries(mode, out, manifest):
    resident = mode == 'resident'
    fixture = ROOT / f'benchmarks/lazy-merge/{mode}-log0/0-baseline.db'
    manifest.update(fixture=str(fixture), fixture_sha256=digest(fixture), fixture_copy_fsynced=True,
                    n=200000 if resident else 2000000, cache_mib=128 if resident else 64,
                    seconds=20 if resident else 10, phases=PHASES, log=0)
    write_json(out / 'manifest.json', manifest)
    for index, label in enumerate(ORDER):
        db = prepare_scratch(out, manifest)
        copy_started = time.time()
        subprocess.run(['cp', '--reflink=auto', '--sparse=always', str(fixture), str(db)], check=True)
        with db.open('r+b') as image:
            os.fsync(image.fileno())
        copy_finished = time.time()
        build = manifest['build_provenance'][label]['builds']['kvbench']
        argv = ['taskset', '-c', '0-11', build['binary'], '--db', str(db), '--open',
                '--n', str(manifest['n']), '--cache-mib', str(manifest['cache_mib']),
                '--disk-gib', '8', '--log', '0', '--threads', '8', '--mt-bg', '1',
                '--normal-bg', '3', '--memtable-mib', '4', '--secs', str(manifest['seconds']),
                '--warmup', '1', '--csv', str(out / 'results.csv'), '--vtag', label,
                '--stag', mode, '--otag', str(index), '--rep', str(index // 2),
                '--seed', '42', '--hang-secs', '60', '--hang-dir', str(out), *PHASES]
        record = dict(index=index, version=label, command=argv, started=time.time(),
                      correctness_passed=False, binary_sha256=build['binary_sha256'],
                      copy_started=copy_started, copy_fsync_finished=copy_finished)
        print('invalid-addresses', mode, index + 1, '/', len(ORDER), label, flush=True)
        with (out / f'{index}-{label}.log').open('w') as stream:
            process = subprocess.run(argv, stdout=stream, stderr=subprocess.STDOUT, timeout=600)
        finish_run(out, manifest, record, process, db)
        rows = read_csv(out / 'results.csv')
        current = [row for row in rows if row['order'] == str(index)]
        assert len(current) == len(PHASES), current
        assert all(int(row['ops']) > 0 and int(row['errors']) == 0 and int(row['violations']) == 0
                   for row in current), current
        if resident:
            assert all(float(row['rd_B_op']) == 0 for row in current if row['phase'] != 'warm'), current
        record['correctness_passed'] = True
        write_json(out / 'manifest.json', manifest)
    query_summary(out, rows)


def insert_summary(out, rows):
    write_csv(out / 'results.csv', rows)
    summaries = []
    for wal in (0, 1):
        groups = {label: [row for row in rows if row['wal'] == wal and row['version'] == label]
                  for label in set(ORDER)}
        assert all(len(group) == 2 for group in groups.values())
        mean = lambda label, key: statistics.fmean(row[key] for row in groups[label])
        for label in ('parent', 'validated'):
            rates = [row['ops_s'] for row in groups[label]]
            record = dict(wal=wal, threads=8, version=label, samples=len(rates),
                          ops_s_mean=mean(label, 'ops_s'), ops_s_min=min(rates), ops_s_max=max(rates),
                          throughput_delta_parent_pct=100 * (mean(label, 'ops_s') / mean('parent', 'ops_s') - 1),
                          drain_delta_parent_pct=100 * (mean(label, 'drain_ops_s') / mean('parent', 'drain_ops_s') - 1),
                          cpu_us_op=mean(label, 'cpu_us_op'),
                          cpu_efficiency_delta_parent_pct=100 * (mean(label, 'ops_per_cpu_s') / mean('parent', 'ops_per_cpu_s') - 1),
                          p99_us=mean(label, 'p99_us'), max_ms=max(row['max_ms'] for row in groups[label]),
                          close_s=mean(label, 'close_s'), read_B_op=mean(label, 'read_B_op'),
                          write_B_op=mean(label, 'write_B_op'))
            summaries.append(record)
            print('insert WAL', wal, label, f'{record["ops_s_mean"]:.1f} ops/s '
                  f'[{min(rates):.1f},{max(rates):.1f}], '
                  f'{record["throughput_delta_parent_pct"]:+.2f}%, CPUeff '
                  f'{record["cpu_efficiency_delta_parent_pct"]:+.2f}%', flush=True)
    write_csv(out / 'summary.csv', summaries)


def run_insert(out, manifest):
    manifest.update(records=20000000, seconds=0, report_ms=20, threads=8,
                    cache_mib=256, memtable_mib=8, wal_modes=[0, 1])
    write_json(out / 'manifest.json', manifest)
    rows = []
    for wal in (0, 1):
        for index, label in enumerate(ORDER):
            prefix = out / f'wal{wal}-t8-{index}-{label}'
            db = prepare_scratch(out, manifest)
            build = manifest['build_provenance'][label]['builds']['insert_benchmark']
            argv = ['taskset', '-c', '0-11', build['binary'], '--db', str(db),
                    '--csv', str(prefix.with_suffix('.csv')), '--summary', str(prefix.with_suffix('.summary.csv')),
                    '--records', '20000000', '--seconds', '0', '--report-ms', '20',
                    '--cache-mib', '256', '--disk-gib', '16', '--direct',
                    '--inserters', '8', '--readers', '0', '--memtable-bg', '1', '--normal-bg', '3',
                    '--memtable-mib', '8', '--key-size', '16', '--value-size', '16',
                    '--min-free-gib', '12', '--seed', '42']
            if not wal:
                argv += ['--no-log']
            record = dict(wal=wal, threads=8, index=index, version=label, command=argv,
                          binary_sha256=build['binary_sha256'], started=time.time(), correctness_passed=False)
            print('invalid-addresses insert:', wal, index, label, flush=True)
            with prefix.with_suffix('.log').open('w') as stream:
                process = subprocess.run(argv, stdout=stream, stderr=subprocess.STDOUT, timeout=180)
            finish_run(out, manifest, record, process, db)
            summaries = read_csv(prefix.with_suffix('.summary.csv'))
            assert len(summaries) == 1
            summary = summaries[0]
            inserted = int(summary['inserted'])
            assert inserted == 20000000, summary
            log = prefix.with_suffix('.log').read_text()
            assert re.search(r'load_end .* error=0(?: |\n)', log), prefix
            close = re.search(r'close_s=([\d.]+) close_rc=0', log)
            assert close, prefix
            intervals = read_csv(prefix.with_suffix('.csv'))
            cpu_s = sum(float(row['cpu_cores']) * float(row['interval_s']) for row in intervals)
            rows.append(dict(wal=wal, threads=8, version=label, index=index,
                             ops_s=float(summary['achieved_insert_ops_s']),
                             drain_ops_s=inserted / (float(summary['elapsed_s']) + float(close[1])),
                             close_s=float(close[1]), inserted=inserted,
                             p99_us=1000 * float(summary['insert_service_p99_ms']),
                             max_ms=float(summary['insert_service_max_ms']),
                             cpu_us_op=cpu_s * 1e6 / inserted, ops_per_cpu_s=inserted / cpu_s,
                             read_B_op=sum(float(row['read_mib_s']) * float(row['interval_s'])
                                           for row in intervals) * 2**20 / inserted,
                             write_B_op=sum(float(row['write_mib_s']) * float(row['interval_s'])
                                            for row in intervals) * 2**20 / inserted))
            record['correctness_passed'] = True
            write_json(out / 'manifest.json', manifest)
            write_csv(out / 'results.csv', rows)
    insert_summary(out, rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['resident', 'disk', 'insert'])
    args = parser.parse_args()
    out = ROOT / 'benchmarks/cache-invalid-addresses' / args.mode
    out.mkdir(parents=True, exist_ok=False)
    filesystem = subprocess.check_output(['findmnt', '-n', '-o', 'FSTYPE,SOURCE', '-T', str(out)], text=True).strip()
    fstype, source = filesystem.split()[:2]
    assert fstype not in {'tmpfs', 'ramfs'} and source.startswith('/dev/'), filesystem
    manifest = dict(project='cache-invalid-addresses', regime=args.mode, order=ORDER,
                    output_directory=str(out), filesystem=filesystem, io='O_DIRECT', cpu_affinity='0-11',
                    scope='invalid disk address validation against compact cache-errors parent',
                    build_provenance=provenance(), runner_sha256=digest(__file__), runs=[])
    write_json(out / 'manifest.json', manifest)
    (out / 'invalid_addresses_compare.py').write_bytes(Path(__file__).read_bytes())
    source_name = 'insert_benchmark.c' if args.mode == 'insert' else 'kvbench.c'
    (out / source_name).write_bytes((ROOT / 'baseline/benchmarks' / source_name).read_bytes())
    if args.mode == 'insert':
        run_insert(out, manifest)
    else:
        run_queries(args.mode, out, manifest)
    assert len(manifest['runs']) == len(ORDER) * (2 if args.mode == 'insert' else 1)
    assert all(run['correctness_passed'] for run in manifest['runs'])
    manifest['complete'] = True
    write_json(out / 'manifest.json', manifest)
    print('Completed', out, flush=True)


if __name__ == '__main__':
    main()
