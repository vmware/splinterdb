#!/usr/bin/env python3
"""Measure disk-format layering changes on the same format-8 fixture.

Invoke through run_job.py benchmark mode; it supplies the campaign-wide
benchmark/machine locks and verifies the database directory is on real disk.
The runner never changes the source fixture. Each private copy is fsynced
before O_DIRECT opening and is retained with its successful or failed run.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time


ROOT = Path(__file__).resolve().parent.parent
TREE = ROOT / 'worktrees/disk-format'
BEFORE = ROOT / 'coordination/disk-format-layering-before'
PHASES = ['warm', 'point:uniform@1', 'point:uniform@8',
          'range:uniform:1@1', 'range:uniform:1@8']


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def csv_rows(path):
    with path.open() as source:
        return list(csv.DictReader(source))


def save_csv(path, rows):
    with path.open('w') as target:
        writer = csv.DictWriter(target, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def provenance(candidate_build):
    prior = json.loads((BEFORE / 'preservation.json').read_text())
    for name, record in prior['files'].items():
        assert digest(BEFORE / name) == record['sha256'], name
    binary = TREE / candidate_build / 'release/bin/kvbench'
    library = TREE / candidate_build / 'release/lib/libsplinterdb.a'
    build = json.loads(binary.with_suffix('.json').read_text())
    assert build['binary_sha256'] == digest(binary), 'candidate binary identity'
    assert build['source_sha256'] == digest(BEFORE / 'kvbench.c'), 'benchmark source changed'
    diff = subprocess.check_output(['git', 'diff', 'HEAD', '--'], cwd=TREE)
    candidate = dict(build_record=build, binary=str(binary), binary_sha256=digest(binary),
                     library=str(library), library_sha256=digest(library),
                     source_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'],
                                                         cwd=TREE, text=True).strip(),
                     tracked_diff_sha256=hashlib.sha256(diff).hexdigest(),
                     tracked_diff_bytes=len(diff))
    return {'before': BEFORE / 'kvbench', 'layered': binary}, {
        'before': prior, 'layered': candidate}, diff


def summarize(out, rows, rounds):
    result = []
    lines = ['phase | before ops/s | layered ops/s | throughput change | CPU us/op change']
    for phase in PHASES[1:]:
        groups = {label: [row for row in rows if row['version'] == label
                          and row['phase'] == phase] for label in ['before', 'layered']}
        assert all(len(group) == 2 * rounds for group in groups.values()), (phase, groups)
        rates = {label: [float(row['ops_s']) for row in group]
                 for label, group in groups.items()}
        mean = lambda label, key: statistics.fmean(float(row[key]) for row in groups[label])
        cpu = {label: statistics.fmean(1e6 * float(row['cpu_s']) / int(row['ops'])
                                      for row in group) for label, group in groups.items()}
        before = statistics.fmean(rates['before'])
        layered = statistics.fmean(rates['layered'])
        record = dict(phase=phase, threads=int(phase.rsplit('@', 1)[1]), samples_per_variant=2 * rounds,
                      before_ops_s=before, layered_ops_s=layered,
                      throughput_change_pct=100 * (layered / before - 1),
                      before_min_ops_s=min(rates['before']), before_max_ops_s=max(rates['before']),
                      layered_min_ops_s=min(rates['layered']), layered_max_ops_s=max(rates['layered']),
                      before_cpu_us_op=cpu['before'], layered_cpu_us_op=cpu['layered'],
                      cpu_us_op_change_pct=100 * (cpu['layered'] / cpu['before'] - 1),
                      before_read_B_op=mean('before', 'rd_B_op'),
                      layered_read_B_op=mean('layered', 'rd_B_op'),
                      before_write_B_op=mean('before', 'wr_B_op'),
                      layered_write_B_op=mean('layered', 'wr_B_op'),
                      before_p99_us=mean('before', 'p99_us'), layered_p99_us=mean('layered', 'p99_us'))
        result.append(record)
        lines.append(f'{phase} | {before:.0f} | {layered:.0f} | '
                     f'{record["throughput_change_pct"]:+.2f}% | '
                     f'{record["cpu_us_op_change_pct"]:+.2f}%')
    save_csv(out / 'summary.csv', result)
    report = '\n'.join(lines) + '\n'
    (out / 'summary.txt').write_text(report)
    print(report, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=15)
    parser.add_argument('--rounds', type=int, default=1)
    parser.add_argument('--candidate-build', default='build-layering')
    parser.add_argument('--output-name', default='resident-layering-20261012')
    args = parser.parse_args()
    assert args.seconds >= 10 and args.rounds >= 1
    assert Path(args.output_name).name == args.output_name
    paths, builds, diff = provenance(args.candidate_build)
    fixture = ROOT / 'benchmarks/disk-format/resident-log0-final-singleload/1-candidate.db'
    out = ROOT / 'benchmarks/disk-format' / args.output_name
    out.mkdir(parents=True, exist_ok=False)
    storage = subprocess.check_output(['findmnt', '-n', '-o', 'FSTYPE,SOURCE', '-T', str(out)],
                                      text=True).strip()
    fstype, device = storage.split()[:2]
    assert fstype not in {'tmpfs', 'ramfs'} and device.startswith('/dev/'), storage
    order = []
    for number in range(args.rounds):
        order.extend(['before', 'layered', 'layered', 'before'] if number % 2 == 0
                     else ['layered', 'before', 'before', 'layered'])
    manifest = dict(project='disk-format-layering', output_directory=str(out), regime='resident',
                    io='O_DIRECT', cpu_affinity='0-11', filesystem=storage, order=order,
                    expected_runs=len(order), n=200000, cache_mib=128, log=0, phases=PHASES,
                    seconds=args.seconds, fixture=str(fixture), fixture_sha256=digest(fixture),
                    copies_fsynced_before_open=True, build_provenance=builds,
                    runner_sha256=digest(__file__), runs=[], complete=False)
    save_json(out / 'manifest.json', manifest)
    shutil.copy2(__file__, out / Path(__file__).name)
    (out / 'measured-source.patch').write_bytes(diff)
    rows = []
    for index, label in enumerate(order):
        db = out / f'{index}-{label}.db'
        copy_started = time.time()
        subprocess.run(['cp', '--reflink=auto', '--sparse=always', str(fixture), str(db)], check=True)
        with db.open('r+b') as image:
            os.fsync(image.fileno())
        copied_at = time.time()
        argv = ['taskset', '-c', '0-11', str(paths[label]), '--db', str(db), '--open',
                '--n', '200000', '--cache-mib', '128', '--disk-gib', '8', '--log', '0',
                '--threads', '8', '--mt-bg', '1', '--normal-bg', '3', '--memtable-mib', '4',
                '--secs', str(args.seconds), '--warmup', '1', '--csv', str(out / 'results.csv'),
                '--vtag', label, '--stag', 'resident', '--otag', str(index), '--rep', str(index // 4),
                '--seed', '42', '--hang-secs', '60', '--hang-dir', str(out), *PHASES]
        record = dict(index=index, version=label, command=argv, binary_sha256=digest(paths[label]),
                      copy_started=copy_started, copy_fsync_finished=copied_at, started=time.time(),
                      correctness_passed=False)
        manifest['runs'].append(record)
        save_json(out / 'manifest.json', manifest)
        print(f'format layering {index + 1}/{len(order)} {label}', flush=True)
        with (out / f'{index}-{label}.log').open('w') as target:
            process = subprocess.run(argv, stdout=target, stderr=subprocess.STDOUT,
                                     timeout=max(600, args.seconds * len(PHASES) + 120))
        record.update(returncode=process.returncode, finished=time.time(),
                      db_allocated_bytes=db.stat().st_blocks * 512, db_apparent_bytes=db.stat().st_size)
        save_json(out / 'manifest.json', manifest)
        assert process.returncode == 0, record
        rows = csv_rows(out / 'results.csv')
        current = [row for row in rows if row['order'] == str(index)]
        assert len(current) == len(PHASES), (index, current)
        assert all(int(row['errors']) == 0 and int(row['violations']) == 0 for row in current), current
        assert all(int(row['ops']) > 0 for row in current), current
        record['correctness_passed'] = True
        save_json(out / 'manifest.json', manifest)
    summarize(out, rows, args.rounds)
    manifest['complete'] = True
    save_json(out / 'manifest.json', manifest)
    print('Completed', out, flush=True)


if __name__ == '__main__':
    main()
