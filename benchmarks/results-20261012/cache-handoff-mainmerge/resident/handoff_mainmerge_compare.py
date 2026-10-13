#!/usr/bin/env python3
"""Measure the entire origin/main merge into compact cache handoff.

This compares the old handoff head to the merged source, including upstream
blob configuration and btree statistics changes. It is not a cache API A/B.

Invoke with run_job.py benchmark mode to serialize against other machine work.
All images are real-disk copies of immutable fixtures and fsynced before open.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time


ROOT = Path(__file__).resolve().parent.parent
TREE = ROOT / 'worktrees/cache-handoff'
BUILDS = {'before': 'build-selected', 'merged': 'build-mainmerge'}
ORDER = ['before', 'merged', 'merged', 'before']
PHASES = {
    'resident': ['warm', 'point:uniform@1', 'point:uniform@8',
                 'range:uniform:1@1', 'range:uniform:1@8'],
    'disk': ['warm', 'point:uniform@8', 'range:uniform:1@8', 'range:uniform:10000@8'],
}


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=PHASES)
    args = parser.parse_args()
    resident = args.mode == 'resident'
    phases = PHASES[args.mode]
    out = ROOT / 'benchmarks/cache-handoff-mainmerge' / args.mode
    out.mkdir(parents=True, exist_ok=False)
    filesystem = subprocess.check_output(['findmnt', '-n', '-o', 'FSTYPE,SOURCE', '-T', str(out)], text=True).strip()
    fstype, source = filesystem.split()[:2]
    assert fstype not in {'tmpfs', 'ramfs'} and source.startswith('/dev/'), filesystem
    fixture = ROOT / f'benchmarks/lazy-merge/{args.mode}-log0/0-baseline.db'
    provenance = json.loads((ROOT / 'coordination/cache-handoff-mainmerge/builds.json').read_text())
    for relative, expected in provenance['source_files_sha256'].items():
        assert digest(TREE / relative) == expected, relative
    for label, build in BUILDS.items():
        assert digest(TREE / build / 'release/bin/kvbench') == provenance['builds'][label]['binary_sha256']
        assert digest(TREE / build / 'release/lib/libsplinterdb.a') == provenance['builds'][label]['library_sha256']
    manifest = dict(project='cache-handoff-mainmerge', regime=args.mode, order=ORDER,
                    output_directory=str(out), filesystem=filesystem, io='O_DIRECT',
                    fixture=str(fixture), fixture_sha256=digest(fixture), fixture_copy_fsynced=True,
                    n=200000 if resident else 2000000, cache_mib=128 if resident else 64,
                    seconds=20 if resident else 10, cpu_affinity='0-11', phases=phases,
                    scope='whole merge including upstream changes; not isolated cache overhead',
                    build_provenance=provenance, runner_sha256=digest(__file__), runs=[])
    write_json(out / 'manifest.json', manifest)
    (out / 'handoff_mainmerge_compare.py').write_bytes(Path(__file__).read_bytes())
    (out / 'kvbench.c').write_bytes((ROOT / 'baseline/benchmarks/kvbench.c').read_bytes())
    for index, label in enumerate(ORDER):
        db = out / 'probe.db'
        if db.exists():
            assert manifest['runs'][-1]['correctness_passed']
            db.unlink()
        subprocess.run(['cp', '--reflink=auto', '--sparse=always', str(fixture), str(db)], check=True)
        with db.open('r+b') as image:
            os.fsync(image.fileno())
        binary = TREE / BUILDS[label] / 'release/bin/kvbench'
        argv = ['taskset', '-c', '0-11', str(binary), '--db', str(db), '--open',
                '--n', str(manifest['n']), '--cache-mib', str(manifest['cache_mib']),
                '--disk-gib', '8', '--log', '0', '--threads', '8', '--mt-bg', '1',
                '--normal-bg', '3', '--memtable-mib', '4', '--secs', str(manifest['seconds']),
                '--warmup', '1', '--csv', str(out / 'results.csv'), '--vtag', label,
                '--stag', args.mode, '--otag', str(index), '--rep', str(index // 2),
                '--seed', '42', '--hang-secs', '60', '--hang-dir', str(out), *phases]
        record = dict(index=index, version=label, command=argv, started=time.time(),
                      correctness_passed=False, binary_sha256=digest(binary))
        print('handoff-mainmerge', args.mode, index + 1, '/', len(ORDER), label, flush=True)
        with (out / f'{index}-{label}.log').open('w') as stream:
            result = subprocess.run(argv, stdout=stream, stderr=subprocess.STDOUT, timeout=600)
        record.update(returncode=result.returncode, finished=time.time(),
                      db_allocated_bytes=db.stat().st_blocks * 512)
        manifest['runs'].append(record)
        write_json(out / 'manifest.json', manifest)
        assert result.returncode == 0, record
        with (out / 'results.csv').open() as stream:
            rows = list(csv.DictReader(stream))
        current = [row for row in rows if row['order'] == str(index)]
        assert len(current) == len(phases)
        assert all(int(row['ops']) > 0 and int(row['errors']) == 0 and int(row['violations']) == 0
                   for row in current), current
        if resident:
            assert all(float(row['rd_B_op']) == 0 for row in current if row['phase'] != 'warm'), current
        record['correctness_passed'] = True
        write_json(out / 'manifest.json', manifest)
    summaries = []
    for phase in phases[1:]:
        groups = {label: [row for row in rows if row['phase'] == phase and row['version'] == label]
                  for label in BUILDS}
        assert all(len(group) == 2 for group in groups.values())
        mean = lambda label, key: statistics.fmean(float(row[key]) for row in groups[label])
        for label in BUILDS:
            rates = [float(row['ops_s']) for row in groups[label]]
            record = dict(phase=phase, version=label, samples=len(rates),
                          ops_s_mean=statistics.fmean(rates), ops_s_min=min(rates), ops_s_max=max(rates),
                          throughput_delta_before_pct=100 * (mean(label, 'ops_s') / mean('before', 'ops_s') - 1),
                          cpu_us_op=statistics.fmean(1e6 * float(row['cpu_s']) / int(row['ops']) for row in groups[label]),
                          cpu_efficiency_delta_before_pct=100 * (mean(label, 'ops_per_cpu_s') / mean('before', 'ops_per_cpu_s') - 1),
                          read_B_op=mean(label, 'rd_B_op'), write_B_op=mean(label, 'wr_B_op'),
                          p99_us=mean(label, 'p99_us'), errors=0, violations=0)
            summaries.append(record)
            print(phase, label, f'{record["ops_s_mean"]:.1f} ops/s '
                  f'[{min(rates):.1f},{max(rates):.1f}], '
                  f'{record["throughput_delta_before_pct"]:+.2f}%, CPUeff '
                  f'{record["cpu_efficiency_delta_before_pct"]:+.2f}%', flush=True)
    with (out / 'summary.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summaries[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(summaries)
    manifest['complete'] = True
    write_json(out / 'manifest.json', manifest)
    print('Completed', out, flush=True)


if __name__ == '__main__':
    main()
