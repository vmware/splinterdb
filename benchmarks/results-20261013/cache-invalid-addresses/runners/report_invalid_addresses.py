#!/usr/bin/env python3
"""Produce the benchmark portion of the cache address-validation report."""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COORD = ROOT / 'coordination/cache-invalid-addresses'
DATA = ROOT / 'benchmarks/cache-invalid-addresses'


def summaries(name):
    with (DATA / name / 'summary.csv').open() as stream:
        return list(csv.DictReader(stream))


def query_row(rows, phase):
    return next(row for row in rows if row['phase'] == phase and row['version'] == 'validated')


def percent(row, field='throughput_delta_parent_pct'):
    return f'{float(row[field]):+.2f}%'


def main():
    names = ['resident', 'disk', 'insert', 'resident-repeat', 'insert-repeat']
    metadata = [json.loads((DATA / name / 'manifest.json').read_text()) for name in names]
    assert all(item['complete'] for item in metadata)
    assert all(run['correctness_passed'] and run['returncode'] == 0
               for item in metadata for run in item['runs'])
    resident, disk, insert, repeat, insert_repeat = [summaries(name) for name in names]
    verified = json.loads((COORD / 'verified-source-commit.json').read_text())
    lines = [
        f'Compared cache-errors parent `{verified["parent_commit"]}` against the '
        f'invalid-address candidate `{verified["candidate_commit"]}`. All '
        f'{sum(len(item["runs"]) for item in metadata)} benchmark processes completed '
        'successfully. Query error/violation counters were zero; every insertion '
        'process inserted all 20 million records and reported a successful close.',
        '',
        'All databases were on ext4 `/dev/mapper/snafu-root_crypt`, using `O_DIRECT`. '
        'Query runs used fresh sparse copies of immutable existing fixtures, each '
        'fsynced before opening. The campaign coordinator held `benchmark.lock` and '
        'an exclusive `machine.lock` during timing, preventing concurrent benchmark, '
        'build, or test jobs. CPU affinity was 0–11. Original fixtures were preserved.',
        '',
        'The machine was an Intel Core i7-12800H (20 logical CPUs); observed '
        'frequency governors were `powersave`. Frequency was not pinned. '
        'The compiler was GCC 14.2.0 (Debian 14.2.0-19).',
        '',
        'Both variants were built freshly with the same GCC compiler and release '
        '`-O3 -ffast-math -flto` library flags; memory-allocation fault injection '
        'was disabled in both (`BUILD_MEMORY_FAULT_INJECTION=0`). This does not '
        'claim that cache debug-hook checks were removed. The identical frozen '
        'public query/insertion harnesses additionally '
        'used `-march=native`. SHA-256 provenance records cover exact binaries, '
        'static libraries, harness sources, fixture images, and all 106 production '
        'source/Makefile files. The candidate source inventory was verified against '
        'its committed source after the build.',
        '',
        'Query keys were 24 bytes and values 100 bytes. Resident queries used '
        '200,000 keys and a 128 MiB cache; the timed resident phases all reported '
        'zero disk-read bytes. Disk queries used 2 million keys and a 64 MiB cache. '
        'Logging was disabled for query runs. Initial runs used ABBA order '
        '(parent, candidate, candidate, parent), 20 seconds per resident phase '
        'and 10 seconds per disk phase, with one second of phase warmup. The '
        'focused resident repeat used BAAB order and 30 seconds per phase with '
        'the same binaries and fixtures.',
        '',
        'Throughput changes below compare arithmetic means of the two candidate '
        'samples with the two parent samples in each campaign. Positive means '
        'faster. The raw CSV files include each sample, CPU time, latency, and I/O.',
        '',
        '| Query | Threads | Initial resident | Resident BAAB repeat | Disk |',
        '|---|---:|---:|---:|---:|',
    ]
    for query, descriptor in [('point', 'Point'), ('range:uniform:1', 'Range 1'),
                              ('range:uniform:10000', 'Range 10,000')]:
        for threads in (1, 8):
            phase = f'{"point:uniform" if query == "point" else query}@{threads}'
            repeated = [row for row in repeat if row['phase'] == phase and row['version'] == 'validated']
            repeat_delta = percent(repeated[0]) if repeated else '—'
            lines.append(f'| {descriptor} | {threads} | {percent(query_row(resident, phase))} | '
                         f'{repeat_delta} | {percent(query_row(disk, phase))} |')
    lines += [
        '',
        'Insertions used 8 inserters, 20 million 16-byte keys/16-byte values per '
        'process, a 256 MiB cache, an 8 MiB memtable, one memtable background '
        'thread and three normal background threads. Both WAL modes were measured '
        'in ABBA order and repeated in BAAB order. Drain throughput includes the '
        'final close time. CPU efficiency is operations per process CPU second.',
        '',
        '| WAL | Initial throughput | BAAB throughput | Initial drain | BAAB drain | '
        'Initial CPU efficiency | BAAB CPU efficiency |',
        '|---|---:|---:|---:|---:|---:|---:|',
    ]
    for wal in (0, 1):
        before = next(row for row in insert if row['wal'] == str(wal) and row['version'] == 'validated')
        after = next(row for row in insert_repeat if row['wal'] == str(wal) and row['version'] == 'validated')
        lines.append(f'| {"On" if wal else "Off"} | {percent(before)} | {percent(after)} | '
                     f'{percent(before, "drain_delta_parent_pct")} | '
                     f'{percent(after, "drain_delta_parent_pct")} | '
                     f'{percent(before, "cpu_efficiency_delta_parent_pct")} | '
                     f'{percent(after, "cpu_efficiency_delta_parent_pct")} |')
    lines += [
        '',
        'The initial resident controls drifted substantially: parent point/1 '
        'fell from 1.268M to 1.162M ops/s and point/8 from 5.929M to 5.464M. '
        'These intervals bracketed the candidate samples, so the initial small '
        'negative means alone do not isolate validation overhead. The reverse '
        'repeats were added to check that ambiguity. Each campaign has only two '
        'samples per variant; do not interpret sample ranges as confidence intervals.',
        '',
        'The focused resident repeat ranged from -0.10% to +3.05%, so the initial '
        'query losses did not reproduce under reversed ordering. Disk-query means '
        'ranged from -1.02% to +0.53%. WAL-enabled insertion remained slightly '
        'slower in both orders (-2.94% and -1.77%); across all four samples per '
        'variant, mean throughput was 2.36% lower and throughput including close '
        'was 2.34% lower. Its CPU efficiency was essentially unchanged (+0.01% '
        'in both orders). The repeated insertion sample ranges overlapped. These '
        'data leave a small possible WAL-insertion throughput cost unresolved; '
        'they do not establish its cause or demonstrate a CPU-cost increase.',
        '',
        'This comparison covers ordinary point/range queries and insertions on '
        'valid databases. It does not measure a repeated-invalid-address workload '
        '(including warning output) or establish equivalence for every application.',
        '',
        'The archived runners, command arrays, build configurations, source patch, '
        'binary/library hashes, logs, and result tables are in '
        '`benchmarks/results-20261013/cache-invalid-addresses/`. '
        'Database images and executable binaries remain in the campaign workspace '
        'and are not included in the git artifact directory.',
        '',
    ]
    (COORD / 'benchmark-report.md').write_text('\n'.join(lines))
    print(COORD / 'benchmark-report.md')


if __name__ == '__main__':
    main()
