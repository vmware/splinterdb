"""Summarize the integration screen without treating few short samples as proof."""
import csv
import json
import statistics as st
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent.parent / 'benchmarks/main-fc0bc8f7'

def rows(path):
    return list(csv.DictReader(path.open()))

def stats(rr, field):
    values = [float(r[field]) for r in rr]
    return dict(n=len(values), mean=st.mean(values), minimum=min(values), maximum=max(values))

summary = {'log': [], 'disabled_repeat': {}, 'branches': []}
lines = ['Main fc0bc8f7 integration and asynchronous log policy',
         'All database files use verified real ext4 disk and O_DIRECT; jobs are serialized.',
         'These are short closed-loop samples; sample ranges are not confidence intervals.', '']
p = OUT/'log-async/results.csv'
if p.exists():
    data = rows(p)
    lines += ['Log insertion: 8 seconds/run, 3 rounds, 1/8 writers, one memtable and3 normal workers.',
              '256MiB cache,16GiB logical disk,8MiB memtable,16-byte keys/values,CPU0-11.',
              'Threshold0 means default64 pages; UINT64_MAX disables. No explicit periodic barriers.',
              'Main has no automatic durability, so it supplies a throughput ceiling, not equal durability.',
              'Sync and async policies are based on the same main. Shutdown drains accepted tasks.',
              'API return/count/close checked; insertion readback is covered separately by recovery tests.', '']
    for threads in [1, 8]:
        group = [r for r in data if int(r['threads']) == threads]
        main = [r for r in group if r['version'] == 'main']
        if not main:
            continue
        base = st.mean(float(r['ops_s']) for r in main)
        for name in ['main', 'disabled', 'sync64', 'async64', 'sync1024', 'async1024']:
            rr = [r for r in group if r['version'] == name]
            if not rr:
                continue
            rec = dict(threads=threads, version=name)
            for field in ['ops_s', 'drain_ops_s', 'close_s', 'p99_us', 'p999_us', 'cpu_us_op', 'process_cpu_us_op', 'write_B_op', 'process_write_B_op']:
                rec[field] = stats(rr, field)
            rec['throughput_percent_vs_main'] = 100*(rec['ops_s']['mean']/base-1)
            paired = []
            for r in rr:
                controls = [float(c['ops_s']) for c in main if c['rep'] == r['rep']]
                paired.append(100*(float(r['ops_s'])/st.mean(controls)-1))
            rec['round_normalized_percent_vs_main'] = paired
            summary['log'].append(rec)
            q = rec['ops_s']
            lines.append(f"writers={threads} {name:10s} n={q['n']} mean={q['mean']:.0f}/s range=[{q['minimum']:.0f},{q['maximum']:.0f}] vsMain={rec['throughput_percent_vs_main']:+.2f}% drain={rec['drain_ops_s']['mean']:.0f}/s close={rec['close_s']['mean']:.3f}s p99={rec['p99_us']['mean']:.2f}us p999={rec['p999_us']['mean']:.2f}us fullCPU={rec['process_cpu_us_op']['mean']:.3f}us/op")
        for pages in [64, 1024]:
            sync = [r for r in group if r['version'] == 'sync'+str(pages)]
            async_ = [r for r in group if r['version'] == 'async'+str(pages)]
            if sync and async_:
                delta = 100*(st.mean(float(r['ops_s']) for r in async_)/st.mean(float(r['ops_s']) for r in sync)-1)
                lines.append(f'  async versus sync at {pages} pages: {delta:+.2f}% throughput')
    lines += ['', 'CPU/I/O sampled-window totals end before joins/close; process_* counters include complete process lifetime.', '']

p = OUT/'log-disabled-repeat/results.csv'
if p.exists():
    rr = rows(p)
    lines += ['Disabled-policy follow-up: two ABBA rounds,12seconds/run,8writers.',
              'Main controls drift sharply in the first round; preserve both rounds.']
    for profile in ['main', 'disabled']:
        selected = [r for r in rr if r['version'] == profile]
        summary['disabled_repeat'][profile] = stats(selected, 'ops_s')
    for rep in sorted({r['rep'] for r in rr}):
        means = {v: st.mean(float(r['ops_s']) for r in rr if r['version'] == v and r['rep'] == rep) for v in ['main', 'disabled']}
        delta = 100*(means['disabled']/means['main']-1)
        summary['disabled_repeat']['round'+rep] = dict(means=means, percent_vs_main=delta)
        lines.append(f'  round{rep}: disabled versus main {delta:+.2f}%')
    lines += ['This does not establish an exact small overhead; the second, steadier round is effectively tied.', '']

for pattern in ['ordinary', 'common', 'random']:
    path = OUT/'branch-screen'/(pattern+'-results.csv')
    if not path.exists():
        continue
    rr = rows(path)
    lines += [f'Branch screen, {pattern}: resident200k records,3-second measured phases,8threads;2 candidate samples.',
              'Prefix format uses separate images with identical logical tuples. Warm/point/range values validated.']
    for phase in sorted({r['phase'] for r in rr if r['phase'] != 'warm'}):
        group = [r for r in rr if r['phase'] == phase]
        main = [r for r in group if r['version'] == 'main']
        if not main:
            continue
        baseline = st.mean(float(r['ops_s']) for r in main)
        for profile in sorted({r['version'] for r in group}):
            selected = [r for r in group if r['version'] == profile]
            rec = dict(pattern=pattern, phase=phase, profile=profile, ops_s=stats(selected, 'ops_s'), ops_per_cpu_s=stats(selected, 'ops_per_cpu_s'))
            rec['throughput_percent_vs_main'] = 100*(rec['ops_s']['mean']/baseline-1)
            summary['branches'].append(rec)
            q = rec['ops_s']
            lines.append(f"  {profile:24s} {phase:20s} mean={q['mean']:.0f}/s range=[{q['minimum']:.0f},{q['maximum']:.0f}] vsMain={rec['throughput_percent_vs_main']:+.2f}%")
    lines += ['']
(HERE/'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
(HERE/'report.txt').write_text('\n'.join(lines)+'\n')
print('\n'.join(lines))
