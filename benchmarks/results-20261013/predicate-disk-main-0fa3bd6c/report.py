"""Validate complete manifests before aggregating; retain raw sample ranges."""
import csv, json, statistics
from pathlib import Path
HERE=Path(__file__).resolve().parent
OUT=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/benchmarks/filter-predicate-20261013')

def save(p,x):p.write_text(json.dumps(x,indent=2)+'\n')
def valid(mode):
    m=json.loads((OUT/mode/'manifest.json').read_text())
    assert m['complete'] and m['runs'] and all(r['verified'] and r['returncode']==0 for r in m['runs'])
    return m

def describe(values):return {'mean':statistics.fmean(values),'min':min(values),'max':max(values),'samples':len(values),'values':values}
summary={};controls=[]
for regime in ['resident','disk','repeat-resident-disk-point8']:
    m=valid(regime)
    expected_order=['disk-format','main','main','disk-format'] if regime.startswith('repeat-') else ['main','filtered-ranges','disk-format','disk-format','filtered-ranges','main']
    assert [r['profile'] for r in m['runs']]==expected_order
    with (OUT/regime/'results.csv').open() as f:rows=list(csv.DictReader(f))
    assert len(rows)==len(m['runs'])*len(m['phases'])
    for run in m['runs']:
        cmd=run['command']; index=cmd[cmd.index('--otag')+1]
        rr=[r for r in rows if r['order']==index]
        assert [r['phase'] for r in rr]==m['phases'] and all(r['version']==run['profile'] for r in rr)
        assert all(int(r['ops'])>0 and int(r['errors'])==int(r['violations'])==0 for r in rr)
        assert all(int(r['ops'])==int(r['n']) for r in rr if r['phase']=='warm')
    results=[]
    for phase in m['phases'][1:]:
        base=[r for r in rows if r['phase']==phase and r['version']=='main']
        baseline=describe([float(r['ops_s']) for r in base])
        controls.append({'regime':regime,'phase':phase,'samples':baseline,'last_vs_first_pct':100*(float(base[-1]['ops_s'])/float(base[0]['ops_s'])-1)})
        for profile in dict.fromkeys(r['profile'] for r in m['runs']):
            rr=[r for r in rows if r['phase']==phase and r['version']==profile]
            throughput=describe([float(r['ops_s']) for r in rr])
            results.append(dict(profile=profile,phase=phase,throughput=throughput,baseline=baseline,
                                throughput_delta_pct=100*(throughput['mean']/baseline['mean']-1),
                                ranges_overlap=max(throughput['min'],baseline['min'])<=min(throughput['max'],baseline['max']),
                                cpu_us_per_op=describe([float(r['cpu_s'])*1e6/int(r['ops']) for r in rr]),
                                read_B_per_op=describe([float(r['rd_B_op']) for r in rr]),
                                write_B_per_op=describe([float(r['wr_B_op']) for r in rr]),
                                p99_us=describe([float(r['p99_us']) for r in rr]),
                                physical_read_bytes=sum(int(r['rd_bytes']) for r in rr),errors=0,violations=0))
    summary[regime]=results
m=valid('feature');results=[]
assert len(m['runs'])==12
for run in m['runs']:
    for row in run['rows']:
        count=int(row['queries']); limit=int(row['limit'])
        if row['mode']=='application':
            expected_visited=count*((limit-1)*32+1)+sum(q%32 for q in range(count))
            assert int(row['visited'])==int(row['predicate_calls'])==expected_visited
        else:
            assert int(row['visited'])==count*limit
for collision in [0,1]:
    for limit in [1,2,10,100]:
        groups={mode:[r for run in m['runs'] for r in run['rows'] if r['mode']==mode and int(r['collisions'])==collision and int(r['limit'])==limit] for mode in ['application','predicate','pruned']}
        assert all(len(rows)==2 for rows in groups.values())
        checks={r['checksum'] for rows in groups.values() for r in rows};assert len(checks)==1
        base=describe([float(r['ns_per_query']) for r in groups['application']])
        for mode,rows in groups.items():
            costs=describe([float(r['ns_per_query']) for r in rows])
            results.append(dict(collisions=collision,limit=limit,mode=mode,latency_ns=costs,baseline=base,
                                latency_delta_pct=100*(costs['mean']/base['mean']-1),
                                ranges_overlap=max(costs['min'],base['min'])<=min(costs['max'],base['max']),
                                cpu_us_per_query=describe([float(r['cpu_s'])*1e6/int(r['queries']) for r in rows]),
                                read_B_per_query=describe([int(r['read_bytes'])/int(r['queries']) for r in rows]),
                                write_B_per_query=describe([int(r['write_bytes'])/int(r['queries']) for r in rows]),
                                visited_per_query=describe([int(r['visited'])/int(r['queries']) for r in rows]),
                                predicate_calls_per_query=describe([int(r['predicate_calls'])/int(r['queries']) for r in rows]),
                                checksum=next(iter(checks)),verified=True))
summary['feature']=results
save(HERE/'summary.json',summary);save(HERE/'control-drift.json',controls)
counts={mode:len(valid(mode)['runs']) for mode in ['fixtures','resident','disk','feature','repeat-resident-disk-point8']}
assert counts=={'fixtures':4,'resident':6,'disk':6,'feature':12,'repeat-resident-disk-point8':4}
save(HERE/'status.json',{'complete':True,'runs':counts,'errors':sum(r.get('errors',0) for mode in counts for r in valid(mode)['runs']),
                         'violations':sum(r.get('violations',0) for mode in counts for r in valid(mode)['runs']),
                         'verification':'All processes succeeded; all manifests verified; exact warm/load counts and output checks validated.'})
lines=['Filtered predicates and disk-format screening against main 0fa3bd6c','',
       'OPEN PERFORMANCE CONCERN: disk-format resident point8 throughput fell 4.39% in the broad screen and 7.75% in the separate longer repeat, both with non-overlapping sample ranges and zero physical reads.',
       'The cause remains unassigned. Disk-format is ready for code review; these results do not support performance-neutrality or merge-ready claims. Broad and repeat samples are retained separately.', '',
       'All databases were on verified real disk using O_DIRECT. Timing was serialized with the campaign benchmark lock.',
       'Ordinary controls: main / filtered / disk-format / disk-format / filtered / main, 8-second resident and 6-second disk phases.',
       'Fresh deterministic fixtures were created serially, closed and fsynced; disk-format uses equivalent data in its own format.',
       'A full warm scan validates every key/value before timed phases. Timed operations also validate their results.',
       'Feature runs: application / predicate-only / pruned / pruned / predicate-only / application for each collision setting.',
       'Each feature run ingests 524288 keys grouped by tenant, then queries before close so selective immutable branches remain.',
       'Each limit has 500 untimed warmup queries and 10000 measured queries; warmup counters are excluded.',
       'Feature checksums are independently recomputed in Python; every matched tuple is checked for row, tenant, 64-byte value length and its 16-byte identity prefix.',
       '', 'These compare each whole feature branch with new main, not the isolated cost of its main merge.',
       'These are screening samples, not confidence intervals or proof of performance neutrality. Two samples per profile.',
       'Sample range overlap is descriptive. Device and CPU drift can obscure small differences; all controls and raw samples are retained.',
       'A separate 15-second resident point8 repeat uses disk-format/main/main/disk-format to investigate the first disk sample; it does not replace the broad-screen results.',
       'Feature CPU/I/O is per measured limit; process resource usage in manifests also includes ingestion, warmup and close.',
       'Predicate callbacks increment a call counter in both variants; the candidate also checks returned tenant identity externally.',
       'Collision mode maps each tenant pair to the same full 32-bit hash under every seed; exact predicates still distinguish tenants.', '']
for regime in ['resident','disk','repeat-resident-disk-point8']:
    lines += [regime.upper(), 'phase | profile | throughput change | main ops/s range | profile ops/s range | overlaps']
    for r in summary[regime]:
        if r['profile']=='main':continue
        a=r['baseline'];b=r['throughput']
        lines.append(f"{r['phase']} | {r['profile']} | {r['throughput_delta_pct']:+.2f}% | {a['min']:.0f}..{a['max']:.0f} | {b['min']:.0f}..{b['max']:.0f} | {r['ranges_overlap']}")
    lines.append('')
lines+=['FEATURE LATENCY (negative change is faster)','collisions | limit | mode | latency change | main ns/query range | mode ns/query range | overlaps']
for r in results:
    if r['mode']=='application':continue
    a=r['baseline'];b=r['latency_ns']
    lines.append(f"{r['collisions']} | {r['limit']} | {r['mode']} | {r['latency_delta_pct']:+.2f}% | {a['min']:.0f}..{a['max']:.0f} | {b['min']:.0f}..{b['max']:.0f} | {r['ranges_overlap']}")
lines+=['','MAIN CONTROL DRIFT']
for r in controls:lines.append(f"{r['regime']} {r['phase']}: last versus first {r['last_vs_first_pct']:+.2f}%")
lines+=['','Resident measured physical read bytes: '+str(sum(r['physical_read_bytes'] for r in summary['resident'])),
        'All runs verified: '+json.dumps(counts),
        'Detailed CPU/op, I/O/op, p99, sample rates and predicate/visited counts are in summary.json.']
(HERE/'report.txt').write_text('\n'.join(lines)+'\n');print('\n'.join(lines))
