import csv,json,statistics
from pathlib import Path
OUT=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/benchmarks/main-7bb5fab')
COORD=Path(__file__).resolve().parent
allrows=[]
def read(path):
 with path.open() as f:return list(csv.DictReader(f))
def verified_manifest(folder):
 p=folder/'manifest.json'
 if not p.exists():return None
 m=json.loads(p.read_text())
 if not m.get('complete'):return None
 assert m['runs'] and all(r.get('verified') and r.get('returncode')==0 for r in m['runs']),folder
 return m
def summary(mode,group,version,rs,base,rate='ops_s'):
 rates=[float(r[rate]) for r in rs];baseline=[float(r[rate]) for r in base]
 avg=statistics.fmean
 return dict(workload=mode,phase=group,version=version,samples=len(rs),ops_s_mean=avg(rates),ops_s_min=min(rates),ops_s_max=max(rates),baseline_ops_s_mean=avg(baseline),baseline_ops_s_min=min(baseline),baseline_ops_s_max=max(baseline),delta_pct=100*(avg(rates)/avg(baseline)-1),ranges_overlap=max(min(rates),min(baseline))<=min(max(rates),max(baseline)))
for mode in ['resident','disk','prefix-resident-common','prefix-resident-random','prefix-disk-common','prefix-disk-random',*[p.name for p in OUT.glob('repeat-*')]]:
 folder=OUT/mode
 if not (folder/'results.csv').exists() or not (folder/'manifest.json').exists():continue
 m=verified_manifest(folder)
 if m is None:continue
 rr=read(folder/'results.csv')
 for run in m['runs']:
  command=run['command']; order=command[command.index('--otag')+1]
  own=[r for r in rr if r['order']==order]
  phases=command[command.index('warm'):]
  assert len(own)==len(phases) and [r['phase'] for r in own]==phases and all(r['version']==run['profile'] for r in own),run['label']
 assert len(rr)==sum(len(run['command'][run['command'].index('warm'):]) for run in m['runs'])
 for phase in dict.fromkeys(r['phase'] for r in rr if r['phase']!='warm'):
  bas=[r for r in rr if r['version']=='main' and r['phase']==phase]
  if mode=='resident' or mode.startswith('repeat-'):assert all(int(r['rd_bytes'])==0 for r in rr if r['phase']!='warm'), 'resident queries performed physical reads'
  for version in dict.fromkeys(r['version'] for r in rr):
   rs=[r for r in rr if r['version']==version and r['phase']==phase]
   if not rs or not bas:continue
   s=summary(mode,phase,version,rs,bas)
   s.update(errors=sum(int(r['errors']) for r in rs),violations=sum(int(r['violations']) for r in rs),cpu_us_op=statistics.fmean(1e6*float(r['cpu_s'])/int(r['ops']) for r in rs),read_B_op=statistics.fmean(float(r['rd_B_op']) for r in rs),write_B_op=statistics.fmean(float(r['wr_B_op']) for r in rs),p99_us=statistics.fmean(float(r['p99_us']) for r in rs));allrows.append(s)
folder=OUT/'insert'
if (folder/'results.csv').exists() and verified_manifest(folder):
 m=verified_manifest(folder);rr=read(folder/'results.csv')
 assert len(rr)==len(m['runs'])
 assert {f"wal{r['wal']}-{int(r['index']):02d}-{r['version']}" for r in rr}=={r['label'] for r in m['runs']}
 for wal in dict.fromkeys(r['wal'] for r in rr):
  bas=[r for r in rr if r['version']=='main' and r['wal']==wal]
  for version in dict.fromkeys(r['version'] for r in rr if r['wal']==wal):
   rs=[r for r in rr if r['version']==version and r['wal']==wal];s=summary('insert','WAL'+wal,version,rs,bas)
   s.update(errors=sum(int(r['errors']) for r in rs),violations=sum(int(r['violations']) for r in rs));s.update({k:statistics.fmean(float(r[k]) for r in rs) for k in ['cpu_us_op','read_B_op','write_B_op','p99_us']});allrows.append(s)
if allrows:
 with (COORD/'summary.csv').open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(allrows[0]),lineterminator='\n');w.writeheader();w.writerows(allrows)
lines=['# Fresh main integration benchmark screening','','These compare current feature branches with main 7bb5fab. They do not isolate the cost of merging main. Short runs are screening evidence, not proof of performance neutrality. All databases are on verified real disk with O_DIRECT; copied fixtures are fsynced before opening. Query samples use serial-built immutable fixtures, with separate equivalent fixtures for incompatible formats. Main controls bracket forward and reverse variant sweeps.','', '| Workload | Phase | Branch | Change | Sample range overlap |','| --- | --- | --- | ---: | --- |']
for r in allrows:
 if r['version']=='main':continue
 lines.append(f"| {r['workload']} | {r['phase']} | {r['version']} | {r['delta_pct']:+.2f}% | {r['ranges_overlap']} |")
counts={}
for p in OUT.glob('*/manifest.json'):
 m=json.loads(p.read_text());counts[p.parent.name]=dict(runs=len(m['runs']),complete=m.get('complete',False),verified=sum(r.get('verified',False) for r in m['runs']))
(COORD/'counts.json').write_text(json.dumps(counts,indent=2)+'\n')
lines+=['','Detailed throughput ranges, CPU/op, I/O/op and p99 are in summary.csv. Raw commands, source/library/executable hashes, verification and logs remain in the per-workload manifest and adjacent artifacts.','',json.dumps(counts,indent=2)]
(COORD/'report.md').write_text('\n'.join(lines)+'\n')
print(json.dumps(counts,indent=2))
feature_summaries={}
p=OUT/'hash/manifest.json'
if p.exists() and verified_manifest(p.parent):
 m=verified_manifest(p.parent);hh=[]
 for limit in ['1','2','10','100']:
  groups={profile:[r for run in m['runs'] if run['profile']==profile and run['verified'] for r in run['rows'] if r['limit']==limit] for profile in ['main','filtered-ranges']}
  if not all(groups.values()):continue
  mean=lambda name:statistics.fmean(float(r['ns_per_query']) for r in groups[name])
  for profile,rr in groups.items():
   costs=[float(r['ns_per_query']) for r in rr]
   hh.append(dict(profile=profile,limit=int(limit),samples=len(rr),ns_per_query_mean=statistics.fmean(costs),ns_per_query_min=min(costs),ns_per_query_max=max(costs),latency_delta_main_pct=100*(mean(profile)/mean('main')-1),visited_mean=statistics.fmean(int(r['visited']) for r in rr),verified=True,verification_kind='successful process with harness assertions and matching result checksums'))
 feature_summaries['hash']=hh
p=OUT/'barrier/results.csv'
if p.exists() and verified_manifest(p.parent):
 m=verified_manifest(p.parent);rr=read(p);bb=[]
 assert len(rr)==len(m['runs']) and {r['label'] for r in rr}=={r['label'] for r in m['runs']}
 for wal in ['0','1']:
  for workers in ['0','1']:
   cases=[('main','sync'),('async-durable-barrier','sync'),('async-durable-barrier','async')]
   base=[r for r in rr if r['log']==wal and r['workers']==workers and '-main-' in r['label']]
   if not base:continue
   for profile,mode in cases:
    rs=[r for r in rr if r['log']==wal and r['workers']==workers and f'-{profile}-' in r['label'] and r['mode']==mode]
    if not rs:continue
    s=summary('barrier',f'wal{wal}-workers{workers}-{mode}',profile,rs,base,rate='updates_s')
    s.update(launch_p99_us=statistics.fmean(float(r['launch_p99_us']) for r in rs),completion_p99_us=statistics.fmean(float(r['observed_completion_p99_us']) for r in rs),verified=all(int(r['verified'])==1 for r in rs),verification_kind='crash after completed final barrier, reopen and full key/value readback',cpu_us_update=statistics.fmean(1e6*float(r['cpu_s'])/(int(r['batch'])*int(r['barriers'])) for r in rs),read_bytes_update=statistics.fmean(int(r['read_bytes'])/(int(r['batch'])*int(r['barriers'])) for r in rs),write_bytes_update=statistics.fmean(int(r['write_bytes'])/(int(r['batch'])*int(r['barriers'])) for r in rs));bb.append(s)
 feature_summaries['barrier']=bb
(COORD/'feature-summaries.json').write_text(json.dumps(feature_summaries,indent=2)+'\n')
controls=[]
for folder in OUT.iterdir():
 p=folder/'results.csv'
 if not folder.is_dir() or not p.exists():continue
 rr=read(p)
 if not rr or not {'version','phase','ops_s'}.issubset(rr[0]):continue
 for phase in dict.fromkeys(r['phase'] for r in rr if r['phase']!='warm'):
  rs=[r for r in rr if r['version']=='main' and r['phase']==phase]
  if not rs:continue
  first=float(rs[0]['ops_s'])
  for r in rs:controls.append(dict(workload=folder.name,phase=phase,order=r['order'],ops_s=float(r['ops_s']),change_from_first_pct=100*(float(r['ops_s'])/first-1),cpu_us_op=1e6*float(r['cpu_s'])/int(r['ops']),read_bytes=int(r['rd_bytes'])))
if controls:
 with (COORD/'control-drift.csv').open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(controls[0]),lineterminator='\n');w.writeheader();w.writerows(controls)
with (COORD/'report.md').open('a') as f:
 f.write('\n## Main control drift\n\n')
 f.write('All controls are retained. Large variation in these unchanged-main samples prevents attributing small feature differences to code. The measured cause of this variation is unknown. Nearby ABBA repeats are separate evidence; they do not replace these results.\n\n')
 f.write('| Regime | Phase | First ops/s | Middle ops/s | Last ops/s | Middle vs first | Last vs first |\n| --- | --- | ---: | ---: | ---: | ---: | ---: |\n')
 for mode in ['resident','disk']:
  for phase in dict.fromkeys(r['phase'] for r in controls if r['workload']==mode):
   rr=[r for r in controls if r['workload']==mode and r['phase']==phase]
   if len(rr)!=3:continue
   f.write(f"| {mode} | {phase} | {rr[0]['ops_s']:,.1f} | {rr[1]['ops_s']:,.1f} | {rr[2]['ops_s']:,.1f} | {rr[1]['change_from_first_pct']:+.2f}% | {rr[2]['change_from_first_pct']:+.2f}% |\n")

with (COORD/'report.md').open('a') as f:
 f.write('\n## Interpretation and limits\n\n')
 f.write('These samples support feature tradeoffs, not an isolated estimate of merge overhead. There are two samples per candidate and two or three main controls; non-overlap is a descriptive range check, not a statistical confidence interval. Unchanged main controls drifted substantially, including in nearby repeats. The cause is unproven.\n\n')
 f.write('The invalid-address checks reproduce a roughly 4.5% resident single-thread point-query loss: broad screening -4.60%, nearby warmed ABBA -4.54% with non-overlapping sample ranges. Its repeated eight-thread point query (-3.64%) overlaps main, and repeated one-result/eight-thread range query (+0.03%) does not reproduce the initial apparent loss. Atomic directory nearby repeats (-6.18% point and -3.89% short range) and async-barrier ordinary-query repeats (-0.52% point and -2.28% short range) all overlap main; their smaller effects remain inconclusive. The scalar/SIMD directory variants show larger resident feature costs in the broad screen. These are comparisons of the whole features to main, not newly introduced merge regressions.\n\n')
 f.write('Prefix compression improves several common-prefix workloads, but the random-key resident long-range screen loses 10.88% with non-overlapping samples. The default 64-page automatic log-group durability threshold loses 42.7% insert throughput in the short WAL-enabled screen; this is the cost of added automatic durability versus main without automatic barriers. Disabling the threshold gives -1.3% with overlapping ranges. These tradeoffs require workload-specific judgment, not a blanket performance-neutral claim.\n\n')
 f.write('All timed processes passed their declared verification. Query runs validate keys/values/ranges and close successfully. Insert-only runs validate API returns, positive insert counts and close, but do not perform readback. Barrier runs crash after the final completed barrier, reopen and verify every key/value. Hash-filter runs compare checksums and expected output counts. Every measured ordinary resident and focused-repeat query phase has zero physical read bytes. The retained pre-repeat warmup runs point and one-result range queries at eight threads for 10 seconds each; it is not pooled with comparison controls.\n\n')
 f.write('Separate correctness testing reproduced an inherited small-cache async progress stall on main and invalid-addresses under restricted CPU scheduling. That limitation is documented in the branch correctness evidence; it was not repaired in these merges, and no benchmark run stalled.\n\n')
 f.write('Barrier measurements are short and show substantial run-to-run I/O variation in some configurations. In particular, WAL-enabled/no-background-worker candidate sync spans about 60k to 194k updates/s, while main spans 63k to 65k; its +99% mean alone is not a stable speedup estimate. Async launch latency is distinct from durability completion. These runs use depth 1: completion is observed by waiting or polling before the next insert batch. Greater-depth harness modes may include intervening inserts in observed completion, but were not measured here.\n\n')
 f.write('| Barrier config | Candidate mode | Main updates/s range | Candidate updates/s range | Mean change | Mean launch p99 us | Mean observed completion p99 us |\n| --- | --- | ---: | ---: | ---: | ---: | ---: |\n')
 for r in feature_summaries.get('barrier',[]):
  if r['version']=='main':continue
  f.write(f"| {r['phase'].rsplit('-',1)[0]} | {r['phase'].rsplit('-',1)[1]} | {r['baseline_ops_s_min']:,.0f}–{r['baseline_ops_s_max']:,.0f} | {r['ops_s_min']:,.0f}–{r['ops_s_max']:,.0f} | {r['delta_pct']:+.2f}% | {r['launch_p99_us']:.2f} | {r['completion_p99_us']:.2f} |\n")
 f.write('\nHash-filter API versus application filtering: mean latency changes for limits 1/2/10/100 are -5.70%/-16.71%/+0.56%/-9.59%; output checksums match. Full ranges, visited tuple counts, CPU/I/O and provenance are retained in the raw results and feature-summaries.json.\n')
