import collections,json,statistics
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE=HERE.parents[2]/'benchmarks/filtered-disk-pr-20261013'
def stats(rr,k):
 v=[r[k] for r in rr];return dict(mean=statistics.mean(v),min=min(v),max=max(v),samples=len(v),values=v)
report={};lines=['Filtered predicates integrated with disk-format: controlled end-to-end comparison','', 'Ordinary tests: same harness; fixed CPU/thread-id assignments; full validating scan at one thread; one-second query warmup; phase-gated counters. Main/disk/combined forward-reverse order. Source/libs/binaries/fixtures are pinned by SHA256. Throughput on this laptop is sensitive to thermal/frequency drift; compare cycles/instructions and I/O with wall rates, not wall rates alone. User cycles/ref-cycles is the relative frequency indicator; user cycles/task-clock also includes kernel time in its denominator and must not be interpreted as physical frequency for I/O workloads. Feature counters aggregate the four measured limits, excluding load/warmup/close.','']
for mode in ['smoke','resident','disk','repeat-disk-range1','sampled-disk-range1','repeat-disk-range10000']:
 path=BASE/mode/'manifest.json'
 if not path.exists():continue
 m=json.loads(path.read_text())
 if not m['complete']:continue
 rows=[];groups=collections.defaultdict(list)
 for r in m['runs']:
  assert r['verified'];c=r['counters'];q=r['rows'][1];ops=int(q['ops'])
  row=dict(label=r['label'],profile=r['profile'],phase=q['phase'],queries=ops,tail_observations_p99=ops*0.01,tail_observations_p999=ops*0.001,ops_s=float(q['ops_s']),cpu_us_op=float(q['cpu_s'])*1e6/ops,read_B_op=float(q['rd_bytes'])/ops,instructions_op=c['cpu_core/instructions/u']/ops,cycles_op=c['cpu_core/cycles/u']/ops,branches_op=c['cpu_core/branches/u']/ops,branch_misses_op=c['cpu_core/branch-misses/u']/ops,estimated_effective_ghz=c['cpu_core/cycles/u']/c['task-clock']/1e6,cycles_ref_ratio=c['cpu_core/cycles/u']/c['cpu_core/ref-cycles/u'],cpu_utilization=float(q['cpu_s'])/float(q['secs'])/int(q['threads']),mean_us=float(q['mean_us']),p99_us=float(q['p99_us']),p999_us=float(q['p999_us']),migrations=c['cpu-migrations'],context_switches=c['context-switches'])
  if 'cpu_core/cycles/k' in c:row.update(kernel_cycles_op=c['cpu_core/cycles/k']/ops,kernel_instructions_op=c['cpu_core/instructions/k']/ops)
  if q['phase'].startswith('range:'):
   tuples=int(q['tuples']);assert tuples>0
   row.update(tuples=tuples,tuples_s=float(q['tuples_s']),tuples_query=tuples/ops,instructions_tuple=c['cpu_core/instructions/u']/tuples,cycles_tuple=c['cpu_core/cycles/u']/tuples,cpu_ns_tuple=float(q['cpu_s'])*1e9/tuples,read_B_tuple=float(q['rd_bytes'])/tuples)
  rows.append(row);groups[(q['phase'],r['profile'])].append(row)
 summaries=[]
 for (phase,profile),rr in groups.items():
  v=dict(phase=phase,profile=profile,**{k:stats(rr,k) for k in ['ops_s','cpu_us_op','read_B_op','instructions_op','cycles_op','branches_op','branch_misses_op','estimated_effective_ghz','cycles_ref_ratio','cpu_utilization','mean_us','p99_us','p999_us']})
  if 'kernel_cycles_op' in rr[0]:v.update({k:stats(rr,k) for k in ['kernel_cycles_op','kernel_instructions_op']})
  if phase.startswith('range:'):
   v.update({k:stats(rr,k) for k in ['tuples_s','tuples_query','instructions_tuple','cycles_tuple','cpu_ns_tuple','read_B_tuple']})
  v['percent_delta']={}
  for reference in ['main','disk-format']:
   if (phase,reference) in groups:
    v['percent_delta'][reference]={k:100*(v[k]['mean']/statistics.mean(r[k] for r in groups[(phase,reference)])-1) for k in ['ops_s','cpu_us_op','instructions_op','cycles_op']+(['tuples_s','instructions_tuple','cycles_tuple','cpu_ns_tuple'] if phase.startswith('range:') else [])}
  summaries.append(v)
 report[mode]=dict(rows=rows,summaries=summaries)
 lines.extend([mode,'phase | profile | mean op/s [min,max] | instructions/op | cycles/op | user cycles/ref-cycles | CPU utilization | physical read B/op | p99 us'])
 for v in summaries:
  x=v['ops_s'];lines.append(f"{v['phase']} | {v['profile']} | {x['mean']:.1f} [{x['min']:.1f},{x['max']:.1f}] | {v['instructions_op']['mean']:.2f} | {v['cycles_op']['mean']:.2f} | {v['cycles_ref_ratio']['mean']:.3f} | {v['cpu_utilization']['mean']:.3f} | {v['read_B_op']['mean']:.2f} | {v['p99_us']['mean']:.3f}")
  if v['profile']=='combined':
   for ref,d in v['percent_delta'].items():
    lines.append(f" combined vs {ref}: throughput {d['ops_s']:+.2f}%, instructions/op {d['instructions_op']:+.3f}%, cycles/op {d['cycles_op']:+.2f}%")
    if 'tuples_s' in d:lines.append(f"  per returned tuple: throughput {d['tuples_s']:+.2f}%, instructions {d['instructions_tuple']:+.3f}%, cycles {d['cycles_tuple']:+.2f}%")
  if 'kernel_cycles_op' in v:lines.append(f" kernel cycles/query {v['kernel_cycles_op']['mean']:.2f}, kernel instructions/query {v['kernel_instructions_op']['mean']:.2f}; user cycles/query above")
  if 'tuples_s' in v:lines.append(f" returned tuples/query {v['tuples_query']['mean']:.3f}, tuples/s {v['tuples_s']['mean']:.1f}, instructions/tuple {v['instructions_tuple']['mean']:.3f}, cycles/tuple {v['cycles_tuple']['mean']:.3f}")
 lines.append('')
p=BASE/'feature-v2/manifest.json'
if p.exists():
 m=json.loads(p.read_text())
 if m['complete']:
  rows=[];groups=collections.defaultdict(list);total=[]
  for r in m['runs']:
   assert r['verified'];c=r['counters']
   total.append(dict(label=r['label'],instructions=c['cpu_core/instructions/u'],cycles=c['cpu_core/cycles/u'],estimated_effective_ghz=c['cpu_core/cycles/u']/c['task-clock']/1e6))
   for q in r['rows']:
    row=dict(label=r['label'],mode=q['mode'],collisions=int(q['collisions']),limit=int(q['limit']),ns_query=float(q['ns_per_query']),cpu_ns_query=float(q['cpu_s'])*1e9/int(q['queries']),read_B_query=int(q['read_bytes'])/int(q['queries']),predicate_calls_query=int(q['predicate_calls'])/int(q['queries']),visited_query=int(q['visited'])/int(q['queries']),checksum=int(q['checksum']))
    rows.append(row);groups[(row['collisions'],row['limit'],row['mode'])].append(row)
  summaries=[]
  for (collisions,limit,mode),rr in groups.items():
   v=dict(collisions=collisions,limit=limit,mode=mode,**{k:stats(rr,k) for k in ['ns_query','cpu_ns_query','read_B_query','predicate_calls_query','visited_query']})
   reference=groups[(collisions,limit,'application')];v['speedup_application']=statistics.mean(r['ns_query'] for r in reference)/v['ns_query']['mean'];summaries.append(v)
  report['feature']=dict(rows=rows,summaries=summaries,counter_totals=total)
  lines.extend(['feature','collisions | limit | mode | mean ns/query | speedup vs application | predicate calls/query'])
  for v in summaries:lines.append(f"{v['collisions']} | {v['limit']} | {v['mode']} | {v['ns_query']['mean']:.1f} | {v['speedup_application']:.2f}x | {v['predicate_calls_query']['mean']:.1f}")
(HERE/'summary.json').write_text(json.dumps(report,indent=2)+'\n');(HERE/'report.txt').write_text('\n'.join(lines)+'\n');print('\n'.join(lines))
