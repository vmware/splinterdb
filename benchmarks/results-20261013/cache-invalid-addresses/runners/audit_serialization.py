#!/usr/bin/env python3
from pathlib import Path
import json,time
r=Path(__file__).resolve().parent
jobs=[json.loads(line) for line in (r/'jobs.jsonl').read_text().splitlines() if line.strip()]
bench=[j for j in jobs if j['mode']=='benchmark'];overlap=[]
for i,b in enumerate(jobs):
 if b['mode']!='benchmark':continue
 for k,j in enumerate(jobs):
  if i==k or (j['mode']=='benchmark' and k<i):continue
  seconds=min(b['finished'],j['finished'])-max(b['started'],j['started'])
  if seconds>0:overlap.append({'benchmark':b['name'],'other':j['name'],'seconds':seconds})
result={'audited_at':time.time(),'completed_jobs':len(jobs),'completed_benchmark_jobs':len(bench),'overlaps':overlap,'failed_jobs':[{'name':j['name'],'mode':j['mode'],'returncode':j.get('returncode')} for j in jobs if j.get('returncode')!=0]}
(r/'serialization-audit.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k!='failed_jobs'},indent=2))
if overlap:raise SystemExit('Benchmark overlap detected')
