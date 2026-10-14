"""Read perf.data after sampled timing completes; run under shared test lock."""
import json,subprocess,hashlib
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE=HERE.parents[2]/'benchmarks/filtered-disk-pr-20261013/sampled-disk-range1'
m=json.loads((BASE/'manifest.json').read_text());assert m['complete'] and all(r['verified'] for r in m['runs'])
records=[]
for r in m['runs']:
 p=BASE/r['label'];data=p/'perf.data'
 assert hashlib.sha256(data.read_bytes()).hexdigest()==r['perf_data_sha256']
 row={'label':r['label'],'reports':[]}
 for name,opts in [('leaf-report',['--no-children','--sort','dso,symbol','--percent-limit','0.1']),('callgraph-report',['--children','--sort','symbol','--percent-limit','0.5'])]:
  dst=p/(name+'.txt');cmd=['perf','report','--stdio','-i',str(data),*opts]
  with dst.open('w') as f:proc=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
  assert proc.returncode==0
  content=dst.read_text();assert 'Error:' not in content and 'Invalid --sort key' not in content and 'Samples:' in content
  row['reports'].append({'command':cmd,'file':str(dst),'sha256':hashlib.sha256(dst.read_bytes()).hexdigest()})
 records.append(row);print(r['label'],flush=True)
(HERE/'profile-reports.json').write_text(json.dumps(records,indent=2)+'\n')
