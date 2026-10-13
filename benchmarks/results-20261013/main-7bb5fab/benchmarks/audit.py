"""Validate final matrix coverage, provenance, row counts and exclusive scheduling."""
import csv,hashlib,json
from pathlib import Path
C=Path(__file__).resolve().parent
P=C.parents[2]
O=P/'benchmarks/main-7bb5fab'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def readcsv(p):return list(csv.DictReader(p.open()))
expected={'fixtures':12,'resident':23,'disk':23,'prefix-resident-common':4,'prefix-resident-random':4,'prefix-disk-common':4,'prefix-disk-random':4,'insert':31,'hash':4,'barrier':24,'repeat-warmup':1,'repeat-cache-invalid-addresses':4,'repeat-directory-bucket-atomic':4,'repeat-async-durable-barrier':4}
known_runner_hashes={sha(C/n):n for n in ['run_matrix.py','run_matrix-fixtures.py','repeat.py']}
builds={p.name.removesuffix('-build.json'):json.loads(p.read_text()) for p in C.glob('*-build.json')}
for profile,b in builds.items():
 assert sha(Path(b['library']))==b['library_sha256'],profile
 for f,h in b['source_files_sha256'].items():assert sha(Path(b['tree'])/f)==h,(profile,f)
 for exe,v in b['binaries'].items():
  assert sha(Path(v['binary']))==v['binary_sha256']
  assert sha(Path(v['source']))==v['source_sha256']
runs=[];counts={}
for mode,count in expected.items():
 folder=O/mode;m=json.loads((folder/'manifest.json').read_text())
 assert m['complete'] and len(m['runs'])==count,mode
 assert m['runner_sha256'] in known_runner_hashes,mode
 if 'shared_runner_sha256' in m:assert m['shared_runner_sha256']==sha(C/'run_matrix.py')
 for r in m['runs']:
  assert r['verified'] and r['returncode']==0,(mode,r['label'])
  log=folder/(r['label']+'.log');assert sha(log)==r['log_sha256']
  assert 'FINDING: splinterdb_close' not in log.read_text(errors='replace')
  b=builds[r['profile']];assert r['source_head']==b['head'] and r['library_sha256']==b['library_sha256']
  assert r['binary_sha256'] in {v['binary_sha256'] for v in b['binaries'].values()}
  assert r['finished']>=r['started'] and r['process_cpu_s']>0
  runs.append(dict(mode=mode,label=r['label'],started=r['started'],finished=r['finished']))
 counts[mode]=dict(runs=count,runner=known_runner_hashes[m['runner_sha256']],verified=True)
 if (folder/'results.csv').exists():
  rows=readcsv(folder/'results.csv')
  if 'phase' in rows[0]:
   for r in rows:
    assert int(r['errors'])==0 and int(r['violations'])==0 and int(r['ops'])>0
    if r['phase']=='warm':assert int(r['ops'])==int(r['n'])
    elif mode=='resident' or mode.startswith('repeat-'):assert int(r['rd_bytes'])==0
  if mode=='barrier':assert len(rows)==count and all(int(r['verified'])==1 for r in rows)
  if mode=='insert':assert len(rows)==count and all(int(r['errors'])==0 and int(r['violations'])==0 and int(r['inserted'])>0 for r in rows)
 if mode=='fixtures':
  for r in m['runs']:
   rows=readcsv(folder/(r['label']+'.csv'));assert len(rows)==1 and int(rows[0]['ops'])==int(rows[0]['n']) and int(rows[0]['errors'])==int(rows[0]['violations'])==0
 if mode=='hash':
  for r in m['runs']:assert [int(x['limit']) for x in r['rows']]==[1,2,10,100] and all(int(x['queries'])==1000 for x in r['rows'])
  for i in range(4):assert len({r['rows'][i]['checksum'] for r in m['runs']})==1
runs.sort(key=lambda r:r['started'])
assert all(a['finished']<=b['started'] for a,b in zip(runs,runs[1:])), 'Benchmark subprocesses overlap'
jobs=[json.loads(line) for line in (P/'coordination/jobs.jsonl').read_text().splitlines()]
ours=[j for j in jobs if j['mode']=='benchmark' and j['name'].startswith('main-7bb5fab-benchmark-')]
assert len(ours)==11 and all(j['mode']=='benchmark' and j['returncode']==0 for j in ours)
for j in ours:
 for k in jobs:
  if j is k:continue
  assert min(j['finished'],k['finished'])<=max(j['started'],k['started']),(j['name'],k['name'])
for r in runs:assert sum(j['started']<=r['started'] and j['finished']>=r['finished'] for j in ours)==1
(C/'run_job.py').write_bytes((P/'coordination/run_job.py').read_bytes())
(C/'benchmark-jobs.json').write_text(json.dumps(ours,indent=2)+'\n')
result=dict(complete=True,scope='new main versus updated feature; merge cost not isolated',counts=counts,total_processes=len(runs),comparison_processes=len(runs)-13,fixture_processes=12,recorded_warmup_processes=1,exclusive_benchmark_jobs=len(ours),source_and_binary_hashes_verified=True,runner_hashes_verified=True,all_processes_serial=True,no_overlapping_build_test_jobs=True,resident_physical_read_bytes=0,errors=0,violations=0,insert_verification='API return, positive insert count and successful close; no readback',storage='ext4 /dev/mapper/snafu-root_crypt; O_DIRECT; fsynced fixture copies')
(C/'audit.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
