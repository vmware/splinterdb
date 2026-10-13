"""Produce compact per-project archives without fixtures or executables."""
import csv,hashlib,json,shutil,subprocess
from pathlib import Path
COORD=Path(__file__).resolve().parent
OUT=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/benchmarks/main-7bb5fab')
DEST=COORD/'project-archives'
PROJECTS=['disk-format','filtered-ranges','log-group-limit','branch-prefix','lazy-merge','cache-directory','async-durable-barrier','cache-invalid-addresses']
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def readcsv(p):
 with p.open() as f:return list(csv.DictReader(f))
def writecsv(p,rs):
 if not rs:return
 with p.open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(rs[0]),lineterminator='\n');w.writeheader();w.writerows(rs)
subprocess.run(['python3',str(COORD/'report.py')],check=True)
summary=readcsv(COORD/'summary.csv')
expected={'fixtures':12,'resident':23,'disk':23,'prefix-resident-common':4,'prefix-resident-random':4,'prefix-disk-common':4,'prefix-disk-random':4,'insert':31,'hash':4,'barrier':24,'repeat-warmup':1,'repeat-cache-invalid-addresses':4,'repeat-directory-bucket-atomic':4,'repeat-async-durable-barrier':4}
for mode,count in expected.items():
 manifest=json.loads((OUT/mode/'manifest.json').read_text())
 assert manifest['complete'] and len(manifest['runs'])==count and all(r['verified'] and r['returncode']==0 for r in manifest['runs']),mode

for project in PROJECTS:
 profiles=[project] if project!='cache-directory' else ['directory-'+x for x in ['direct','bucket','bucket-simd','bucket-atomic']]
 versions=profiles+(['log-disabled','log-default'] if project=='log-group-limit' else [])
 path=DEST/project;path.mkdir(parents=True,exist_ok=False)
 own=[r for r in summary if r['version'] in versions]
 for r in own:r.update(scope='new main versus updated feature; merge cost not isolated')
 writecsv(path/'summary.csv',own)
 source_names={'kvbench.c','insert_benchmark.c'}
 fixtures=json.loads((OUT/'fixtures/manifest.json').read_text());assert fixtures['complete'] and all(r['verified'] for r in fixtures['runs'])
 fixtures['runs']=[r for r in fixtures['runs'] if r['profile'] in ['main',*profiles] and (('ordinary' not in r['label']) if project=='branch-prefix' else ('ordinary' in r['label']))]
 fixture_out=path/'fixtures';fixture_out.mkdir();write(fixture_out/'manifest.json',fixtures)
 for record in fixtures['runs']:
  for suffix in ['.csv','.log']:
   file=OUT/'fixtures'/(record['label']+suffix);shutil.copyfile(file,fixture_out/file.name)
 if project=='branch-prefix':source_names.add('prefix_kvbench.c')
 if project=='async-durable-barrier':source_names.add('barrier_bench.c')
 if project=='filtered-ranges':source_names.add('hash_range_benchmark.c')
 for name in source_names:shutil.copyfile(COORD/'sources'/name,path/name)
 for name in ['run_matrix.py','build_all.py','report.py','package.py','preparation.json','feature-summaries.json','host.json','run_matrix-fixtures.py','control-drift.csv','repeat.py','audit.json','audit.py','report.md','benchmark-jobs.json','run_job.py']:shutil.copyfile(COORD/name,path/name)
 for profile in ['main',*profiles]:
  build=json.loads((COORD/(profile+'-build.json')).read_text());build['binaries']={name:value for name,value in build['binaries'].items() if name+'.c' in source_names};write(path/(profile+'-build.json'),build)
 tests=[]
 for folder in OUT.iterdir():
  if not folder.is_dir() or folder.name=='fixtures' or not (folder/'manifest.json').exists():continue
  m=json.loads((folder/'manifest.json').read_text())
  assert m.get('complete') and m['runs'] and all(r['verified'] for r in m['runs']),folder
  selected=[r for r in m['runs'] if r['profile'] in profiles]
  if folder.name=='repeat-warmup' and project in ['async-durable-barrier','cache-invalid-addresses','cache-directory']:selected=m['runs']
  if not selected:continue
  target=path/folder.name;target.mkdir()
  rr=[r for r in m['runs'] if r['profile'] in profiles+['main']]
  m['runs']=rr;write(target/'manifest.json',m)
  for r in rr:
   for file in folder.glob(r['label']+'.*'):
    if file.suffix in ['.csv','.log']:shutil.copyfile(file,target/file.name)
  if (folder/'results.csv').exists():
   rows=readcsv(folder/'results.csv')
   if 'version' in rows[0]:rows=[r for r in rows if r['version'] in versions+['main']]
   elif 'label' in rows[0]:rows=[r for r in rows if r['label'] in [x['label'] for x in rr]]
   writecsv(target/'results.csv',rows)
  tests.append(dict(workload=folder.name,runs=len(rr),feature_runs=sum(r['profile'] in profiles for r in rr),warmup_runs=len(rr) if folder.name=='repeat-warmup' else 0,verified=sum(r['verified'] for r in rr),complete=m['complete']))
 assert own and tests,project
 write(path/'status.json',dict(project=project,scope='new main versus updated feature; merge cost not isolated',workloads=tests,reported_errors=sum(int(r['errors']) for r in own),reported_violations=sum(int(r['violations']) for r in own),all_runs_verified=bool(tests) and all(x['verified']==x['runs'] and x['complete'] for x in tests)))
 inventory={str(p.relative_to(path)):sha(p) for p in path.rglob('*') if p.is_file()};write(path/'inventory.json',inventory)
print(DEST)
