from pathlib import Path
import csv,hashlib,json,shutil,statistics,subprocess
coord=Path(__file__).resolve().parent
camp=coord.parent.parent
wt=camp/'worktrees/log-group-limit'
runs=camp/'benchmarks/log-pr-1024-20261014/default-check'
archive=wt/'benchmarks/results-20261014/log-group-limit-pr'
manifest=json.loads((runs/'manifest.json').read_text())
assert manifest['complete'] and len(manifest['runs'])==12
assert all(r['verified'] and r['returncode']==0 for r in manifest['runs'])
for mode in ['debug','release']:
 tests=json.loads((coord/(mode+'-validation.json')).read_text())
 assert len(tests)==4 and all(t['returncode']==0 for t in tests)
 assert 'log returned 500000 of 500000 entries' in (coord/(mode+'-3-driver_test.log')).read_text()
archive.mkdir(parents=True,exist_ok=True)
(archive/'.clang-format').write_text('DisableFormat: true\n')
(archive/'.gitattributes').write_text('*.csv whitespace=cr-at-eol\n*.patch -whitespace\n')
shutil.copytree(runs,archive/'performance',dirs_exist_ok=True)
for name in ['build_benchmarks.py','run_benchmarks.py','runner_core.py','insert_benchmark.c','run_validation.py','format_final.py','archive.py','main-build.json','candidate-build.json','default1024.patch','benchmark-build.log','benchmark.log','format.log','debug-build-matched.log','release-build-matched.log']:
 shutil.copyfile(coord/name,archive/name)
(archive/'tests').mkdir(exist_ok=True)
for mode in ['debug','release']:
 for p in coord.glob(mode+'-[0-9]-*.log'):shutil.copyfile(p,archive/'tests'/p.name)
 shutil.copyfile(coord/(mode+'-validation.json'),archive/'tests'/(mode+'-validation.json'))
rows=list(csv.DictReader((runs/'results.csv').open()))
summary=[]
metrics=['ops_s','drain_ops_s','close_s','cpu_us_op','p99_us','p999_us']
for threads in [1,8]:
 means={}
 for profile in ['main','default1024','explicit1024']:
  cells=[r for r in rows if int(r['threads'])==threads and r['version']==profile]
  means[profile]={metric:statistics.mean(float(r[metric]) for r in cells) for metric in metrics}
 for profile in means:
  record=dict(threads=threads,profile=profile,n=2,**means[profile])
  record['throughput_change_percent']=100*(record['ops_s']/means['main']['ops_s']-1)
  record['including_close_change_percent']=100*(record['drain_ops_s']/means['main']['drain_ops_s']-1)
  record['throughput_samples']=[float(r['ops_s']) for r in rows if int(r['threads'])==threads and r['version']==profile]
  summary.append(record)
(archive/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
files={str(p.relative_to(archive)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(archive.rglob('*')) if p.is_file() and p.name!='sha256.json'}
(archive/'sha256.json').write_text(json.dumps(files,indent=2)+'\n')
