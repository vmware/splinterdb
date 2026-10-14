import argparse,csv,hashlib,json,os,subprocess,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--pattern',default='common');p.add_argument('--regime',default='resident');a=p.parse_args()
root=Path(__file__).resolve().parent.parent
out=root/'benchmarks/branch-prefix'/(a.regime+'-'+a.pattern+'-fixed-final');out.mkdir(exist_ok=False)
profiles={'baseline':(root/'baseline/build/release/bin/prefix_kvbench','0-baseline.db'), 'pre-reconstruct':(root/'worktrees/branch-prefix/build/release/bin/prefix_kvbench','1-candidate.db'), 'candidate':(root/'worktrees/branch-prefix/build-reconstruct/release/bin/prefix_kvbench','1-candidate.db')}
manifest=dict(runs=[],io='O_DIRECT',note='Identical candidate image for old/new reconstruction; own incompatible baseline image, serial load with no background workers.')
order=['baseline','pre-reconstruct','candidate','candidate','pre-reconstruct','baseline'] if a.regime=='resident' else ['baseline','candidate','candidate','baseline']
phases=['warm','point:uniform@1','point:uniform@8','range:uniform:1@1','range:uniform:1@8','range:uniform:10000@1','range:uniform:10000@8']
for index,version in enumerate(order):
 binary,fixture=profiles[version];source=root/'benchmarks/branch-prefix'/(a.regime+'-'+a.pattern)/fixture;db=out/'scratch.db'
 subprocess.run(['cp','--reflink=auto','--sparse=always',str(source),str(db)],check=True)
 argv=['taskset','-c','0-11',str(binary),'--db',str(db),'--open','--n','200000' if a.regime=='resident' else '2000000','--cache-mib','128' if a.regime=='resident' else '64','--disk-gib','8','--log','0','--threads','8','--mt-bg','0','--normal-bg','0','--memtable-mib','4','--secs','7','--warmup','1','--csv',str(out/'results.csv'),'--vtag',version,'--stag',a.regime,'--otag',str(index),'--rep',str(index//2),'--seed','42','--hang-secs','60','--hang-dir',str(out),*phases]
 record=dict(index=index,version=version,command=argv,fixture=str(source),started=time.time(),binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
 print('Prefix fixed comparison',a.regime,a.pattern,index,version,flush=True)
 with (out/f'{index}-{version}.log').open('w') as f:result=subprocess.run(argv,env=dict(os.environ,PREFIX_PATTERN=a.pattern),stdout=f,stderr=subprocess.STDOUT,timeout=180)
 record.update(returncode=result.returncode,finished=time.time());manifest['runs'].append(record);(out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 if result.returncode:raise SystemExit(result.returncode)
 rows=list(csv.DictReader((out/'results.csv').open()));assert all(int(r[x])==0 for r in rows for x in ['violations','errors']);db.unlink()
print('Completed',out,flush=True)
