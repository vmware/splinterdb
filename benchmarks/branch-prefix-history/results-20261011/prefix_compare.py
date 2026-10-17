#!/usr/bin/env python3
"""Prefix workload ABBA; invoke once per pattern/regime under benchmark lock."""
import argparse, csv, hashlib, json, os, re, subprocess, time
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--pattern', choices=['common','random','nonlexical'], required=True)
p.add_argument('--regime', choices=['resident','disk'], required=True)
p.add_argument('--seconds',type=int,default=5)
p.add_argument('--disabled', action='store_true')
a=p.parse_args()
root=Path(__file__).resolve().parent.parent
out=root/'benchmarks/branch-prefix'/(a.regime+'-'+a.pattern+('-disabled' if a.disabled else ''))
out.mkdir(parents=True,exist_ok=False)
env=dict(os.environ,PREFIX_PATTERN=a.pattern,KVBENCH_STATS_BEFORE_CLOSE='1')
if a.disabled: env['PREFIX_DISABLE']='1'
phases=['load:random@1','warm','point:uniform@1','point:uniform@8']
phases += [f'range:uniform:{length}@{threads}' for threads in [1,8] for length in [1,10,10000]]
manifest=dict(pattern=a.pattern,regime=a.regime,io='O_DIRECT',runs=[],source='same generated kvbench source',loading_threads=1)
for index,version in enumerate(['baseline','candidate','candidate','baseline']):
    tree=root/('baseline' if version=='baseline' else 'worktrees/branch-prefix')
    binary=tree/'build/release/bin/prefix_kvbench'
    db=out/f'{index}-{version}.db'
    argv=['taskset','-c','0-11',str(binary),'--db',str(db),'--create',
          '--n','200000' if a.regime=='resident' else '2000000',
          '--cache-mib','128' if a.regime=='resident' else '64','--disk-gib','8',
          '--log','0','--threads','8','--mt-bg','0','--normal-bg','0','--memtable-mib','4',
          '--secs',str(a.seconds),'--warmup','1','--csv',str(out/'results.csv'),
          '--vtag',version,'--stag',a.regime,'--otag',str(index),'--rep',str(index//2),
          '--seed','42','--hang-secs','60','--hang-dir',str(out),*phases]
    record=dict(index=index,version=version,command=argv,started=time.time(),
                binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                build=json.loads(binary.with_suffix('.json').read_text()))
    print(f'prefix {a.pattern} {a.regime}: {index+1}/4 {version}',flush=True)
    with (out/f'{index}-{version}.log').open('w') as log:
        result=subprocess.run(argv,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=900)
    record.update(returncode=result.returncode,finished=time.time(),
                  db_allocated_bytes=db.stat().st_blocks*512,db_apparent_bytes=db.stat().st_size)
    printed=(out/f'{index}-{version}.log').read_text()
    live=re.findall(r'Currently Allocated:\s+(\d+) extents',printed)
    branch=re.findall(r'\| branch\s*\|\s+[-0-9]+ \|\s+[-0-9]+ \|\s+([0-9]+)',printed)
    record['preclose_live_extents']=[int(n) for n in live]
    record['preclose_branch_extents']=[int(n) for n in branch]
    manifest['runs'].append(record)
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if result.returncode: raise SystemExit(result.returncode)
with (out/'results.csv').open() as f: rows=list(csv.DictReader(f))
assert rows and all(int(r[x])==0 for r in rows for x in ['violations','errors'])
print('Completed',out,flush=True)
