#!/usr/bin/env python3
"""Same-source barrier matrix; one WAL/worker pair per locked invocation."""
import argparse, csv, hashlib, json, math, os, subprocess, time
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--log',type=int,choices=[0,1],required=True)
p.add_argument('--workers',type=int,choices=[0,1],required=True)
p.add_argument('--barriers',type=int)
p.add_argument('--campaign',default='')
a=p.parse_args()
root=Path(__file__).resolve().parent.parent
out=root/'benchmarks/async-durable-barrier'/(f'log{a.log}-workers{a.workers}'+('-'+a.campaign if a.campaign else ''))
out.mkdir(parents=True,exist_ok=False)
profiles={'baseline':('baseline','sync',1),'sync':('worktrees/async-durable-barrier','sync',1),
          'async1':('worktrees/async-durable-barrier','async',1),'async8':('worktrees/async-durable-barrier','async',8)}
manifest=dict(io='O_DIRECT',crash_recovery=True,batch=256,runs=[])
def run(name,index,count,pilot=False):
    tree,mode,depth=profiles[name]
    binary=root/tree/'build/release/bin/barrier_bench'
    db=out/'scratch.db'
    csv_path=out/('pilot.csv' if pilot else 'results.csv')
    argv=['taskset','-c','0-11',str(binary),str(db),str(csv_path),name,mode,str(a.log),str(a.workers),'256',str(count)]
    print(f'barrier log{a.log}/worker{a.workers}: {index} {name}, {count} barriers',flush=True)
    record=dict(version=name,index=index,command=argv,depth=depth,pilot=pilot,
                started=time.time(),build=json.loads(binary.with_suffix('.json').read_text()))
    with (out/f'{index}-{name}.log').open('w') as f:
        result=subprocess.run(argv,env=dict(os.environ,BARRIER_DEPTH=str(depth)),stdout=f,stderr=subprocess.STDOUT,timeout=300)
    record.update(returncode=result.returncode,finished=time.time())
    if db.exists(): record['db_allocated_bytes']=db.stat().st_blocks*512
    manifest['runs'].append(record)
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if result.returncode: raise SystemExit(result.returncode)
    with csv_path.open() as f: row=list(csv.DictReader(f))[-1]
    assert int(row['verified'])==1
    # Only discard this campaign's successfully crash-verified scratch image.
    # The next create must receive a fresh file, including both superblock slots.
    db.unlink()
    return row
if a.barriers: count=a.barriers
else:
    pilot=run('baseline','pilot',128,True)
    count=max(1024,min(8192,math.ceil(4.0/float(pilot['seconds'])*128/128)*128))
manifest['barriers_per_run']=count
order=['baseline','sync','async1','async8','async8','async1','sync','baseline','sync','baseline','async8','async1']
for index,name in enumerate(order): run(name,index,count)
print('Completed',out,flush=True)
