#!/usr/bin/env python3
import csv,hashlib,json,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parent.parent
out=root/'benchmarks/disk-format/resident-scalar-fixed'
out.mkdir(parents=True,exist_ok=False)
profiles={'baseline':(root/'baseline/build/release/bin/kvbench','0-baseline.db'),
          'pre-scalar':(root/'worktrees/disk-format/build/release/bin/kvbench','1-candidate.db'),
          'candidate':(root/'worktrees/disk-format/build-scalar/release/bin/kvbench','1-candidate.db')}
manifest=dict(runs=[],io='O_DIRECT',note='Old and new format reader binaries use copies of the exact same candidate fixture; baseline uses its own incompatible format fixture.')
for index,version in enumerate(['baseline','pre-scalar','candidate','candidate','pre-scalar','baseline']):
    binary,fixture=profiles[version]
    source=root/'benchmarks/disk-format/resident-log0-final-singleload'/fixture
    db=out/f'{index}-{version}.db'
    subprocess.run(['cp','--reflink=auto','--sparse=always',str(source),str(db)],check=True)
    argv=['taskset','-c','0-11',str(binary),'--db',str(db),'--open','--n','200000',
          '--cache-mib','128','--disk-gib','8','--log','0','--threads','8',
          '--mt-bg','1','--normal-bg','3','--memtable-mib','4','--secs','10','--warmup','1',
          '--csv',str(out/'results.csv'),'--vtag',version,'--stag','resident','--otag',str(index),
          '--rep',str(index//3),'--seed','42','--hang-secs','60','--hang-dir',str(out),'warm',
          'point:uniform@1','point:uniform@8','range:uniform:1@1','range:uniform:1@8']
    record=dict(index=index,version=version,command=argv,fixture=str(source),started=time.time(),
                binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    print('Format scalar comparison',index+1,version,flush=True)
    with (out/f'{index}-{version}.log').open('w') as f:
        result=subprocess.run(argv,stdout=f,stderr=subprocess.STDOUT,timeout=120)
    record.update(returncode=result.returncode,finished=time.time());manifest['runs'].append(record)
    (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if result.returncode:raise SystemExit(result.returncode)
rows=list(csv.DictReader((out/'results.csv').open()))
assert all(int(r[x])==0 for r in rows for x in ['violations','errors'])
print('Completed',out,flush=True)
