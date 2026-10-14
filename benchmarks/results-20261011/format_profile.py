#!/usr/bin/env python3
import hashlib,json,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parent.parent
out=root/'benchmarks/disk-format/resident-profile'
out.mkdir(parents=True,exist_ok=False)
manifest=[]
for version,slot in [('baseline',0),('candidate',1)]:
    tree=root/('baseline' if version=='baseline' else 'worktrees/disk-format')
    binary=tree/'build/release/bin/kvbench'
    fixture=root/f'benchmarks/disk-format/resident-log0-final-singleload/{slot}-{version}.db'
    for threads in [1,8]:
        stem=out/f'{version}-t{threads}'
        db=stem.with_suffix('.db')
        subprocess.run(['cp','--reflink=auto','--sparse=always',str(fixture),str(db)],check=True)
        perf=stem.with_suffix('.perf')
        argv=['perf','record','-e','cycles:u','-F','997','-o',str(perf),'--',
              'taskset','-c','0-11',str(binary),'--db',str(db),'--open','--n','200000',
              '--cache-mib','128','--disk-gib','8','--log','0','--threads',str(threads),
              '--mt-bg','1','--normal-bg','3','--memtable-mib','4','--secs','15',
              '--warmup','1','--seed','42','--hang-secs','60','warm',f'point:uniform@{threads}']
        record=dict(version=version,threads=threads,fixture=str(fixture),command=argv,
                    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),started=time.time())
        print('Format profile',version,threads,flush=True)
        with stem.with_suffix('.log').open('w') as f:
            result=subprocess.run(argv,stdout=f,stderr=subprocess.STDOUT,timeout=120)
        record.update(returncode=result.returncode,finished=time.time());manifest.append(record)
        (out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        if result.returncode:raise SystemExit(result.returncode)
        with stem.with_suffix('.txt').open('w') as f:
            subprocess.run(['perf','report','--stdio','--no-children','--percent-limit','0.3','--sort','symbol','-i',str(perf)],stdout=f,check=True)
print('Completed',out,flush=True)
