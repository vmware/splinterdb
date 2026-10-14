#!/usr/bin/env python3
import hashlib,json,re,subprocess,sys,time
from pathlib import Path
OUT=Path(__file__).resolve().parent
TREE=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/worktrees/branch-prefix')
B=TREE/'build-main-7bb5fab'
mode=sys.argv[1]
cases=[('quick','splinterdb_quick_test',[]),('btree','btree_test',[]),('superblock','superblock_test',[]),('blob','splinter_test',['test_blob_'])]
records=[]
for name,suite,args in cases:
    scratch=OUT/('scratch-'+mode+'-'+name)
    scratch.mkdir(exist_ok=True)
    link=scratch/'build-main-7bb5fab'
    if not link.exists(): link.symlink_to(B,target_is_directory=True)
    exe=B/mode/'bin/unit'/suite
    command=[str(exe),*args]
    record=dict(name=name,mode=mode,cwd=str(scratch),command=command,binary_sha256=hashlib.sha256(exe.read_bytes()).hexdigest(),started=time.time())
    print('Running',mode,name,flush=True)
    with (OUT/(mode+'-'+name+'.log')).open('w') as log:
        p=subprocess.run(command,cwd=scratch,stdout=log,stderr=subprocess.STDOUT,timeout=600)
    record.update(returncode=p.returncode,finished=time.time())
    text=(OUT/(mode+'-'+name+'.log')).read_text()
    counts=re.findall(r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)',text)
    if counts:
        record.update(zip(['tests','passed','failed','skipped'],map(int,counts[-1])))
    records.append(record)
    (OUT/(mode+'-results.json')).write_text(json.dumps(records,indent=2)+'\n')
    print(record,flush=True)
    assert p.returncode==0 and record.get('passed',0)>0 and record.get('failed',-1)==0
