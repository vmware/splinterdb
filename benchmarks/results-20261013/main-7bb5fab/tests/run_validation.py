from pathlib import Path
import subprocess, hashlib, json, re, sys, os, time
root=Path(__file__).resolve().parent
wt=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/worktrees/async-durable-barrier')
coordinator=root.parents[1]/'run_job.py'
mode=sys.argv[1]
jobs=[('notification_test',[]),('notification_test',['--use-shmem']),('splinterdb_optimize_test',[]),('splinterdb_optimize_test',['--use-shmem']),('splinterdb_quick_test',[]),('splinter_test',[])]
if mode=='debug-asan': jobs=jobs[:4]
records=[]
for index,(suite,flags) in enumerate(jobs):
 name=f'{mode}-{index}-{suite}'
 scratch=root/'scratch'/name;scratch.mkdir(parents=True,exist_ok=True)
 link=scratch/'build-main-7bb5fab'
 if not link.exists(): link.symlink_to(wt/'build-main-7bb5fab',target_is_directory=True)
 binary=wt/'build-main-7bb5fab'/mode/'bin/unit'/suite
 command=[sys.executable,str(coordinator),'--name','main-merge-async-'+name,'test','--',str(binary),*flags]
 env=dict(os.environ, ASAN_OPTIONS='detect_leaks=0:abort_on_error=1')
 start=time.time()
 with (root/(name+'.log')).open('w') as f:
  result=subprocess.run(command,cwd=scratch,stdout=f,stderr=subprocess.STDOUT,timeout=600,env=env)
 log=(root/(name+'.log')).read_text()
 counts=re.findall(r'RESULTS:.*',log)
 record=dict(name=name,command=command,cwd=str(scratch),returncode=result.returncode,results=counts,seconds=time.time()-start,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),log_sha256=hashlib.sha256((root/(name+'.log')).read_bytes()).hexdigest())
 records.append(record)
 (root/(mode+'-validation.json')).write_text(json.dumps(records,indent=2)+'\n')
 print(name,result.returncode,counts,flush=True)
 if result.returncode or not counts: sys.exit(1)
