from pathlib import Path
import subprocess, hashlib, json, re, sys, os, time
root=Path(__file__).resolve().parent
wt=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/worktrees/log-group-limit')
coordinator=root.parents[1]/'run_job.py'
mode=sys.argv[1]
jobs=[('unit/splinterdb_forked_child_test',['automatic_log_barriers_survive_process_exit']),('unit/splinterdb_quick_test',[]),('driver_test',['log_test','--db-location','log.db','--cache-capacity-mib','64','--db-capacity-gib','4']),('unit/splinter_test',[])]
validation_id=mode
if len(sys.argv)>2:
 jobs=jobs[:1]
 validation_id=mode+"-recovery-final"
records=[]
for index,(suite,flags) in enumerate(jobs):
 name=f'{validation_id}-{index}-{Path(suite).name}'
 scratch=root/'scratch'/name;scratch.mkdir(parents=True,exist_ok=True)
 link=scratch/'build-main-7bb5fab'
 if not link.exists(): link.symlink_to(wt/'build-main-7bb5fab',target_is_directory=True)
 binary=wt/'build-main-7bb5fab'/mode/'bin'/suite
 command=[sys.executable,str(coordinator),'--name','main-merge-log-group-'+name,'test','--',str(binary),*flags]
 start=time.time()
 with (root/(name+'.log')).open('w') as f:
  result=subprocess.run(command,cwd=scratch,stdout=f,stderr=subprocess.STDOUT,timeout=600)
 log=(root/(name+'.log')).read_text()
 counts=re.findall(r'RESULTS:.*',log)
 record=dict(name=name,command=command,cwd=str(scratch),returncode=result.returncode,results=counts,seconds=time.time()-start,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),log_sha256=hashlib.sha256((root/(name+'.log')).read_bytes()).hexdigest())
 records.append(record)
 (root/(validation_id+'-validation.json')).write_text(json.dumps(records,indent=2)+'\n')
 print(name,result.returncode,counts,flush=True)
 if result.returncode or (suite.startswith('unit/') and (not counts or '0 tests' in counts[-1])): sys.exit(1)
