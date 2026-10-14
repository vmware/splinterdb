from pathlib import Path
import subprocess,json,hashlib,re,sys,time
root=Path(__file__).resolve().parent
wt=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/worktrees/log-group-limit')
mode=sys.argv[1]
jobs=[('unit/splinter_test',[]),('unit/splinterdb_quick_test',[]),('unit/splinterdb_forked_child_test',['automatic_log_barriers_survive_process_exit']),('driver_test',['log_test','--db-location','log.db','--cache-capacity-mib','64','--db-capacity-gib','4'])]
records=[]
for i,(suite,args) in enumerate(jobs):
 name=f'{mode}-{i}-{Path(suite).name}'
 scratch=root/'scratch'/name;scratch.mkdir(parents=True,exist_ok=True)
 for bn in ['build-main-fc0bc8f7','build']:
  link=scratch/bn
  if not link.exists():link.symlink_to(wt/'build-main-fc0bc8f7',target_is_directory=True)
 binary=wt/'build-main-fc0bc8f7'/mode/'bin'/suite
 cmd=[sys.executable,str(root.parent/'run_job.py'),'--name','log-async-fc0-'+name,'test','--',str(binary),*args]
 start=time.time()
 with (root/(name+'.log')).open('w') as f:r=subprocess.run(cmd,cwd=scratch,stdout=f,stderr=subprocess.STDOUT,timeout=900)
 log=(root/(name+'.log')).read_text();results=re.findall(r'RESULTS:.*',log)
 records.append(dict(command=cmd,cwd=str(scratch),returncode=r.returncode,seconds=time.time()-start,results=results,binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()))
 (root/(mode+'-validation.json')).write_text(json.dumps(records,indent=2)+'\n')
 print(name,r.returncode,results,flush=True)
 if r.returncode:sys.exit(1)
