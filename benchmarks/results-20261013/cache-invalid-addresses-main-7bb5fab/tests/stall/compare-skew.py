#!/usr/bin/env python3
import hashlib,json,subprocess,time,os
from pathlib import Path
OUT=Path(__file__).resolve().parent
MAIN=Path('/home/rob/projects/splinterdb')
CAND=MAIN/'.cache/projects-20261011/worktrees/cache-invalid-addresses'
records=[]
cases=[('main',1,32),('main',2,32),('main',2,32),('candidate',2,32),('candidate',1,32),('candidate',0,64)]
for run,(variant,ncpus,cache_mib) in enumerate(cases):
 tree=MAIN if variant=='main' else CAND
 build=tree/'build-main-7bb5fab'
 exe=build/'debug/bin/driver_test'
 lib=build/'debug/lib/libsplinterdb.so'
 scratch=OUT/f'skew-scratch-{run}-{variant}'
 scratch.mkdir(exist_ok=True)
 link=scratch/'build-main-7bb5fab'
 if not link.exists():link.symlink_to(build,target_is_directory=True)
 command=[str(exe),'cache_test','--async','--db-location',str(scratch/'async.db'),'--cache-capacity-mib',str(cache_mib),'--db-capacity-gib','1']
 if ncpus: command=['taskset','-c',','.join(str(c) for c in sorted(os.sched_getaffinity(0))[:ncpus]),*command]
 record=dict(ncpus=ncpus,cache_mib=cache_mib,run=run,variant=variant,command=command,cwd=str(scratch),binary_sha256=hashlib.sha256(exe.read_bytes()).hexdigest(),library_sha256=hashlib.sha256(lib.read_bytes()).hexdigest(),started=time.time(),timeout_seconds=20)
 print('Run',run,variant,flush=True)
 with (OUT/f'skew-{run}-{variant}.log').open('w') as log:
  p=subprocess.Popen(command,cwd=scratch,stdout=log,stderr=subprocess.STDOUT)
  record['pid']=p.pid
  try:
   record['returncode']=p.wait(timeout=20)
   record['timed_out']=False
  except subprocess.TimeoutExpired:
   record['timed_out']=True
   gdb=['gdb','-nx','-q','-batch','-iex','set sysroot /','-iex','set solib-search-path '+str(lib.parent),'-ex','set pagination off','-ex','thread apply all bt 16','-x',str(OUT/'inspect.gdb'),str(exe),'-p',str(p.pid)]
   record['gdb_command']=gdb
   with (OUT/f'skew-{run}-{variant}-stacks-state.log').open('w') as stack:
    capture=subprocess.run(gdb,cwd=tree,stdout=stack,stderr=subprocess.STDOUT,timeout=30)
   record['gdb_returncode']=capture.returncode
   p.terminate()
   record['returncode']=p.wait(timeout=10)
  record['finished']=time.time()
 records.append(record)
 (OUT/'skew-comparison.json').write_text(json.dumps(records,indent=2)+'\n')
 print(record,flush=True)
