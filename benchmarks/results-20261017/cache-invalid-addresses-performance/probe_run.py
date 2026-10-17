#!/usr/bin/env python3
import argparse,csv,fcntl,hashlib,json,sys,time
from pathlib import Path
from job import ROOT,run

p=argparse.ArgumentParser()
p.add_argument('label');p.add_argument('--profiles',nargs='+',default=['main','original','outlined','outlined','original','main'])
p.add_argument('--apis',nargs='+',default=['blocking','async'])
p.add_argument('--threads',nargs='+',type=int,default=[1,8]);p.add_argument('--seconds',type=int,default=3)
p.add_argument('--cache-mib',type=int,default=64);p.add_argument('--mode',default='both')
p.add_argument('--resume',action='store_true')
p.add_argument('--counters',action='store_true')
p.add_argument('--exclusive',action='store_true')
a=p.parse_args();out=ROOT/'probe-results'/a.label;out.mkdir(parents=True,exist_ok=a.resume)
binary=ROOT/('harness/counters/cache_api_probe' if a.counters else 'harness/cache_api_probe')
metadata={'arguments':vars(a),'driver_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),
          'libraries':{n:hashlib.sha256((ROOT/'builds'/n/'release/lib/libsplinterdb.so').read_bytes()).hexdigest() for n in a.profiles},'runs':[]}
rows=[]
suite_lock=None
if a.exclusive:
 from job import LOCK
 print('WAIT suite machine.lock',flush=True)
 suite_lock=LOCK.open('rb');fcntl.flock(suite_lock,fcntl.LOCK_EX)
 metadata['suite_lock_acquired']=time.time()
if a.resume:
 metadata=json.loads((out/'manifest.json').read_text())
 rows=list(csv.DictReader((out/'results.csv').open()))
 assert metadata['driver_sha256']==hashlib.sha256(binary.read_bytes()).hexdigest()
 for n,digest in metadata['libraries'].items():
  assert digest==hashlib.sha256((ROOT/'builds'/n/'release/lib/libsplinterdb.so').read_bytes()).hexdigest()
for api in a.apis:
 for threads in a.threads:
  for i,profile in enumerate(a.profiles):
   name=f'{a.label}-{api}-{threads}-{i:02d}-{profile}'
   if any(row['job']==name for row in rows):
    print('RESUMED',name,flush=True);continue
   prior=ROOT/'jobs'/name
   if prior.exists() and not (prior/'run.json').exists():
    prior.rename(prior.with_name(name+'-interrupted'))
    oldscratch=ROOT/'scratch'/name
    if oldscratch.exists():oldscratch.rename(oldscratch.with_name(name+'-interrupted'))
   scratch=ROOT/'scratch'/name;scratch.mkdir(parents=True)
   db=scratch/'cache.db';libdir=ROOT/'builds'/profile/'release/lib'
   cmd=['taskset','-c','2' if threads==1 else '0-11',str(binary),str(db),str(a.cache_mib),str(threads),str(a.seconds),a.mode,api]
   info=run(name,cmd,scratch,timeout=300,env_extra={'LD_LIBRARY_PATH':str(libdir)},lock_held=a.exclusive)
   lines=(ROOT/'jobs'/name/'output.log').read_text().splitlines()
   header=next(i for i,s in enumerate(lines) if s.startswith('mode,api,'))
   parsed=list(csv.DictReader(lines[header:]))
   assert parsed
   for row in parsed:
    assert int(row['errors'])==0,row
    if a.mode!='churn':assert int(row['read_bytes'])==int(row['write_bytes'])==int(row['major_faults'])==0,row
    row.update(profile=profile,order=i,job=name)
    rows.append(row)
    print('RESULT',profile,api,threads,row['mode'],row['ops_s'],flush=True)
   metadata['runs'].append(info)
   (out/'manifest.json').write_text(json.dumps(metadata,indent=2)+'\n')
   with (out/'results.csv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
   db.unlink()
metadata['finished']=time.time()
(out/'manifest.json').write_text(json.dumps(metadata,indent=2)+'\n')
print('COMPLETE',a.label,len(rows),'rows',flush=True)
