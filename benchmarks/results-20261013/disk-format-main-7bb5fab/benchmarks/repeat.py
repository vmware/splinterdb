"""Nearby warmed ABBA controls; invoke under run_job.py benchmark."""
import argparse
import run_matrix as r
p=argparse.ArgumentParser();p.add_argument('profile',choices=['warmup','cache-invalid-addresses','directory-bucket-atomic','async-durable-barrier']);a=p.parse_args()
def interrupted(signum,frame):raise InterruptedError(f'benchmark interrupted by signal {signum}')
for sig in [r.signal.SIGTERM,r.signal.SIGHUP]:r.signal.signal(sig,interrupted)
original=r.kvargs
def longer(*args,**kwargs):
 kwargs['secs']=10
 return original(*args,**kwargs)
r.kvargs=longer
folder,m=r.newmode('repeat-'+a.profile)
m['runner_sha256']=r.sha(__file__)
m['shared_runner_sha256']=r.sha(r.__file__)
m['scope']='Nearby main ABBA controls after broad screening; all earlier samples retained'
if a.profile=='warmup':m['scope']='Recorded workload warmup before nearby ABBA comparisons; retained separately from their samples'
phases=['warm']+(['point:uniform@1'] if a.profile=='cache-invalid-addresses' else [])+['point:uniform@8','range:uniform:1@8']
for i,name in enumerate(['main'] if a.profile=='warmup' else ['main',a.profile,a.profile,'main']):r.query_one(folder,m,i,name,'resident',phases)
m['complete']=True;r.save(folder/'manifest.json',m)
