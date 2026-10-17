#!/usr/bin/env python3
"""Run one measured or build job exclusively; save command, timing and output."""
import argparse,contextlib,fcntl,hashlib,json,os,signal,subprocess,time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
LOCK=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/machine.lock')

def run(name,cmd,cwd=None,timeout=1200,env_extra=None,lock_held=False):
    out=ROOT/'jobs'/name
    out.mkdir(parents=True,exist_ok=False)
    tmp=out/'tmp';tmp.mkdir()
    env=os.environ.copy()
    for k in ('CC','LD','CFLAGS','CPPFLAGS','LDFLAGS','MAKEFLAGS','MFLAGS','MAKELEVEL','BUILD_ROOT','BUILD_MODE','TMPDIR','LD_LIBRARY_PATH'):
        env.pop(k,None)
    env.update(TMPDIR=str(tmp))
    if env_extra:env.update(env_extra)
    record={'name':name,'command':cmd,'cwd':str(cwd or ROOT),'environment_overrides':env_extra,'queued':time.time(),'machine_lock':str(LOCK)}
    print('WAIT',name,flush=True)
    with (contextlib.nullcontext() if lock_held else LOCK.open('rb')) as lock:
        if not lock_held:fcntl.flock(lock,fcntl.LOCK_EX)
        record['started']=time.time()
        print('START',name,flush=True)
        with (out/'output.log').open('w') as output:
            p=subprocess.Popen(cmd,cwd=cwd or ROOT,env=env,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
            try:
                record['returncode']=p.wait(timeout=timeout)
            except BaseException:
                os.killpg(p.pid,signal.SIGTERM)
                try:p.wait(timeout=10)
                except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
                record['returncode']='timeout-or-interruption'
                raise
            finally:
                record['finished']=time.time()
                record['log_sha256']=hashlib.sha256((out/'output.log').read_bytes()).hexdigest()
                (out/'run.json').write_text(json.dumps(record,indent=2)+'\n')
        print('END',name,record['returncode'],round(record['finished']-record['started'],2),flush=True)
        assert record['returncode']==0,(name,record['returncode'])
    return record

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('name');p.add_argument('--cwd');p.add_argument('--timeout',type=int,default=1200);p.add_argument('command',nargs=argparse.REMAINDER)
    a=p.parse_args();cmd=a.command[1:] if a.command[0]=='--' else a.command
    run(a.name,cmd,a.cwd,a.timeout)
