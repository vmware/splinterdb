"""Controlled combined-format/predicate benchmarks. Run via run_job benchmark only.
Main/disk/combined ABC CBA comparisons use pinned workers and one gated phase
per process; same-format combined and disk-format share immutable fixtures.
"""
import argparse,csv,hashlib,json,os,re,signal,subprocess,time
from pathlib import Path
ROOT=Path('/home/rob/projects/splinterdb');HERE=Path(__file__).resolve().parent
OUT=ROOT/'.cache/projects-20261011/benchmarks/filtered-disk-pr-20261013'
OLD=ROOT/'.cache/projects-20261011/benchmarks/filter-predicate-20261013/fixtures/fixtures.json'
CPUS=[0,2,4,6,8,10,1,3]
ORDER=['main','disk-format','combined','combined','disk-format','main']
EVENTS='{cpu_core/cycles/u,cpu_core/ref-cycles/u,cpu_core/instructions/u,cpu_core/branches/u,cpu_core/branch-misses/u},task-clock,context-switches,cpu-migrations'
def sha(p):
 h=hashlib.sha256()
 with Path(p).open('rb') as f:
  for c in iter(lambda:f.read(1048576),b''):h.update(c)
 return h.hexdigest()
def save(p,x):p.write_text(json.dumps(x,indent=2)+'\n')
def power_snapshot():
 result={'time':time.time(),'thermal':{},'cpu':{}}
 for zone in Path('/sys/class/thermal').glob('thermal_zone*'):
  try:result['thermal'][zone.name]={'type':(zone/'type').read_text().strip(),'millidegrees':int((zone/'temp').read_text())}
  except (OSError,ValueError):pass
 for cpu in range(12):
  base=Path('/sys/devices/system/cpu')/f'cpu{cpu}';values={}
  for key in ['cpufreq/scaling_cur_freq','cpufreq/scaling_governor','thermal_throttle/core_throttle_count','thermal_throttle/package_throttle_count','thermal_throttle/core_throttle_total_time_ms','thermal_throttle/package_throttle_total_time_ms']:
   try:values[key]=(base/key).read_text().strip()
   except OSError:pass
  result['cpu'][str(cpu)]=values
 for name in ['meminfo','vmstat','diskstats','pressure/cpu','pressure/io','pressure/memory']:
  try:result[name]=Path('/proc',name).read_text()
  except OSError:pass
 return result

def build(name):
 bfile=HERE/(name+'-build.json');b=json.loads(bfile.read_text());tree=Path(b['tree'])
 assert sha(b['library'])==b['library_sha256'] and all(sha(tree/f)==v for f,v in b['source_files_sha256'].items())
 for exe in b['binaries'].values():assert sha(exe['binary'])==exe['binary_sha256'] and sha(exe['source'])==exe['source_sha256']
 return bfile,b

def mode_start(mode):
 folder=OUT/mode;folder.mkdir(parents=True,exist_ok=False)
 fs=subprocess.check_output(['findmnt','-n','-o','FSTYPE,SOURCE','-T',str(folder)],text=True).strip()
 assert fs.split()[0] not in ('tmpfs','ramfs') and fs.split()[1].startswith('/dev/')
 m=dict(mode=mode,storage=fs,io='O_DIRECT',runner_sha256=sha(__file__),runs=[],complete=False)
 save(folder/'manifest.json',m);(folder/'runner.py').write_bytes(Path(__file__).read_bytes());return folder,m

def run(folder,m,label,name,binary,args,environment,affinity='0-11',timeout=300):
 bfile,b=build(name);exe=b['binaries'][binary];dst=folder/label;dst.mkdir(exist_ok=True)
 ctl=dst/'control.fifo';ack=dst/'ack.fifo';os.mkfifo(ctl);os.mkfifo(ack)
 env=dict(environment,KVBENCH_PERF_CONTROL=str(ctl),KVBENCH_PERF_ACK=str(ack))
 command=['taskset','-c',affinity,'perf','stat','-D','-1','--control',f'fifo:{ctl},{ack}','-x,','-e',EVENTS,'-o',str(dst/'perf-stat.csv'),'--',exe['binary'],*map(str,args)]
 rec=dict(label=label,profile=name,command=command,environment=env,build_manifest=str(bfile),build_manifest_sha256=sha(bfile),binary_sha256=exe['binary_sha256'],library_sha256=b['library_sha256'],started=time.time(),power_start=power_snapshot(),verified=False)
 m['runs'].append(rec);save(folder/'manifest.json',m);print('START '+label,flush=True)
 with (dst/'output.log').open('w') as log:
  proc=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=dict(os.environ,**env),start_new_session=True)
  try:proc.wait(timeout=timeout)
  except BaseException:
   try:os.killpg(proc.pid,signal.SIGKILL)
   except ProcessLookupError:pass
   proc.wait();raise
 rec.update(returncode=proc.returncode,finished=time.time(),power_end=power_snapshot());save(folder/'manifest.json',m);assert proc.returncode==0,(label,proc.returncode)
 log=(dst/'output.log').read_text();assert 'FINDING: splinterdb_close' not in log
 counters={}
 for line in (dst/'perf-stat.csv').read_text().splitlines():
  if not line or line.startswith('#'):continue
  x=line.split(',');assert len(x)>=5 and float(x[4])>=99.0,(label,x)
  counters[x[2]]=float(x[0])
 rec['counters']=counters
 ctl.unlink();ack.unlink();return dst,rec,log

def ordinary(regime,smoke=False,repeat=False):
 folder,m=mode_start('repeat-disk-range1' if repeat else ('smoke' if smoke else regime));secs=12 if repeat else (2 if smoke else (15 if regime=='resident' else 8))
 order=['disk-format','combined','combined','disk-format','disk-format','combined'] if repeat else (['disk-format','combined'] if smoke else ORDER)
 phases=['range:uniform:1@8'] if repeat else (['point:uniform@8'] if smoke else ['point:uniform@8','range:uniform:1@8','range:uniform:10000@8'])
 m.update(order=order,phases=phases,seconds=secs,cpu_map=CPUS,full_warm_scan_threads=1)
 fixtures=json.loads(OLD.read_text())
 for phase in phases:
  for i,name in enumerate(order):
   fixture=fixtures[regime+'-'+('main' if name=='main' else 'disk-format')];src=Path(fixture['fixture']);assert sha(src)==fixture['fixture_sha256']
   label=phase.replace(':','-').replace('@','-')+f'-{i:02d}-{name}';dst=folder/label;dst.mkdir();db=dst/'scratch.db'
   subprocess.run(['cp','--reflink=auto','--sparse=always',str(src),str(db)],check=True)
   with db.open('rb') as f:os.fsync(f.fileno())
   count=200000 if regime=='resident' else 2000000
   args=['--db',db,'--open','--n',count,'--cache-mib',128 if regime=='resident' else 64,'--disk-gib',8,'--log',0,'--threads',8,'--mt-bg',0,'--normal-bg',0,'--memtable-mib',4,'--secs',secs,'--warmup',1,'--csv',dst/'results.csv','--vtag',name,'--stag',regime,'--otag',i,'--rep',i,'--seed',42,'--hang-secs',60,'--hang-dir',dst,'warm@1',phase]
   dst,rec,log=run(folder,m,label,name,'kvbench_controlled',args,dict(KVBENCH_CPU_MAP=','.join(map(str,CPUS)),KVBENCH_SERIAL_REGISTER='1'),timeout=360)
   with (dst/'results.csv').open() as f:rows=list(csv.DictReader(f))
   assert [r['phase'] for r in rows]==['warm@1',phase] and all(int(r['errors'])==0 and int(r['violations'])==0 and int(r['ops'])>0 for r in rows)
   assert int(rows[0]['ops'])==count
   if regime=='resident':assert int(rows[1]['rd_bytes'])==0
   registrations=[list(map(int,x)) for x in re.findall(r'CONTROL worker=(\d+) tid=(\d+) assigned_cpu=(\d+) actual_cpu=(\d+)',log)]
   assert len(registrations)==9 and all(cpu==CPUS[worker] and actual==cpu for worker,tid,cpu,actual in registrations)
   assert [r[0] for r in registrations]==[0]+list(range(8))
   if m['runs'][:-1]:assert registrations==m['runs'][0]['registrations']
   assert sha(src)==fixture['fixture_sha256']
   rec.update(verified=True,rows=rows,registrations=registrations,fixture=fixture,fixture_copy_fsynced=True)
   save(folder/'manifest.json',m);db.unlink();print('DONE '+label+' ops/s='+rows[1]['ops_s'],flush=True)
 m['complete']=True;save(folder/'manifest.json',m)

def expected_checksum(rows,queries,limit):
 seed=42;checksum=0
 for q in range(queries):
  seed=(seed*6364136223846793005+1442695040888963407)&((1<<64)-1);start=(seed>>32)%(rows-limit)
  checksum+=limit*(start+q%32)+limit*(limit-1)//2
 return checksum

def feature():
 folder,m=mode_start('feature-v2');count=10000;n=16384
 cases=['application','predicate','pruned','pruned','predicate','application']
 m.update(queries_per_limit=count,rows_per_tenant=n,warmup_queries_per_limit=500,order=cases,cpu_affinity='0',counter_scope='Sum of four measured query limits, excluding setup/warmup/close')
 for collisions in [0,1]:
  for i,mode in enumerate(cases):
   label=f'collisions{collisions}-{i:02d}-{mode}';db=folder/(label+'.db');assert not db.exists()
   dst,rec,log=run(folder,m,label,'combined','hash_range_benchmark',[db,n,count],dict(FILTER_MODE=mode,FILTER_HASH_COLLISIONS=str(collisions)),affinity='0',timeout=900)
   lines=log.splitlines();assert sum(line.startswith('loaded=524288 ') for line in lines)==1
   header=next(i for i,line in enumerate(lines) if line.startswith('mode,collisions,'));csvlines=[lines[header]]+[line for line in lines[header+1:] if line.startswith(mode+',')];rr=list(csv.DictReader(csvlines));assert len(rr)==4
   assert [int(r['limit']) for r in rr]==[1,2,10,100]
   for r in rr:
    limit=int(r['limit']);assert r['mode']==mode and int(r['collisions'])==collisions and int(r['queries'])==count and int(r['keys_per_tenant'])==n
    assert int(r['checksum'])==expected_checksum(n,count,limit) and int(r['warmup_queries'])==500 and float(r['ns_per_query'])>0 and float(r['cpu_s'])>0
    if mode!='application':assert int(r['visited'])==limit*count
    else:assert int(r['predicate_calls'])==int(r['visited'])
   rec.update(verified=True,verification_kind='exact key/value/count and independent checksum for every query',rows=rr,errors=0,violations=0,allocated_bytes=db.stat().st_blocks*512)
   save(folder/'manifest.json',m);db.unlink();print('DONE '+label,flush=True)
 m['complete']=True;save(folder/'manifest.json',m)

def interrupted(sig,frame):raise InterruptedError(str(sig))
if __name__=='__main__':
 for sig in (signal.SIGTERM,signal.SIGHUP):signal.signal(sig,interrupted)
 p=argparse.ArgumentParser();p.add_argument('mode',choices=['smoke','resident','disk','feature','repeat-disk-range1']);a=p.parse_args()
 if a.mode=='repeat-disk-range1':
  EVENTS='{cpu_core/cycles/u,cpu_core/ref-cycles/u,cpu_core/instructions/u,cpu_core/branches/u,cpu_core/branch-misses/u,cpu_core/cycles/k,cpu_core/instructions/k},task-clock,context-switches,cpu-migrations'
  ordinary('disk',repeat=True)
 elif a.mode=='feature':feature()
 else:ordinary('resident' if a.mode=='smoke' else a.mode,a.mode=='smoke')
