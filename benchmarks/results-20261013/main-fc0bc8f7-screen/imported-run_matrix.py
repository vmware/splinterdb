"""Fresh main integration screening. Invoke every mode under run_job.py benchmark."""
import argparse,csv,hashlib,json,os,re,resource,signal,statistics,subprocess,time
from pathlib import Path
ROOT=Path('/home/rob/projects/splinterdb'); CAMP=ROOT/'.cache/projects-20261011'
COORD=Path(__file__).resolve().parent; OUT=CAMP/'benchmarks/main-7bb5fab'
VARIANTS=['disk-format','filtered-ranges','log-group-limit','lazy-merge','async-durable-barrier','cache-invalid-addresses','directory-direct','directory-bucket','directory-bucket-simd','directory-bucket-atomic']
ORDER=['main',*VARIANTS,'main',*reversed(VARIANTS),'main']
def sha(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def save(path,value):path.write_text(json.dumps(value,indent=2)+'\n')
def rows(path):
 with path.open() as f:return list(csv.DictReader(f))
def csvwrite(path,data):
 if not data:return
 with path.open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(data[0]),lineterminator='\n');w.writeheader();w.writerows(data)
def build(name):
 r=json.loads((COORD/(name+'-build.json')).read_text())
 assert sha(r['library'])==r['library_sha256'],name
 for filename,expected in r['source_files_sha256'].items():assert sha(Path(r['tree'])/filename)==expected,(name,filename)
 for b in r['binaries'].values():
  assert sha(b['binary'])==b['binary_sha256'],name
  assert sha(b['source'])==b['source_sha256'],(name,b['source'])
 return r
def cmdrun(folder,manifest,label,profile,binary,args,env=None,timeout=600):
 b=build(profile);exe=b['binaries'][binary]
 cmd=['taskset','-c','0-11',exe['binary'],*map(str,args)]
 rec=dict(label=label,profile=profile,command=cmd,started=time.time(),binary_sha256=exe['binary_sha256'],library_sha256=b['library_sha256'],source_head=b['head'],verified=False)
 print(label,flush=True)
 log=folder/(label+'.log')
 try:
  before=resource.getrusage(resource.RUSAGE_CHILDREN)
  with log.open('w') as f:
   p=subprocess.Popen(cmd,env=dict(os.environ,**(env or {})),stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
   try:p.wait(timeout=timeout)
   except BaseException:
    try:os.killpg(p.pid,signal.SIGKILL)
    except ProcessLookupError:pass
    p.wait()
    raise
  after=resource.getrusage(resource.RUSAGE_CHILDREN)
  rec.update(returncode=p.returncode,finished=time.time(),log_sha256=sha(log),process_cpu_s=(after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime),process_read_bytes=512*(after.ru_inblock-before.ru_inblock),process_write_bytes=512*(after.ru_oublock-before.ru_oublock))
 except BaseException as e:
  rec.update(exception=str(e),finished=time.time());manifest['runs'].append(rec);save(folder/'manifest.json',manifest);raise
 manifest['runs'].append(rec);save(folder/'manifest.json',manifest)
 assert p.returncode==0,rec
 assert 'FINDING: splinterdb_close' not in log.read_text(),rec
 return rec

def newmode(mode):
 folder=OUT/mode;folder.mkdir(parents=True,exist_ok=False)
 fs=subprocess.check_output(['findmnt','-n','-o','FSTYPE,SOURCE','-T',str(folder)],text=True).strip();assert fs.split()[0] not in ['tmpfs','ramfs'] and fs.split()[1].startswith('/dev/')
 manifest=dict(mode=mode,storage=fs,io='O_DIRECT',cpu_affinity='0-11',scope='Updated feature branches versus fresh main; not an isolation of merge cost',runner_sha256=sha(__file__),runs=[])
 save(folder/'manifest.json',manifest);return folder,manifest

def kvargs(db,csvpath,profile,regime,index,secs=6,prefix=False,create=False):
 return ['--db',db,'--create' if create else '--open','--n',200000 if regime=='resident' else 2000000,'--cache-mib',128 if regime=='resident' else 64,'--disk-gib',8,'--log',0,'--threads',1 if create else 8,'--mt-bg',0,'--normal-bg',0,'--memtable-mib',4,'--secs',secs,'--warmup',1,'--csv',csvpath,'--vtag',profile,'--stag',regime,'--otag',index,'--rep',index,'--seed',42,'--hang-secs',60,'--hang-dir',str(csvpath.parent)]
def verifykv(csvpath,index,count):
 rr=[r for r in rows(csvpath) if r['order']==str(index)]
 assert len(rr)==count,(index,len(rr),count)
 assert all(int(r['ops'])>0 and int(r['errors'])==0 and int(r['violations'])==0 for r in rr),rr
 assert all(int(r['ops'])==int(r['n']) for r in rr if r['phase']=='warm' or r['phase'].startswith('load:')),rr
 return rr

def fixtures():
 folder,m=newmode('fixtures');ff={}
 for regime in ['resident','disk']:
  for profile,pattern in [('main','ordinary'),('disk-format','ordinary'),('main','common'),('branch-prefix','common'),('main','random'),('branch-prefix','random')]:
   label=f'{regime}-{profile}-{pattern}';db=folder/(label+'.db');binary='kvbench' if pattern=='ordinary' else 'prefix_kvbench'
   rec=cmdrun(folder,m,label,profile,binary,kvargs(db,folder/(label+'.csv'),profile,regime,0,secs=1,create=True)+['load:random'],dict(PREFIX_PATTERN=pattern) if pattern!='ordinary' else None)
   verifykv(folder/(label+'.csv'),0,1)
   with db.open('rb') as f:os.fsync(f.fileno())
   rec.update(verified=True,verification_kind='load API errors and operation count; query phases later validate data',errors=0,violations=0,fixture=str(db),fixture_sha256=sha(db),allocated_bytes=db.stat().st_blocks*512,apparent_bytes=db.stat().st_size)
   ff[label]={k:rec[k] for k in ['fixture','fixture_sha256','allocated_bytes','apparent_bytes']};save(folder/'manifest.json',m);save(folder/'fixtures.json',ff)
 m['complete']=True;save(folder/'manifest.json',m)

def query_one(folder,m,index,profile,regime,phases,pattern='ordinary'):
 f=json.loads((OUT/'fixtures/fixtures.json').read_text());owner=profile if profile in ['disk-format','branch-prefix'] else 'main'
 fixture=f[f'{regime}-{owner}-{pattern}'];src=Path(fixture['fixture']);before=src.stat()
 db=folder/'scratch.db';assert not db.exists()
 subprocess.run(['cp','--reflink=auto','--sparse=always',str(src),str(db)],check=True)
 with db.open('rb') as image:os.fsync(image.fileno())
 label=f'{index:02d}-{profile}';csvpath=folder/'results.csv';binary='kvbench' if pattern=='ordinary' else 'prefix_kvbench'
 rec=cmdrun(folder,m,label,profile,binary,kvargs(db,csvpath,profile,regime,index,secs=6 if regime=='resident' else 5)+phases,dict(PREFIX_PATTERN=pattern) if pattern!='ordinary' else None)
 rr=verifykv(csvpath,index,len(phases))
 rec.update(verified=True,verification_kind='query key/value/range validation and successful close',errors=sum(int(r['errors']) for r in rr),violations=sum(int(r['violations']) for r in rr),fixture=fixture,fixture_copy_fsynced=True,db_allocated_bytes=db.stat().st_blocks*512)
 assert src.stat().st_mtime_ns==before.st_mtime_ns
 save(folder/'manifest.json',m);db.unlink()

def queries(regime):
 folder,m=newmode(regime)
 phases=['warm','point:uniform@1','point:uniform@8','range:uniform:1@1','range:uniform:1@8','range:uniform:10000@8'] if regime=='resident' else ['warm','point:uniform@8','range:uniform:1@8','range:uniform:10000@8']
 m.update(order=ORDER,phases=phases)
 for i,profile in enumerate(ORDER):query_one(folder,m,i,profile,regime,phases)
 m['complete']=True;save(folder/'manifest.json',m)

def prefix():
 for regime in ['resident','disk']:
  for pattern in ['common','random']:
   folder,m=newmode(f'prefix-{regime}-{pattern}')
   phases=['warm','point:uniform@8','range:uniform:1@8','range:uniform:10000@8']
   if regime=='resident':phases+=['ow:uniform@8']
   for i,profile in enumerate(['main','branch-prefix','branch-prefix','main']):query_one(folder,m,i,profile,regime,phases,pattern)
   m['complete']=True;save(folder/'manifest.json',m)

def insert():
 folder,m=newmode('insert');data=[]
 profiles=['disk-format','filtered-ranges','lazy-merge','async-durable-barrier','cache-invalid-addresses','directory-direct','directory-bucket','directory-bucket-simd','directory-bucket-atomic','log-disabled','log-default']
 for wal,versions in [(1,['main',*profiles,'main',*reversed(profiles),'main']),(0,['main','cache-invalid-addresses','directory-bucket-atomic','directory-bucket-atomic','cache-invalid-addresses','main'])]:
  for i,version in enumerate(versions):
   profile='log-group-limit' if version.startswith('log-') else version
   label=f'wal{wal}-{i:02d}-{version}';stem=folder/label;db=folder/'scratch.db';assert not db.exists()
   args=['--db',db,'--csv',str(stem)+'.csv','--summary',str(stem)+'.summary.csv','--records',100000000,'--seconds',5,'--report-ms',100,'--cache-mib',256,'--disk-gib',16,'--direct','--inserters',8,'--readers',0,'--memtable-bg',1,'--normal-bg',3,'--memtable-mib',8,'--key-size',16,'--value-size',16,'--min-free-gib',12,'--seed',42]
   if not wal:args+=['--no-log']
   if version.startswith('log-'):args+=['--log-group-size-pages',2**64-1 if version=='log-disabled' else 0]
   rec=cmdrun(folder,m,label,profile,'insert_benchmark',args,timeout=240)
   summary=rows(Path(str(stem)+'.summary.csv'));assert len(summary)==1;summary=summary[0];inserted=int(summary['inserted']);assert inserted>0
   log=(folder/(label+'.log')).read_text();error_match=re.search(r'load_end .* error=([0-9]+)(?: |\n)',log);assert error_match and int(error_match[1])==0
   close=re.search(r'close_s=([\d.]+) close_rc=0',log);assert close
   interval=rows(Path(str(stem)+'.csv'));cpu=sum(float(r['cpu_cores'])*float(r['interval_s']) for r in interval)
   data.append(dict(wal=wal,version=version,index=i,errors=int(error_match[1]),violations=0,verification_kind='API return, positive insert count, successful close; no readback',ops_s=float(summary['achieved_insert_ops_s']),inserted=inserted,close_s=float(close[1]),drain_ops_s=inserted/(float(summary['elapsed_s'])+float(close[1])),cpu_us_op=cpu*1e6/inserted,p99_us=1000*float(summary['insert_service_p99_ms']),max_ms=float(summary['insert_service_max_ms']),read_B_op=sum(float(r['read_mib_s'])*float(r['interval_s']) for r in interval)*2**20/inserted,write_B_op=sum(float(r['write_mib_s'])*float(r['interval_s']) for r in interval)*2**20/inserted))
   rec.update(verified=True,errors=int(error_match[1]),verification_kind='API return, positive insert count, successful close; no readback',allocated_bytes=db.stat().st_blocks*512);save(folder/'manifest.json',m);csvwrite(folder/'results.csv',data);db.unlink()
 m['complete']=True;save(folder/'manifest.json',m)

def hash_range():
 folder,m=newmode('hash');reference=None
 for i,profile in enumerate(['main','filtered-ranges','filtered-ranges','main']):
  label=f'{i}-{profile}';db=folder/'scratch.db';rec=cmdrun(folder,m,label,profile,'hash_range_benchmark',[db,16384,1000],timeout=900)
  lines=(folder/(label+'.log')).read_text().splitlines();start=next(j for j,x in enumerate(lines) if x.startswith('mode,keys_per_tenant,'));rr=list(csv.DictReader(lines[start:]));assert len(rr)==4
  assert [int(r['limit']) for r in rr]==[1,2,10,100] and all(int(r['queries'])==1000 and int(r['keys_per_tenant'])==16384 for r in rr),rr
  checks=[x['checksum'] for x in rr]
  if reference is None:reference=checks
  assert checks==reference
  rec.update(verified=True,rows=rr,allocated_bytes=db.stat().st_blocks*512);save(folder/'manifest.json',m);db.unlink()
 m['complete']=True;save(folder/'manifest.json',m)

def barrier():
 folder,m=newmode('barrier')
 for wal in [0,1]:
  for workers in [0,1]:
   order=[('main','sync'),('async-durable-barrier','sync'),('async-durable-barrier','async'),('async-durable-barrier','async'),('async-durable-barrier','sync'),('main','sync')]
   for i,(profile,mode) in enumerate(order):
    label=f'wal{wal}-workers{workers}-{i}-{profile}-{mode}';db=folder/'scratch.db';csvpath=folder/'results.csv'
    rec=cmdrun(folder,m,label,profile,'barrier_bench',[db,csvpath,label,mode,wal,workers,256,2048 if wal else 512],dict(BARRIER_DEPTH='1'))
    rr=rows(csvpath);assert rr[-1]['label']==label and int(rr[-1]['verified'])==1
    rec.update(verified=True,allocated_bytes=db.stat().st_blocks*512);save(folder/'manifest.json',m);db.unlink()
 m['complete']=True;save(folder/'manifest.json',m)

if __name__=='__main__':
 def interrupted(signum,frame):raise InterruptedError(f'benchmark interrupted by signal {signum}')
 for signum in [signal.SIGTERM,signal.SIGHUP]:signal.signal(signum,interrupted)
 p=argparse.ArgumentParser();p.add_argument('mode',choices=['fixtures','resident','disk','prefix','insert','hash','barrier']);a=p.parse_args()
 {'fixtures':fixtures,'resident':lambda:queries('resident'),'disk':lambda:queries('disk'),'prefix':prefix,'insert':insert,'hash':hash_range,'barrier':barrier}[a.mode]()
