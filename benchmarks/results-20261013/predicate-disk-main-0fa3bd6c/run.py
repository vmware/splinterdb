"""Run each mode via run_job benchmark, only after root explicitly clears timing."""
import argparse, csv, hashlib, json, os, re, resource, signal, subprocess, time
from pathlib import Path
ROOT=Path('/home/rob/projects/splinterdb'); HERE=Path(__file__).resolve().parent
OUT=ROOT/'.cache/projects-20261011/benchmarks/filter-predicate-20261013'
ORDER=['main','filtered-ranges','disk-format','disk-format','filtered-ranges','main']

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
    return h.hexdigest()
def save(p,x): p.write_text(json.dumps(x,indent=2)+'\n')
def csvread(p):
    with p.open() as f:return list(csv.DictReader(f))
def build(name):
    r=json.loads((HERE/(name+'-build.json')).read_text())
    ready=json.loads((HERE/'timing-ready.json').read_text())
    assert ready['heads'][name]==r['head']
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=r['tree'],text=True).strip()==r['head']
    assert sha(r['library'])==r['library_sha256']
    for path,expected in r['source_files_sha256'].items(): assert sha(Path(r['tree'])/path)==expected,(name,path)
    for b in r['binaries'].values():
        assert sha(b['binary'])==b['binary_sha256'] and sha(b['source'])==b['source_sha256']
    return r

def mode_start(name):
    folder=OUT/name; folder.mkdir(parents=True,exist_ok=False)
    fs=subprocess.check_output(['findmnt','-n','-o','FSTYPE,SOURCE','-T',str(folder)],text=True).strip()
    assert fs.split()[0] not in ('tmpfs','ramfs') and fs.split()[1].startswith('/dev/')
    m=dict(mode=name,storage=fs,io='O_DIRECT',cpu_affinity='0-11',runner_sha256=sha(__file__),runs=[],complete=False)
    save(folder/'manifest.json',m); return folder,m

def run(folder,m,label,profile,binary,args,environment=None,timeout=600):
    b=build(profile); exe=b['binaries'][binary]
    command=['taskset','-c','0-11',exe['binary'],*map(str,args)]
    rec=dict(label=label,profile=profile,command=command,environment=environment or {},started=time.time(),
             source_head=b['head'],binary_sha256=exe['binary_sha256'],library_sha256=b['library_sha256'],verified=False)
    m['runs'].append(rec);save(folder/'manifest.json',m); print(label,flush=True)
    log=folder/(label+'.log'); before=resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        with log.open('w') as f:
            p=subprocess.Popen(command,stdout=f,stderr=subprocess.STDOUT,env=dict(os.environ,**(environment or {})),start_new_session=True)
            try: p.wait(timeout=timeout)
            except BaseException:
                try: os.killpg(p.pid,signal.SIGKILL)
                except ProcessLookupError: pass
                p.wait(); raise
        after=resource.getrusage(resource.RUSAGE_CHILDREN)
        rec.update(returncode=p.returncode,finished=time.time(),log_sha256=sha(log),
                   process_cpu_s=after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime,
                   process_read_bytes=512*(after.ru_inblock-before.ru_inblock),process_write_bytes=512*(after.ru_oublock-before.ru_oublock))
        assert p.returncode==0,rec
        assert 'FINDING: splinterdb_close' not in log.read_text(),rec
    except BaseException as error:
        rec.update(exception=str(error),finished=time.time());save(folder/'manifest.json',m);raise
    save(folder/'manifest.json',m);return rec

def args(db,csvfile,profile,regime,index,create=False):
    return ['--db',db,'--create' if create else '--open','--n',200000 if regime=='resident' else 2000000,
            '--cache-mib',128 if regime=='resident' else 64,'--disk-gib',8,'--log',0,
            '--threads',1 if create else 8,'--mt-bg',0,'--normal-bg',0,'--memtable-mib',4,
            '--secs',8 if regime=='resident' else 6,'--warmup',1,'--csv',csvfile,'--vtag',profile,
            '--stag',regime,'--otag',index,'--rep',index,'--seed',42,'--hang-secs',60,'--hang-dir',str(csvfile.parent)]

def verify_csv(path,index,phases,profile):
    rows=[r for r in csvread(path) if r['order']==str(index)]
    assert [r['phase'] for r in rows]==phases and all(r['version']==profile for r in rows)
    assert all(int(r['ops'])>0 and int(r['errors'])==0 and int(r['violations'])==0 for r in rows)
    assert all(int(r['ops'])==int(r['n']) for r in rows if r['phase']=='warm' or r['phase'].startswith('load:'))
    return rows

def fixtures():
    folder,m=mode_start('fixtures'); fixtures={}
    for regime in ['resident','disk']:
        for profile in ['main','disk-format']:
            label=regime+'-'+profile; db=folder/(label+'.db'); csvfile=folder/(label+'.csv')
            rec=run(folder,m,label,profile,'kvbench',args(db,csvfile,profile,regime,0,True)+['load:random'])
            rr=verify_csv(csvfile,0,['load:random'],profile)
            with db.open('rb') as image:os.fsync(image.fileno())
            rec.update(verified=True,verification_kind='load count/API errors/close; warm scans later validate every key/value',
                       errors=0,violations=0,fixture=str(db),fixture_sha256=sha(db),fixture_fsynced=True,
                       allocated_bytes=db.stat().st_blocks*512,apparent_bytes=db.stat().st_size)
            fixtures[label]={k:rec[k] for k in ['fixture','fixture_sha256','allocated_bytes','apparent_bytes']}
            save(folder/'manifest.json',m);save(folder/'fixtures.json',fixtures)
    m['complete']=True;save(folder/'manifest.json',m)

def queries(regime):
    folder,m=mode_start(regime)
    phases=['warm','point:uniform@1','point:uniform@8','range:uniform:1@8','range:uniform:10000@8'] if regime=='resident' else ['warm','point:uniform@8','range:uniform:1@8','range:uniform:10000@8']
    m.update(order=ORDER,phases=phases)
    fixtures=json.loads((OUT/'fixtures/fixtures.json').read_text())
    fixture_manifest=json.loads((OUT/'fixtures/manifest.json').read_text())
    assert fixture_manifest['complete'] and all(r['verified'] for r in fixture_manifest['runs'])
    for i,profile in enumerate(ORDER):
        fixture=fixtures[regime+'-'+('disk-format' if profile=='disk-format' else 'main')]
        src=Path(fixture['fixture']); metadata=src.stat(); db=folder/'scratch.db'; assert not db.exists()
        assert sha(src)==fixture['fixture_sha256'],fixture['fixture']
        subprocess.run(['cp','--reflink=auto','--sparse=always',str(src),str(db)],check=True)
        with db.open('rb') as image:os.fsync(image.fileno())
        csvfile=folder/'results.csv'; label=f'{i:02d}-{profile}'
        rec=run(folder,m,label,profile,'kvbench',args(db,csvfile,profile,regime,i)+phases)
        rr=verify_csv(csvfile,i,phases,profile)
        if regime=='resident':assert all(int(r['rd_bytes'])==0 for r in rr if r['phase']!='warm'),rr
        rec.update(verified=True,verification_kind='full warm scan and timed key/value/range checks; successful close',
                   errors=sum(int(r['errors']) for r in rr),violations=sum(int(r['violations']) for r in rr),fixture=fixture,
                   fixture_copy_fsynced=True,allocated_bytes=db.stat().st_blocks*512)
        assert src.stat().st_mtime_ns==metadata.st_mtime_ns and src.stat().st_size==metadata.st_size
        save(folder/'manifest.json',m); db.unlink()
    m['complete']=True;save(folder/'manifest.json',m)

def expected_checksum(rows,queries,limit):
    seed=42;checksum=0
    for q in range(queries):
        seed=(seed*6364136223846793005+1442695040888963407)&((1<<64)-1)
        start=(seed>>32)%(rows-limit)
        checksum+=limit*(start+q%32)+limit*(limit-1)//2
    return checksum

def feature():
    folder,m=mode_start('feature'); count=10000; n=16384
    cases=[('main','application'),('filtered-ranges','predicate'),('filtered-ranges','pruned'),
           ('filtered-ranges','pruned'),('filtered-ranges','predicate'),('main','application')]
    m.update(queries_per_limit=count,rows_per_tenant=n,warmup_queries_per_limit=500,order=cases)
    for collisions in [0,1]:
        for i,(profile,mode) in enumerate(cases):
            label=f'collisions{collisions}-{i:02d}-{profile}-{mode}';db=folder/'scratch.db';assert not db.exists()
            rec=run(folder,m,label,profile,'hash_range_benchmark',[db,n,count],
                    {'FILTER_MODE':mode,'FILTER_HASH_COLLISIONS':str(collisions)},timeout=900)
            lines=(folder/(label+'.log')).read_text().splitlines()
            assert sum(line.startswith('loaded=524288 ') for line in lines)==1
            header=next(i for i,line in enumerate(lines) if line.startswith('mode,collisions,'))
            rr=list(csv.DictReader(lines[header:]));assert len(rr)==4
            assert [int(r['limit']) for r in rr]==[1,2,10,100]
            for r in rr:
                limit=int(r['limit'])
                assert r['mode']==mode and int(r['collisions'])==collisions and int(r['queries'])==count and int(r['keys_per_tenant'])==n
                assert int(r['checksum'])==expected_checksum(n,count,limit)
                assert int(r['warmup_queries'])==500 and float(r['ns_per_query'])>0 and float(r['cpu_s'])>0
                if mode!='application':assert int(r['visited'])==limit*count
                else:assert int(r['predicate_calls'])==int(r['visited'])
            rec.update(verified=True,verification_kind='per-query expected key/value/count checks and independently computed checksums',
                       rows=rr,errors=0,violations=0,allocated_bytes=db.stat().st_blocks*512)
            save(folder/'manifest.json',m);db.unlink()
    m['complete']=True;save(folder/'manifest.json',m)

if __name__=='__main__':
    def interrupted(signum,frame):raise InterruptedError(f'signal {signum}')
    for s in [signal.SIGTERM,signal.SIGHUP]:signal.signal(s,interrupted)
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['fixtures','resident','disk','feature']);a=p.parse_args()
    {'fixtures':fixtures,'resident':lambda:queries('resident'),'disk':lambda:queries('disk'),'feature':feature}[a.mode]()
