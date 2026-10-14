import hashlib,json,re,subprocess,sys,time
from pathlib import Path
out=Path(__file__).resolve().parent
mode=sys.argv[1]
base=Path('build-main-7bb5fab')/mode/'bin'
db=Path('.cache/format-tests/main-7bb5fab')
commands=[('btree',[str(base/'unit/btree_test')]),('superblock',[str(base/'unit/superblock_test')]),('splinter',[str(base/'unit/splinter_test'),'--db-location',str(db/(mode+'-splinter.db')),'--cache-capacity-mib','128','--db-capacity-gib','64']),('quick',[str(base/'unit/splinterdb_quick_test')]),('trunk-async',[str(base/'unit/trunk_async_test')]),('filter',[str(base/'driver_test'),'filter_test','--db-location',str(db/(mode+'-filter.db')),'--cache-capacity-mib','64','--db-capacity-gib','4']),('log',[str(base/'driver_test'),'log_test','--db-location',str(db/(mode+'-log.db')),'--cache-capacity-mib','64','--db-capacity-gib','4','--num-inserts','10000'])]
records=[]
for name,cmd in commands:
 print(mode,name,flush=True)
 log=out/f'{mode}-{name}.log'
 record=dict(name=name,command=cmd,started=time.time());records.append(record)
 with log.open('w') as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
 record.update(returncode=r.returncode,finished=time.time(),log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())
 text=log.read_text()
 record['summaries']=[line for line in text.splitlines() if 'RESULTS:' in line]
 (out/f'validation-{mode}.json').write_text(json.dumps(records,indent=2)+'\n')
 if r.returncode:raise SystemExit(r.returncode)
 if name not in ['filter','log'] and not re.search(r'RESULTS: [1-9][0-9]* tests',text):raise SystemExit('No tests matched: '+name)
