import hashlib,json,re,subprocess,sys,time
from pathlib import Path
OUT=Path(__file__).resolve().parent
ROOT=OUT.parents[1]
TREE=ROOT/'worktrees/disk-format'
mode=sys.argv[1]
base=Path('build-pr-0fa3bd6c')/mode/'bin'
db=TREE/'.cache/pr-format-0fa3bd6c';db.mkdir(parents=True,exist_ok=True)
commands=[
 ('btree',[str(base/'unit/btree_test')]),
 ('superblock',[str(base/'unit/superblock_test')]),
 ('splinter',[str(base/'unit/splinter_test'),'--db-location',str(db/(mode+'-splinter.db')),'--cache-capacity-mib','128','--db-capacity-gib','64']),
 ('quick',[str(base/'unit/splinterdb_quick_test')]),
 ('trunk-async',[str(base/'unit/trunk_async_test')]),
 ('notification',[str(base/'unit/notification_test')]),
 ('notification-shmem',[str(base/'unit/notification_test'),'--use-shmem']),
 ('optimize',[str(base/'unit/splinterdb_optimize_test')]),
 ('optimize-shmem',[str(base/'unit/splinterdb_optimize_test'),'--use-shmem']),
 ('filter',[str(base/'driver_test'),'filter_test','--db-location',str(db/(mode+'-filter.db')),'--cache-capacity-mib','64','--db-capacity-gib','4']),
 ('log',[str(base/'driver_test'),'log_test','--db-location',str(db/(mode+'-log.db')),'--cache-capacity-mib','64','--db-capacity-gib','4','--num-inserts','10000']),
]
records=[]
for name,cmd in commands:
 print(mode,name,flush=True)
 log=OUT/f'{mode}-{name}.log';rec=dict(name=name,command=cmd,cwd=str(TREE),started=time.time());records.append(rec)
 with log.open('w') as f:r=subprocess.run(cmd,cwd=TREE,stdout=f,stderr=subprocess.STDOUT,timeout=900)
 text=log.read_text();rec.update(returncode=r.returncode,finished=time.time(),log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),summaries=[x for x in text.splitlines() if 'RESULTS:' in x])
 (OUT/f'validation-{mode}.json').write_text(json.dumps(records,indent=2)+'\n')
 if r.returncode:raise SystemExit(r.returncode)
 if name not in ['filter','log']:
  match=re.search(r'RESULTS: ([1-9][0-9]*) tests \(([0-9]+) ok, ([0-9]+) failed, ([0-9]+) skipped\)',text)
  assert match and int(match[1])==int(match[2]) and match[3]=='0',name
