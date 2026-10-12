import json,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parent.parent
out=root/'coordination'
base=Path('build-layering/debug/bin')
commands=[('btree',[str(base/'unit/btree_test')]),('superblock',[str(base/'unit/superblock_test')]),('splinter',[str(base/'unit/splinter_test'),'--db-location','.cache/format-tests/layering-splinter.db','--cache-capacity-mib','128','--db-capacity-gib','4']),('quick',[str(base/'unit/splinterdb_quick_test')]),('trunk-async',[str(base/'unit/trunk_async_test')]),('filter',[str(base/'driver_test'),'filter_test','--db-location','.cache/format-tests/layering-filter.db','--cache-capacity-mib','64','--db-capacity-gib','4']),('log',[str(base/'driver_test'),'log_test','--db-location','.cache/format-tests/layering-log.db','--cache-capacity-mib','64','--db-capacity-gib','4','--num-inserts','10000'])]
records=[]
for name,cmd in commands:
 print(name,flush=True)
 record=dict(name=name,command=cmd,started=time.time());records.append(record)
 with (out/f'disk-format-layering-debug-{name}.log').open('w') as f:r=subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT)
 record.update(returncode=r.returncode,finished=time.time())
 (out/'disk-format-layering-tests.json').write_text(json.dumps(records,indent=2)+'\n')
 if r.returncode:raise SystemExit(r.returncode)
