"""A separate longer BAAB repeat of the disk-format resident point8 concern."""
import json, os, subprocess
from pathlib import Path
import run as common
folder,m=common.mode_start('repeat-resident-disk-point8')
order=['disk-format','main','main','disk-format'];phases=['warm','point:uniform@8']
m.update(order=order,phases=phases,seconds_per_phase=15,repeat_runner_sha256=common.sha(__file__),
         rationale='Reverse the broad-screen profile placement and lengthen the one point8 phase; retain broad samples separately.')
fixtures=json.loads((common.OUT/'fixtures/fixtures.json').read_text())
for i,profile in enumerate(order):
    fixture=fixtures['resident-'+profile];src=Path(fixture['fixture']);meta=src.stat()
    assert common.sha(src)==fixture['fixture_sha256']
    db=folder/'scratch.db';assert not db.exists()
    subprocess.run(['cp','--reflink=auto','--sparse=always',str(src),str(db)],check=True)
    with db.open('rb') as image:os.fsync(image.fileno())
    csvfile=folder/'results.csv'
    args=common.args(db,csvfile,profile,'resident',i)
    args[args.index('--secs')+1]=15
    rec=common.run(folder,m,f'{i:02d}-{profile}',profile,'kvbench',args+phases)
    rows=common.verify_csv(csvfile,i,phases,profile)
    assert all(int(r['rd_bytes'])==0 for r in rows if r['phase']!='warm')
    rec.update(verified=True,errors=0,violations=0,verification_kind='full warm scan and point key/value validation; successful close',
               fixture=fixture,fixture_copy_fsynced=True,allocated_bytes=db.stat().st_blocks*512)
    assert src.stat().st_mtime_ns==meta.st_mtime_ns and src.stat().st_size==meta.st_size
    common.save(folder/'manifest.json',m);db.unlink()
m['complete']=True;common.save(folder/'manifest.json',m)
