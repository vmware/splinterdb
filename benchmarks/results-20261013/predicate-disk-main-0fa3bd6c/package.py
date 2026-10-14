"""Prepare reviewable evidence without databases, executables or libraries."""
import hashlib, json, shutil
from pathlib import Path
HERE=Path(__file__).resolve().parent
OUT=Path('/home/rob/projects/splinterdb/.cache/projects-20261011/benchmarks/filter-predicate-20261013')
status=json.loads((HERE/'status.json').read_text())
assert status['complete'] and status['errors']==status['violations']==0
package=HERE/'package';package.mkdir(exist_ok=False)
for p in HERE.iterdir():
    if p.is_file() and p.suffix in ('.py','.json','.txt','.log'):
        shutil.copy2(p,package/p.name)
shutil.copytree(HERE/'sources',package/'sources')
for mode in ['fixtures','resident','disk','feature','repeat-resident-disk-point8']:
    target=package/mode;target.mkdir()
    for p in (OUT/mode).iterdir():
        if p.is_file() and p.suffix in ('.csv','.json','.log','.txt'):
            shutil.copy2(p,target/p.name)
(package/'README.txt').write_text('Fresh filtered-predicate and disk-format screening against main 0fa3bd6c.\n'
 'OPEN PERFORMANCE CONCERN: disk-format resident point8 throughput fell 4.39% in the broad screen and 7.75% in the longer repeat, with non-overlapping sample ranges and zero disk reads.\n'
 'The cause is unresolved. Code review is appropriate; performance-neutrality and merge-ready claims are not supported. The broad and repeat samples remain separate.\n'
 'See report.txt for methodology, sample ranges and limits; summary.json for CPU/I/O and query counts.\n'
 'Each profile build JSON records source, library, executable hashes and compile commands.\n'
 'Each workload manifest records exact invocation, storage, verification and process resource usage.\n'
 'Sources are frozen measured bytes. Historical absolute paths identify the original workspace.\n'
 'Databases, libraries and executables are intentionally not embedded. Fixture creation logs and\n'
 'digests are retained; every copied fixture was checked and fsynced before use.\n')
(package/'.clang-format').write_text('# Preserve immutable measured source snapshots.\nDisableFormat: true\nSortIncludes: Never\n')
(package/'.gitattributes').write_text('*.log -whitespace\n*.txt -whitespace\n')
files={str(p.relative_to(package)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
       for p in sorted(package.rglob('*')) if p.is_file()}
(package/'inventory.json').write_text(json.dumps(files,indent=2)+'\n')
print(json.dumps({'path':str(package),'files':len(files),'bytes':sum(r['bytes'] for r in files.values())}))
