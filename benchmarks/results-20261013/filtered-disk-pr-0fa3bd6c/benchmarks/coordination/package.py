"""Archive text evidence after timing completes; large artifacts keep hashes/paths."""
import hashlib,json,shutil,subprocess
from pathlib import Path
ROOT=Path('/home/rob/projects/splinterdb');HERE=Path(__file__).resolve().parent
BASE=ROOT/'.cache/projects-20261011/benchmarks/filtered-disk-pr-20261013'
DEST=ROOT/'.cache/projects-20261011/worktrees/filtered-ranges/benchmarks/results-20261013/filtered-disk-pr-0fa3bd6c/benchmarks'
MODES=['smoke','resident','disk','repeat-disk-range1','sampled-disk-range1','repeat-disk-range10000','feature-v2']
def sha(p):
 h=hashlib.sha256()
 with Path(p).open('rb') as f:
  for c in iter(lambda:f.read(1048576),b''):h.update(c)
 return h.hexdigest()
def save(p,x):p.write_text(json.dumps(x,indent=2)+'\n')
for mode in MODES:
 m=json.loads((BASE/mode/'manifest.json').read_text());assert m['complete'] and all(r['verified'] for r in m['runs'])
subprocess.run(['python3',str(HERE/'report.py')],stdout=subprocess.DEVNULL,check=True)
inputs={str(p.relative_to(HERE)):sha(p) for p in [*HERE.glob('*.py'),*HERE.glob('sources/*.c')]}
save(HERE/'runner-source-sha256.json',inputs)
jobnames={'filtered-disk-harness-links','filtered-disk-controlled-smoke','filtered-disk-controlled-resident','filtered-disk-controlled-disk','filtered-disk-kernel-counter-probe','filtered-disk-range1-repeat','filtered-disk-range1-sampled','filtered-disk-profile-reports','filtered-disk-predicate-feature','filtered-disk-predicate-feature-verified','filtered-disk-long-range-repeat'}
jobs=[json.loads(line) for line in (HERE.parent.parent/'jobs.jsonl').read_text().splitlines() if line]
save(HERE/'benchmark-jobs.json',[r for r in jobs if r['name'] in jobnames])
DEST.mkdir(parents=True,exist_ok=True)
(DEST/'.clang-format').write_text('DisableFormat: true\nSortIncludes: Never\n')
(DEST/'.gitattributes').write_text('*.log -whitespace\n*.txt -whitespace\n*.patch -whitespace\n*.diff -whitespace\n')
def copy(source,destination):
 destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,destination)
for p in HERE.rglob('*'):
 if not p.is_file() or '__pycache__' in p.parts:continue
 if p.suffix in ['.py','.c','.json','.txt','.csv','.log']:
  copy(p,DEST/'coordination'/p.relative_to(HERE))
for mode in MODES+['feature']:
 for p in (BASE/mode).rglob('*'):
  if p.is_file() and p.suffix in ['.py','.json','.csv','.log','.txt']:
   copy(p,DEST/'measurements'/p.relative_to(BASE))
external={}
def artifact(p,kind,expected=None):
 p=Path(p);digest=sha(p)
 if expected:assert digest==expected,(str(p),digest,expected)
 external[str(p)]=dict(kind=kind,sha256=digest,size=p.stat().st_size)
for name in ['main','disk-format','combined']:
 b=json.loads((HERE/(name+'-build.json')).read_text());tree=Path(b['tree'])
 assert all(sha(tree/f)==v for f,v in b['source_files_sha256'].items())
 artifact(b['library'],'release static library',b['library_sha256'])
 for binary in b['binaries'].values():artifact(binary['binary'],'benchmark executable',binary['binary_sha256']);assert sha(binary['source'])==binary['source_sha256']
for mode in MODES:
 m=json.loads((BASE/mode/'manifest.json').read_text());assert sha(BASE/mode/'runner.py')==m['runner_sha256']
 for r in m['runs']:
  if 'fixture' in r:
   f=r['fixture']
   if f['fixture'] not in external:artifact(f['fixture'],'immutable original fixture',f['fixture_sha256'])
  if 'perf_data_sha256' in r:artifact(BASE/mode/r['label']/'perf.data','gated kernel/user samples',r['perf_data_sha256'])
save(DEST/'external-artifacts.json',external)
for name in ['README.txt','report.txt','summary.json']:copy(HERE/name,DEST/name)
files={str(p.relative_to(DEST)):dict(sha256=sha(p),size=p.stat().st_size) for p in DEST.rglob('*') if p.is_file() and p.name!='inventory.json'}
save(DEST/'inventory.json',files)
print(DEST,len(files),'files',sum(v['size'] for v in files.values()),'bytes',flush=True)
