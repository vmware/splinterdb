"""Link frozen benchmark sources after root clears the build stage.
Usage: run_job build -- python3 build.py COMBINED_TREE COMBINED_LIBRARY
The original main/disk library manifests are checked before linking.
"""
import hashlib,json,subprocess,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[3]
OLD=HERE.parent.parent/'filter-predicate-20261013/benchmarks'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,x):p.write_text(json.dumps(x,indent=2)+'\n')
assert len(sys.argv)==3
flags=['cc','-D_GNU_SOURCE','-ggdb','-Wall','-pthread','-Werror','-DXXH_STATIC_LINKING_ONLY','-fPIC','-DSPLINTERDB_PLATFORM_DIR=platform_linux','-O3','-ffast-math','-flto','-march=native']
for name in ['main','disk-format','combined']:
 if name=='combined':
  tree=Path(sys.argv[1]).resolve();lib=Path(sys.argv[2]).resolve()
  assert not subprocess.check_output(['git','diff','HEAD','--','src','include'],cwd=tree)
  files={f:sha(tree/f) for f in subprocess.check_output(['git','ls-files','src','include'],cwd=tree,text=True).splitlines()}
  sourcehead=subprocess.check_output(['git','rev-parse','HEAD'],cwd=tree,text=True).strip()
 else:
  old=json.loads((OLD/(name+'-build.json')).read_text());tree=Path(old['tree']);lib=Path(old['library']);files=old['source_files_sha256'];sourcehead=old['head']
  assert sha(lib)==old['library_sha256']
  assert all(sha(tree/f)==v for f,v in files.items())
 rec=dict(name=name,tree=str(tree),head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=tree,text=True).strip(),compiled_library_head=sourcehead,library=str(lib),library_sha256=sha(lib),source_files_sha256=files,binaries={})
 for namebin in ['kvbench_controlled']+(['hash_range_benchmark'] if name=='combined' else []):
  source=HERE/'sources'/(namebin+'.c');dst=HERE/'bin'/name/namebin;dst.parent.mkdir(parents=True,exist_ok=True)
  defines=['-DKVBENCH_CLOSE_HAS_FORCE'] if namebin=='kvbench_controlled' else ['-DPREDICATE_RANGE_FILTER_API']
  cmd=flags+defines+['-I',str(tree/'include'),'-I',str(tree/'src'),'-I',str(tree/'src/platform_linux'),str(source),str(lib),'-laio','-lxxhash','-lm','-lpthread','-o',str(dst)]
  with (dst.parent/(namebin+'-build.log')).open('w') as log:subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=True)
  rec['binaries'][namebin]=dict(command=cmd,binary=str(dst),binary_sha256=sha(dst),source=str(source),source_sha256=sha(source))
 assert all(sha(tree/f)==v for f,v in files.items())
 save(HERE/(name+'-build.json'),rec);print(name,rec['head'],sha(lib),flush=True)
