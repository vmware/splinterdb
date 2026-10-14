from pathlib import Path
import hashlib,json,subprocess,shutil
out=Path(__file__).resolve().parent
camp=out.parent.parent
wt=camp/'worktrees/log-group-limit'
old=out.parent/'main-fc0bc8f7'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
main=json.loads((old/'main-build.json').read_text())
main['binaries']={'insert_benchmark':main['binaries']['insert_benchmark']}
assert sha(main['library'])==main['library_sha256']
assert sha(main['binaries']['insert_benchmark']['binary'])==main['binaries']['insert_benchmark']['binary_sha256']
assert sha(out/'insert_benchmark.c')==main['binaries']['insert_benchmark']['source_sha256']
(out/'main-build.json').write_text(json.dumps(main,indent=2)+'\n')
lib=wt/'build-main-fc0bc8f7/release/lib/libsplinterdb.a'
exe=out/'insert_benchmark'
source=out/'insert_benchmark.c'
cmd=['gcc','-D_GNU_SOURCE','-ggdb','-Wall','-pthread','-Werror','-DXXH_STATIC_LINKING_ONLY','-fPIC','-DSPLINTERDB_PLATFORM_DIR=platform_linux','-O3','-ffast-math','-flto','-march=native','-I',str(wt/'include'),'-I',str(wt/'src'),'-I',str(wt/'src/platform_linux'),str(source),str(lib),'-laio','-lxxhash','-lm','-lpthread','-o',str(exe)]
subprocess.run(cmd,check=True)
record=dict(profile='candidate',tree=str(wt),head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=wt,text=True).strip(),patch='default1024.patch',library=str(lib),library_sha256=sha(lib),source_files_sha256={},binaries={'insert_benchmark':dict(command=cmd,binary=str(exe),binary_sha256=sha(exe),source=str(source),source_sha256=sha(source))})
record['library_build_configs']={str(p):{'sha256':sha(p),'text':p.read_text()} for p in sorted(lib.parent.parent.glob('build-config.*'))}
for sub in ['src','include']:
 for p in sorted((wt/sub).rglob('*')):
  if p.is_file():record['source_files_sha256'][str(p.relative_to(wt))]=sha(p)
(out/'candidate-build.json').write_text(json.dumps(record,indent=2)+'\n')
(out/'default1024.patch').write_bytes(subprocess.check_output(['git','diff','HEAD','--','src','include','tests','benchmarks/insert_benchmark.c'],cwd=wt))
print('Matched-source benchmark build complete')
