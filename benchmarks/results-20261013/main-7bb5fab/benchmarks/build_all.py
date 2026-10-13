import hashlib,json,subprocess,sys
from pathlib import Path
ROOT=Path('/home/rob/projects/splinterdb')
CAMP=ROOT/'.cache/projects-20261011'
OUT=Path(__file__).resolve().parent
SOURCES=OUT/'sources'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
profiles={'main':(ROOT,'build-main-7bb5fab')}
for name in ['disk-format','filtered-ranges','log-group-limit','branch-prefix','lazy-merge','async-durable-barrier','cache-invalid-addresses']:
 profiles[name]=(CAMP/'worktrees'/name,'build-main-7bb5fab')
for opt in ['direct','bucket','bucket-simd','bucket-atomic']:
 profiles['directory-'+opt]=(CAMP/'worktrees/cache-directory',f'build-main-7bb5fab/{opt}')
flags=['cc','-D_GNU_SOURCE','-ggdb','-Wall','-pthread','-Werror','-DXXH_STATIC_LINKING_ONLY','-fPIC','-DSPLINTERDB_PLATFORM_DIR=platform_linux','-O3','-ffast-math','-flto','-march=native']
manifest={}
for name,(tree,buildroot) in profiles.items():
 if len(sys.argv)>1 and name not in sys.argv[1:]:continue
 head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=tree,text=True).strip()
 subprocess.run(['git','merge-base','--is-ancestor','7bb5fab',head],cwd=tree,check=True)
 lib=tree/buildroot/'release/lib/libsplinterdb.a'
 header=(tree/'include/splinterdb/splinterdb.h').read_text()
 record=dict(tree=str(tree),head=head,library=str(lib),library_sha256=sha(lib),binaries={},source_files_sha256={})
 for filename in subprocess.check_output(['git','ls-files','src','include'],cwd=tree,text=True).splitlines():
  if (tree/filename).is_file():record['source_files_sha256'][filename]=sha(tree/filename)
 bins=['kvbench','insert_benchmark']
 if name in ['main','branch-prefix']:bins+=['prefix_kvbench']
 if name in ['main','filtered-ranges']:bins+=['hash_range_benchmark']
 if name in ['main','async-durable-barrier']:bins+=['barrier_bench']
 for binaryname in bins:
  source=SOURCES/(binaryname+'.c');binary=OUT/'bin'/name/binaryname;binary.parent.mkdir(parents=True,exist_ok=True)
  macros=[]
  if binaryname in ['kvbench','prefix_kvbench']:macros+=['-DKVBENCH_CLOSE_HAS_FORCE']
  if binaryname=='prefix_kvbench':
   macros+=['-DKVBENCH_SPACE_MODE']
   if name=='branch-prefix':macros+=['-DKVBENCH_PREFIX_SUPPORT']
  if binaryname=='hash_range_benchmark' and name=='filtered-ranges':macros+=['-DHASH_RANGE_FILTER_API']
  if binaryname=='insert_benchmark' and name!='log-group-limit':macros+=['-DKVBENCH_NO_LOG_GROUP_LIMIT']
  if '#define SPLINTERDB_NOTIFICATION_BLOCKING' in header:macros+=['-DHAVE_SENTINEL_NOTIFICATION','-DHAVE_ASYNC_BARRIER']
  command=flags+macros+['-I',str(tree/'include'),'-I',str(tree/'src'),'-I',str(tree/'src/platform_linux'),str(source),str(lib),'-laio','-lxxhash','-lm','-lpthread','-o',str(binary)]
  print('build',name,binaryname,flush=True)
  subprocess.run(command,check=True)
  record['binaries'][binaryname]=dict(command=command,binary=str(binary),binary_sha256=sha(binary),source=str(source),source_sha256=sha(source))
 (OUT/(name+'-build.json')).write_text(json.dumps(record,indent=2)+'\n')
 manifest[name]=record
(OUT/'builds-last.json').write_text(json.dumps(manifest,indent=2)+'\n')
