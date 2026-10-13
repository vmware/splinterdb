"""Run via run_job build after profiles.json pins the final source heads."""
import hashlib, json, subprocess, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p, value): p.write_text(json.dumps(value, indent=2) + '\n')
profiles = json.loads((HERE/'profiles.json').read_text())
flags = ['cc', '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread', '-Werror',
         '-DXXH_STATIC_LINKING_ONLY', '-fPIC', '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
         '-O3', '-ffast-math', '-flto', '-march=native']
for name, config in profiles.items():
    if len(sys.argv)>1 and name not in sys.argv[1:]: continue
    tree = Path(config['tree']); head = config['head']
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=tree,text=True).strip()==head
    assert subprocess.run(['git','rev-parse','--verify','-q','MERGE_HEAD'],cwd=tree,stdout=subprocess.DEVNULL).returncode!=0
    subprocess.run(['git','diff','--quiet','HEAD','--','src','include'],cwd=tree,check=True)
    command=['make','--jobserver-style=pipe','-j4','BUILD_ROOT='+config['build_root'],'BUILD_MODE=release','libs']
    with (HERE/(name+'-libs.log')).open('w') as log:
        subprocess.run(command,cwd=tree,stdout=log,stderr=subprocess.STDOUT,check=True)
    lib=tree/config['build_root']/'release/lib/libsplinterdb.a'
    record=dict(tree=str(tree),head=head,library=str(lib),library_sha256=sha(lib),library_build_command=command,
                source_files_sha256={},binaries={})
    for source in subprocess.check_output(['git','ls-files','src','include'],cwd=tree,text=True).splitlines():
        record['source_files_sha256'][source]=sha(tree/source)
    for binary in ['kvbench']+(['hash_range_benchmark'] if name!='disk-format' else []):
        source=HERE/'sources'/(binary+'.c'); destination=HERE/'bin'/name/binary
        destination.parent.mkdir(parents=True,exist_ok=True)
        defines=['-DKVBENCH_CLOSE_HAS_FORCE'] if binary=='kvbench' else []
        if binary=='hash_range_benchmark' and name=='filtered-ranges': defines+=['-DPREDICATE_RANGE_FILTER_API']
        command=flags+defines+['-I',str(tree/'include'),'-I',str(tree/'src'),'-I',str(tree/'src/platform_linux'),
                              str(source),str(lib),'-laio','-lxxhash','-lm','-lpthread','-o',str(destination)]
        with (HERE/(name+'-'+binary+'-build.log')).open('w') as log:
            subprocess.run(command,stdout=log,stderr=subprocess.STDOUT,check=True)
        record['binaries'][binary]=dict(command=command,binary=str(destination),binary_sha256=sha(destination),
                                        source=str(source),source_sha256=sha(source))
    save(HERE/(name+'-build.json'),record)
    print(name,head,lib,record['library_sha256'],flush=True)
