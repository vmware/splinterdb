#!/usr/bin/env python3
import hashlib, json, os, pathlib, re, subprocess, sys, time
root = pathlib.Path('/home/rob/projects/splinterdb')
wt = root / '.cache/projects-20261011/worktrees/filtered-ranges'
output = pathlib.Path(__file__).resolve().parent
phase, mode = sys.argv[1:]
build_mode = 'debug' if mode == 'debug-asan' else mode
binaries = wt / 'build-filter-predicate' / mode / 'bin'
scratch = output / ('scratch-' + mode)
scratch.mkdir(exist_ok=True)
link=scratch/'build-filter-predicate'
if not link.exists(): link.symlink_to(wt/'build-filter-predicate',target_is_directory=True)
records=[]
def run(name, cmd, cwd, timeout=240):
    path = output / (mode + '-' + name + '.log')
    record = dict(name=name,command=[str(x) for x in cmd],cwd=str(cwd),started=time.time())
    env = dict(os.environ)
    if mode == 'debug-asan':
        env['ASAN_OPTIONS']='detect_leaks=0:abort_on_error=1'
        record['ASAN_OPTIONS']=env['ASAN_OPTIONS']
    with path.open('w') as log:
        try:
            p=subprocess.run(cmd,cwd=cwd,stdout=log,stderr=subprocess.STDOUT,env=env,timeout=timeout)
            record['returncode']=p.returncode
        except subprocess.TimeoutExpired:
            record['returncode']=124
    record['finished']=time.time()
    text=path.read_text(errors='replace')
    record['log_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    record['summaries']=re.findall(r'RESULTS: [^\r\n]+',text)
    if phase=='test' and name!='filter':
        counts=[tuple(map(int,m)) for m in re.findall(r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)',text)]
        record['nonzero_tests_passed']=bool(counts) and all(n>0 and ok>0 and failed==0 for n,ok,failed,skip in counts)
    elif phase=='test':
        record['completed_filter_cases']=text.count('routing filter basic test: false positive rate')
        record['nonzero_tests_passed']=record['completed_filter_cases'] > 0
    records.append(record)
    (output/(mode+'-'+phase+'.json')).write_text(json.dumps(records,indent=2)+'\n')
    print(name,record['returncode'],record['summaries'],flush=True)
    if record['returncode'] or (phase=='test' and not record['nonzero_tests_passed']):
        print(text[-10000:],flush=True)
        raise SystemExit(record['returncode'] or 1)
if phase=='build':
    targets=[f'build-filter-predicate/{mode}/bin/unit/merge_test',f'build-filter-predicate/{mode}/bin/unit/splinterdb_quick_test']
    if mode!='debug-asan':
        targets += [f'build-filter-predicate/{mode}/bin/unit/splinter_test',f'build-filter-predicate/{mode}/bin/driver_test']
    cmd=['make','--jobserver-style=pipe','-j4','BUILD_ROOT=build-filter-predicate','BUILD_MODE='+build_mode]
    if mode=='debug-asan': cmd+=['BUILD_ASAN=1']
    run('build',cmd+targets,wt,360)
else:
    run('merge',[binaries/'unit/merge_test'],scratch)
    quick=[binaries/'unit/splinterdb_quick_test']
    if mode=='debug-asan': quick+=['test_filtered_range']
    run('quick',quick,scratch)
    if mode!='debug-asan':
        core=[binaries/'unit/splinter_test','--db-location',scratch/'core.db','--cache-capacity-mib','128','--db-capacity-gib','64']
        run('cache-core',core+['test_cache_read_errors_'],scratch)
        run('blob-core',core+['test_blob_'],scratch)
        run('filter',[binaries/'driver_test','filter_test','--db-location',scratch/'filter.db','--cache-capacity-mib','64','--db-capacity-gib','4'],scratch)
