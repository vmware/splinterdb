#!/usr/bin/env python3
"""Companion integration correctness checks; invoke through run_job.py."""
import hashlib, json, os, pathlib, re, subprocess, sys, time
TREE = pathlib.Path('/home/rob/projects/splinterdb/.cache/projects-20261011/worktrees/filtered-ranges-disk-format')
OUT = pathlib.Path(__file__).resolve().parent
phase, mode = sys.argv[1:3]
selected = sys.argv[3:]
base = TREE / 'build-filter-disk-integration' / mode / 'bin'
scratch = OUT / ('scratch-' + mode)
scratch.mkdir(exist_ok=True)
link = scratch / 'build-filter-disk-integration'
if not link.exists(): link.symlink_to(TREE / 'build-filter-disk-integration', target_is_directory=True)
records=[]
def run(name, command, cwd, timeout=900):
    if phase == 'test':
        cwd = cwd / name
        cwd.mkdir(exist_ok=True)
        local_link = cwd / 'build-filter-disk-integration'
        if not local_link.exists(): local_link.symlink_to(TREE / 'build-filter-disk-integration', target_is_directory=True)
    log = OUT / f'{mode}-{name}.log'
    record = dict(name=name, command=[str(x) for x in command], cwd=str(cwd), started=time.time())
    with log.open('w') as stream:
        try:
            p = subprocess.run(command,cwd=cwd,stdout=stream,stderr=subprocess.STDOUT,timeout=timeout)
            record['returncode'] = p.returncode
        except subprocess.TimeoutExpired:
            record['returncode'] = 124
    record['finished'] = time.time()
    text=log.read_text(errors='replace')
    record['log_sha256']=hashlib.sha256(log.read_bytes()).hexdigest()
    record['summaries']=re.findall(r'RESULTS: [^\r\n]+',text)
    if phase=='test':
        if name=='filter':
            record['passed'] = text.count('routing filter basic test: false positive rate') == 4 and 'six saved seeds, seven hash calls' in text
        else:
            counts=[tuple(map(int,m)) for m in re.findall(r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)',text)]
            record['passed']=bool(counts) and all(n==ok and n>0 and failed==skip==0 for n,ok,failed,skip in counts)
    records.append(record)
    suffix='-'.join(selected) if selected else phase
    (OUT / f'{mode}-{suffix}.json').write_text(json.dumps(records,indent=2)+'\n')
    print(name,record['returncode'],record['summaries'],flush=True)
    if record['returncode'] or (phase=='test' and not record['passed']):
        print(text[-12000:],flush=True)
        raise SystemExit(record['returncode'] or 1)
if phase=='build':
    targets=['libs',*(str(base/'unit'/name) for name in ['merge_test','splinterdb_quick_test','splinter_test','superblock_test','trunk_async_test']),str(base/'driver_test')]
    # make target names use the relative build root, while tests use absolute paths.
    targets=[str(pathlib.Path(t).relative_to(TREE)) if str(t).startswith(str(TREE)) else t for t in targets]
    run('build',['make','--jobserver-style=pipe','-j6','BUILD_ROOT=build-filter-disk-integration','BUILD_MODE='+mode]+targets,TREE)
else:
    commands=[
        ('filter',[base/'driver_test','filter_test','--db-location',scratch/'filter.db','--cache-capacity-mib','64','--db-capacity-gib','4']),
        ('mixed-seeds',[base/'unit/splinterdb_quick_test','test_filtered_range_persisted_mixed_seeds']),
        ('merge',[base/'unit/merge_test']),
        ('quick',[base/'unit/splinterdb_quick_test']),
        ('splinter',[base/'unit/splinter_test','--db-location',scratch/'core.db','--cache-capacity-mib','128','--db-capacity-gib','64']),
        ('superblock',[base/'unit/superblock_test']),
        ('trunk-async',[base/'unit/trunk_async_test']),
    ]
    for name,cmd in commands:
        if not selected or name in selected: run(name,cmd,scratch)
