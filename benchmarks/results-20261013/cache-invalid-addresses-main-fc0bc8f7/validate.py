#!/usr/bin/env python3
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

name = sys.argv[1]
campaign = Path('/home/rob/projects/splinterdb/.cache/projects-20261011')
tree = campaign / 'worktrees' / name
out = Path(__file__).resolve().parent / name
out.mkdir(parents=True, exist_ok=True)
coordinator = campaign / 'coordination/run_job.py'
records = json.loads((out/'results.json').read_text()) if (out/'results.json').exists() else []
variants = ['direct', 'bucket-atomic', 'bucket', 'bucket-simd'] if name == 'cache-directory' else ['direct']

def run(label, kind, command, cwd, timeout):
    if any(r['name'] == label and r['command'] == command and r.get('returncode') == 0
           and (kind == 'build' or '/bin/unit/' not in command[0] or r.get('passed', 0) > 0)
           for r in records):
        print('ALREADY PASSED', name, label, flush=True)
        return
    record = dict(name=label, kind=kind, command=command, cwd=str(cwd), started=time.time())
    if kind == 'test':
        record['binary_sha256'] = hashlib.sha256(Path(command[0]).read_bytes()).hexdigest()
    logfile = out / (label + '.log')
    print('START', name, label, flush=True)
    with logfile.open('w') as log:
        try:
            result = subprocess.run(['python3', str(coordinator), '--name', name+'-fc0-'+label,
                                     kind, '--', *command], cwd=cwd, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=timeout)
            record['returncode'] = result.returncode
        except subprocess.TimeoutExpired:
            record['returncode'] = 'timeout'
            raise
        finally:
            record['finished'] = time.time()
            counts = re.findall(r'RESULTS: (\d+) tests \((\d+) ok, (\d+) failed, (\d+) skipped\)', logfile.read_text())
            if counts:
                record.update(zip(['tests', 'passed', 'failed', 'skipped'], map(int, counts[-1])))
            records.append(record)
            (out/'results.json').write_text(json.dumps(records, indent=2)+'\n')
    print('END', name, label, record.get('returncode'), record.get('passed'), flush=True)
    assert record['returncode'] == 0, record
    if kind == 'test' and '/bin/unit/' in command[0]:
        assert record.get('passed', 0) > 0 and record.get('failed', -1) == 0, record

for variant in variants:
    root = 'build-main-fc0bc8f7' + ('/'+variant if name=='cache-directory' else '')
    for mode in ['release', 'debug']:
        base = tree/root/mode
        targets = ['libs', 'unit/writeback_set_test', 'unit/splinterdb_quick_test', 'unit/splinter_test',
                   root+'/'+mode+'/bin/driver_test']
        if name == 'cache-directory' and variant != 'direct':
            targets += [root+'/'+mode+'/bin/unit/cache_directory_test']
        args = ['make', 'BUILD_ROOT='+root, 'BUILD_MODE='+mode, '-j4', *targets]
        if name == 'cache-directory': args += ['CACHE_DIRECTORY='+variant]
        run(variant+'-'+mode+'-build', 'build', args, tree, 1200)
        cases = [('writeback', [str(base/'bin/unit/writeback_set_test')]),
                 ('quick', [str(base/'bin/unit/splinterdb_quick_test')]),
                 ('core', [str(base/'bin/unit/splinter_test'), 'test_cache_read_errors_reach_core_operations'])]
        if name == 'cache-directory' and variant != 'direct':
            cases += [('directory', [str(base/'bin/unit/cache_directory_test')])]
        if mode == 'debug':
            cases += [('admission', [str(base/'bin/driver_test'), 'cache_test',
                                    '--directory-race' if name=='cache-directory' else '--prefetch-admission',
                                    '--cache-capacity-mib', '64', '--db-capacity-gib', '2'])]
        for label, command in cases:
            label = variant+'-'+mode+'-'+label
            scratch = out/('scratch-'+label)
            scratch.mkdir(exist_ok=True)
            link = scratch/'build-main-fc0bc8f7'
            if not link.exists(): link.symlink_to(tree/'build-main-fc0bc8f7', target_is_directory=True)
            run(label, 'test', command, scratch, 600)
