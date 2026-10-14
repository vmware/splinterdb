#!/usr/bin/env python3
"""Public API compile and active-source format checks under run_job build lock."""
import hashlib
import json
from pathlib import Path
import subprocess
import time

OUT = Path(__file__).resolve().parent
TREE = OUT.parents[1] / 'worktrees/filtered-ranges'
records = []

def run(name, command):
    path = OUT / ('validation-' + name + '.log')
    record = dict(name=name, command=list(map(str, command)), cwd=str(TREE), started=time.time())
    with path.open('w') as log:
        result = subprocess.run(command, cwd=TREE, stdout=log, stderr=subprocess.STDOUT)
    record.update(returncode=result.returncode, finished=time.time(),
                  log_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    records.append(record)
    (OUT / 'validation-static.json').write_text(json.dumps(records, indent=2) + '\n')
    print(name, result.returncode, flush=True)
    if result.returncode:
        print(path.read_text(), flush=True)
        raise SystemExit(result.returncode)

for compiler, language, standard in [('gcc', 'c', 'c11'), ('g++', 'c++', 'c++17')]:
    run('public-header-' + language,
        [compiler, '-x', language, '-std=' + standard, '-Wall', '-Wextra', '-Werror',
         '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
         '-I', TREE / 'include', '-c', OUT / 'validation-api.c',
         '-o', OUT / ('validation-api-' + language + '.o')])
paths = subprocess.check_output(['git', 'ls-files', 'src', 'include', 'tests', 'tools', 'examples'],
                                cwd=TREE, text=True).splitlines()
run('format', ['clang-format-19', '--Werror', '--dry-run'] +
    [str(TREE / name) for name in paths if name.endswith(('.c', '.h'))])
run('pr-diff', ['git', 'diff', '--check', 'robj/disk-format...HEAD'])
run('working-diff', ['git', 'diff', '--check'])
