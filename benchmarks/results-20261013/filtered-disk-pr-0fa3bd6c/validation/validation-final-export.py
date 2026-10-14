#!/usr/bin/env python3
"""Check the staged PR exactly as CI will see it, including archived C files.

Invoke only after final documentation/evidence is staged, under run_job.py test.
The export comes from the index; untracked build outputs cannot enter it.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import time

OUT = Path(__file__).resolve().parent
TREE = OUT.parents[1] / 'worktrees/filtered-ranges'
COMPANION = '361838074656ddf04ad37c0452f4373da9211269'
RUNTIME = ['src', 'include', 'tests', 'Makefile', '.github']
started = time.time()
export = OUT / ('validation-final-export-' + str(time.time_ns()))
export.mkdir()
record = {'started': started, 'tree': str(TREE), 'export': str(export),
          'source': 'staged index', 'reference_combination': COMPANION}

def git(*args):
    return subprocess.check_output(['git', '-C', str(TREE), *args])

def run(name, command, cwd=TREE):
    path = OUT / ('validation-final-' + name + '.log')
    with path.open('w') as log:
        result = subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
    record[name] = {'command': list(map(str, command)), 'cwd': str(cwd),
                    'returncode': result.returncode,
                    'log_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    (OUT / 'validation-final-export.json').write_text(json.dumps(record, indent=2) + '\n')
    if result.returncode:
        print(path.read_text(), flush=True)
        raise SystemExit(result.returncode)

# All intended edits must be staged. New untracked build directories are fine.
assert not git('diff', '--name-only'), 'Tracked edits remain unstaged'
record['head'] = git('rev-parse', 'HEAD').decode().strip()
index = git('ls-files', '--stage', '-z')
record['index_listing_sha256'] = hashlib.sha256(index).hexdigest()
run('export', ['git', 'checkout-index', '--all', '--prefix=' + str(export) + '/'])
run('format', ['./format-check.sh'], cwd=export)
run('pr-diff', ['git', 'diff', '--cached', '--check', 'robj/disk-format'])
run('main-diff', ['git', 'diff', '--cached', '--check', 'origin/main'])
run('working-diff', ['git', 'diff', '--check'])
assert not git('diff', '--cached', COMPANION, '--', *RUNTIME), 'Runtime source changed'
assert not git('diff', '--', *RUNTIME), 'Working runtime source changed'
record['runtime_source_identical'] = True
record['archive_format_guards'] = {
    str(path.relative_to(export)): hashlib.sha256(path.read_bytes()).hexdigest()
    for path in export.rglob('.clang-format') if path.is_file()}
assert git('ls-files', '--stage', '-z') == index, 'Index changed during check'
record['finished'] = time.time()
record['passed'] = True
(OUT / 'validation-final-export.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record, indent=2), flush=True)
