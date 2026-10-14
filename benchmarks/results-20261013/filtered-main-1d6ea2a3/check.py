#!/usr/bin/env python3
"""Verify a staged filtered-ranges PR after the disk-format squash merge."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--tree', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
a.tree = a.tree.resolve()
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=True)
main = '1d6ea2a3e2056127c38b54a582075c7eaf7acf4d'
disk = '7a0c817165692b7af3cbec94ad34d4e8586841e1'
before = '33af695feb50b702af080e73cfacffff6ff7b9c6'
merge = '6b0a035c2ed53a11c440c574deae9889deb4751b'
companion = '361838074656ddf04ad37c0452f4373da9211269'
runtime = ['src', 'include', 'tests', 'Makefile', '.github']
record = {'started': time.time(), 'tree': str(a.tree), 'target_main': main,
          'baseline_disk': disk, 'previous_candidate': before,
          'merge_commit': merge, 'checks': {}}

def git(*args):
    return subprocess.check_output(['git', '-C', str(a.tree), *args])

def run(label, command, cwd):
    log = a.output / (label + '.log')
    with log.open('w') as f:
        result = subprocess.run(command, cwd=cwd, stdout=f,
                                stderr=subprocess.STDOUT)
    record['checks'][label] = {'command': command, 'cwd': str(cwd),
                               'returncode': result.returncode,
                               'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest()}
    (a.output / 'checks.json').write_text(json.dumps(record, indent=2) + '\n')
    assert result.returncode == 0, (label, log.read_text())

assert not git('diff', '--name-only'), 'Stage all intended tracked edits first'
assert not git('ls-files', '-u'), 'Unresolved merge conflicts'
assert git('merge-base', main, 'HEAD').decode().strip() == main
assert git('rev-parse', main + '^{tree}') == git('rev-parse', disk + '^{tree}')
assert git('rev-parse', merge + '^{tree}') == git('rev-parse', before + '^{tree}')
assert not git('diff', '--cached', before, '--', *runtime)
assert not git('diff', '--cached', companion, '--', *runtime)
old_diff = git('diff', '--binary', disk, before, '--', *runtime)
new_diff = git('diff', '--binary', '--cached', main, '--', *runtime)
assert old_diff == new_diff
record.update(baseline_tree=git('rev-parse', main + '^{tree}').decode().strip(),
              merge_tree=git('rev-parse', merge + '^{tree}').decode().strip(),
              runtime_pr_diff_sha256=hashlib.sha256(new_diff).hexdigest(),
              runtime_pr_diff_identical=True, unchanged_runtime=True,
              script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
index = git('ls-files', '--stage', '-z')
record['index_sha256'] = hashlib.sha256(index).hexdigest()
export = a.output / ('tracked-export-' + str(time.time_ns()))
export.mkdir()
run('export', ['git', 'checkout-index', '--all', '--prefix=' + str(export) + '/'], a.tree)
run('format', ['./format-check.sh'], export)
run('pr-diff', ['git', 'diff', '--cached', '--check', main], a.tree)
run('working-diff', ['git', 'diff', '--check'], a.tree)
assert index == git('ls-files', '--stage', '-z'), 'Index changed during validation'
record.update(finished=time.time(), passed=True,
              reused_tests='125 debug and 125 release integration cases; 66 focused release and 16 ASan cases, full routing-filter and public-header checks',
              reused_benchmarks='66 serialized real-disk invocations; source, baseline and runtime PR diff unchanged',
              limitation='Normal full CI still applies; previous disk-I/O outlier and tail-latency uncertainty remain')
(a.output / 'checks.json').write_text(json.dumps(record, indent=2) + '\n')
print(json.dumps(record, indent=2))
