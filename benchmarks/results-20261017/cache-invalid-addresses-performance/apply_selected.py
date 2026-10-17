#!/usr/bin/env python3
"""Apply only the already validated clockcache source to the active branch."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parent
TARGET = ROOT.parents[2] / 'cache-invalid-addresses'
BASE = 'da5757265109f3a5936e0ef20daff6a955f775a3'
PARENT = 'a26ce40399a60f76f8559eb82703f688bf0bd71662ce93d9ad4232f7451c107e'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args):
    return subprocess.check_output(['git', '-C', str(TARGET), *args], text=True).strip()


def main():
    validation = json.loads((ROOT / 'validation-async-mask/validation.json').read_text())
    assert validation['passed'] and validation['sources_unchanged']
    assert validation['passed_unit_executions'] == 124
    source = ROOT / 'sources/async-mask'
    hashes = validation['source_sha256_after']
    assert git('rev-parse', 'HEAD') == BASE
    assert git('branch', '--show-current') == 'robj/cache-invalid-addresses'
    assert git('diff', '--name-only') == 'src/clockcache.c'
    assert not git('diff', '--cached', '--name-only')
    assert digest(TARGET / 'src/clockcache.c') == PARENT
    for name, expected in hashes.items():
        assert digest(source / name) == expected, name
        if name != 'src/clockcache.c':
            assert digest(TARGET / name) == expected, name
    old_evidence = ROOT / 'delivery-source-check.json'
    retained = ROOT / 'review/async-load-delivery-source-check.json'
    assert not retained.exists()
    shutil.copyfile(old_evidence, retained)
    shutil.copyfile(source / 'src/clockcache.c', TARGET / 'src/clockcache.c')
    for name, expected in hashes.items():
        assert digest(TARGET / name) == expected, name
    subprocess.run(['git', '-C', str(TARGET), 'diff', '--check'], check=True)
    result = dict(target_worktree=str(TARGET), target_branch=git('branch', '--show-current'),
                  base_commit=BASE, selected_profile='async-mask',
                  selected_clockcache_sha256=hashes['src/clockcache.c'],
                  validated_source_files_match=True, checked_files=len(hashes),
                  source_sha256=hashes, checked_at=time.time(),
                  runtime_diff_files=['src/clockcache.c'])
    old_evidence.write_text(json.dumps(result, indent=2) + '\n')
    print('Applied async-mask;', len(hashes), 'validated source hashes match;',
          hashes['src/clockcache.c'])


if __name__ == '__main__':
    main()
