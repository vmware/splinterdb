#!/usr/bin/env python3
"""Link the frozen public harness to the release library after the main merge.

Root builds build-mainmerge/release first. Run this under run_job.py build.
The before binary/library are read-only controls from 23d26d1.
"""
import hashlib
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parent.parent
TREE = ROOT / 'worktrees/cache-handoff'
OUT = ROOT / 'coordination/cache-handoff-mainmerge'
BEFORE = '23d26d165923d174e4d5d5ea2a4774ece697b9aa'
UPSTREAM = '7d607b118b1f606c51430759adc75b069423afae'


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    OUT.mkdir(exist_ok=True)
    binary = TREE / 'build-mainmerge/release/bin/kvbench'
    assert not binary.exists(), 'Refusing to overwrite a measured harness'
    original = json.loads((ROOT / 'coordination/cache-handoff-selection/builds.json').read_text())
    # Establish that the immutable selected binary represents the pre-merge head.
    for relative, expected in original['source_files_sha256'].items():
        source = subprocess.check_output(['git', 'show', f'{BEFORE}:{relative}'], cwd=TREE)
        assert hashlib.sha256(source).hexdigest() == expected, relative
    old_binary = TREE / 'build-selected/release/bin/kvbench'
    old_library = TREE / 'build-selected/release/lib/libsplinterdb.a'
    assert digest(old_binary) == original['builds']['selected']['binary_sha256']
    assert digest(old_library) == original['builds']['selected']['library_sha256']
    for directory in ('src', 'include'):
        for source_path in (TREE / directory).rglob('*'):
            if source_path.is_file():
                assert not any(line.startswith((b'<<<<<<< ', b'>>>>>>> '))
                               for line in source_path.read_bytes().splitlines()), source_path
    subprocess.run(['python3', str(ROOT / 'coordination/build_kvbench.py'), str(TREE),
                    '--build-root', 'build-mainmerge'], check=True)
    records = {}
    configurations = {}
    for label, build in [('before', 'build-selected'), ('merged', 'build-mainmerge')]:
        executable = TREE / build / 'release/bin/kvbench'
        library = TREE / build / 'release/lib/libsplinterdb.a'
        record = json.loads(executable.with_suffix('.json').read_text())
        assert record['binary_sha256'] == digest(executable)
        record['library_sha256'] = digest(library)
        configs = list((TREE / build / 'release').glob('build-config.*'))
        assert len(configs) == 1, configs
        config = configs[0].read_text()
        configurations[label] = [line for line in config.splitlines()
                                 if line.startswith(('CFLAGS =', 'LDFLAGS =', 'LIBS =', 'CC ='))]
        (OUT / f'{label}-build-config.txt').write_text(config)
        (OUT / f'{label}-symbols.txt').write_text(subprocess.check_output(
            ['nm', '-S', '--size-sort', str(executable)], text=True))
        records[label] = record
    assert configurations['before'] == configurations['merged'], configurations
    assert records['before']['source_sha256'] == records['merged']['source_sha256']
    before_command = [part.replace('build-selected', 'BUILD_ROOT') for part in records['before']['command']]
    merged_command = [part.replace('build-mainmerge', 'BUILD_ROOT') for part in records['merged']['command']]
    assert before_command == merged_command
    patch = subprocess.check_output(['git', 'diff', '--binary', BEFORE, '--',
                                     'src', 'include', 'Makefile'], cwd=TREE)
    (OUT / 'measured-source-change.patch').write_bytes(patch)
    files = [TREE / 'Makefile']
    for directory in ('src', 'include'):
        files.extend(path for path in sorted((TREE / directory).rglob('*')) if path.is_file())
    metadata = dict(before_source=BEFORE, upstream_source=UPSTREAM,
                    measured_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=TREE, text=True).strip(),
                    scope='whole origin/main merge including upstream blob and btree changes; not isolated cache overhead',
                    builds=records, measured_patch_sha256=hashlib.sha256(patch).hexdigest(),
                    source_files_sha256={str(path.relative_to(TREE)): digest(path) for path in files})
    (OUT / 'builds.json').write_text(json.dumps(metadata, indent=2) + '\n')
    (OUT / 'build_handoff_mainmerge.py').write_bytes(Path(__file__).read_bytes())
    print(OUT, flush=True)


if __name__ == '__main__':
    main()
