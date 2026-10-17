#!/usr/bin/env python3
"""Build identical frozen public harnesses; invoke under run_job.py build.

Run separately for parent and candidate after their release library builds.
Outputs are immutable: use another build directory for any measured revision.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'coordination/cache-invalid-addresses'
BASE = '2907e97e90a7861e789192485e0892522a9feb3a'
PROFILES = {
    'parent': (ROOT / 'worktrees/cache-errors', 'build-invalid-address-control'),
    'validated': (ROOT / 'worktrees/cache-invalid-addresses', 'build-invalid-address-bench'),
}
HARNESSES = {'kvbench': 'kvbench.c', 'insert_benchmark': 'insert_benchmark.c'}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('label', choices=PROFILES)
    args = parser.parse_args()
    label = args.label
    tree, build = PROFILES[label]
    OUT.mkdir(parents=True, exist_ok=True)
    record_path = OUT / f'{label}-build.json'
    assert not record_path.exists(), record_path
    library = tree / build / 'release/lib/libsplinterdb.a'
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=tree, text=True).strip()
    if label == 'parent':
        assert head == BASE, head
        assert not subprocess.check_output(['git', 'diff', 'HEAD', '--', 'src', 'include', 'Makefile'], cwd=tree)
    builds = {}
    for name, filename in HARNESSES.items():
        source = ROOT / 'baseline/benchmarks' / filename
        binary = tree / build / 'release/bin' / name
        binary.parent.mkdir(parents=True, exist_ok=True)
        assert not binary.exists(), binary
        command = ['cc', '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread', '-Werror',
                   '-DXXH_STATIC_LINKING_ONLY', '-fPIC', '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
                   '-O3', '-ffast-math', '-flto', '-march=native']
        if name == 'kvbench':
            command += ['-DKVBENCH_CLOSE_HAS_FORCE', '-I', str(tree / 'include')]
        else:
            command += ['-std=gnu11', '-I', str(tree / 'include'), '-I', str(tree / 'src'),
                        '-I', str(tree / 'src/platform_linux')]
        command += [str(source), str(library), '-laio', '-lxxhash', '-lm', '-lpthread', '-o', str(binary)]
        subprocess.run(command, check=True)
        builds[name] = dict(command=command, binary=str(binary), source_sha256=digest(source),
                            binary_sha256=digest(binary), library_sha256=digest(library))
        binary.with_suffix('.json').write_text(json.dumps(builds[name], indent=2) + '\n')
    configs = list((tree / build / 'release').glob('build-config.*'))
    assert len(configs) == 1, configs
    config = configs[0].read_text()
    (OUT / f'{label}-build-config.txt').write_text(config)
    flags = [line for line in config.splitlines() if line.startswith(('CFLAGS =', 'LDFLAGS =', 'LIBS =', 'CC ='))]
    files = [tree / 'Makefile']
    for directory in ('src', 'include'):
        files.extend(path for path in sorted((tree / directory).rglob('*')) if path.is_file())
    patch = subprocess.check_output(['git', 'diff', '--binary', BASE, '--', 'src', 'include', 'Makefile'], cwd=tree)
    (OUT / f'{label}-source-change.patch').write_bytes(patch)
    record = dict(label=label, source_head=head, parent=BASE, tree=str(tree), build_root=build,
                  compiler=subprocess.check_output(['cc', '--version'], text=True).splitlines()[0],
                  flags=flags, builds=builds, library=str(library), library_sha256=digest(library),
                  source_files_sha256={str(path.relative_to(tree)): digest(path) for path in files},
                  source_patch_sha256=hashlib.sha256(patch).hexdigest())
    if label == 'validated':
        parent = json.loads((OUT / 'parent-build.json').read_text())
        assert record['flags'] == parent['flags'], (record['flags'], parent['flags'])
        for name in HARNESSES:
            assert builds[name]['source_sha256'] == parent['builds'][name]['source_sha256']
            normalized = [part.replace(str(tree), 'TREE').replace(build, 'BUILD') for part in builds[name]['command']]
            parent_normalized = [part.replace(parent['tree'], 'TREE').replace(parent['build_root'], 'BUILD')
                                 for part in parent['builds'][name]['command']]
            assert normalized == parent_normalized
    record_path.write_text(json.dumps(record, indent=2) + '\n')
    print(record_path, flush=True)


if __name__ == '__main__':
    main()
