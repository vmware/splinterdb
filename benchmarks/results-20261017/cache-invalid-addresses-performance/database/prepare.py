#!/usr/bin/env python3
"""Freeze shared drivers; print compile commands, or build under parent's job lock."""
import argparse
import hashlib
import json
import shlex
import shutil
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
CAMPAIGN = HERE.parent
WORKTREE = CAMPAIGN.parent.parent


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', action='store_true')
    parser.add_argument('--static', metavar='PROFILE', help='also permit per-profile static LTO confirmation')
    args = parser.parse_args()
    profile = args.static or 'main'
    tree = CAMPAIGN / 'sources' / profile
    library = CAMPAIGN / 'builds' / profile / 'release/lib'
    target = HERE / 'bin' / profile if args.static else HERE / 'bin'
    source_dir = HERE / 'harnesses'
    source_dir.mkdir(exist_ok=True)
    target.mkdir(parents=True, exist_ok=True)
    record = {'linkage': 'static' if args.static else 'dynamic', 'profile': profile,
              'tree': str(tree), 'binaries': {}}
    for name in ['kvbench', 'insert_benchmark']:
        source = WORKTREE / 'benchmarks' / (name + '.c')
        frozen = source_dir / source.name
        if frozen.exists():
            assert sha(source) == sha(frozen), f'harness changed: {source}'
        else:
            shutil.copyfile(source, frozen)
        binary = target / name
        flags = ['-DKVBENCH_CLOSE_HAS_FORCE'] if name == 'kvbench' else []
        link = [str(library / 'libsplinterdb.a')] if args.static else ['-L', str(library), '-lsplinterdb']
        command = ['gcc', '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread', '-Werror',
                   '-DXXH_STATIC_LINKING_ONLY', '-fPIC',
                   '-DSPLINTERDB_PLATFORM_DIR=platform_linux', '-O3', '-ffast-math',
                   '-flto', '-march=native', *flags,
                   '-I', str(tree / 'include'), '-I', str(tree / 'src'),
                   '-I', str(tree / 'src/platform_linux'), str(frozen), *link,
                   '-laio', '-lxxhash', '-lm', '-lpthread', '-o', str(binary)]
        print(shlex.join(command), flush=True)
        if args.build:
            assert not binary.exists(), f'will not replace existing binary: {binary}'
            subprocess.run(command, check=True)
        record['binaries'][name] = {'command': command, 'source': str(frozen),
                                   'source_sha256': sha(frozen), 'binary': str(binary)}
        if args.build:
            record['binaries'][name]['binary_sha256'] = sha(binary)
    record['built'] = args.build
    record['library_sha256'] = sha(library / ('libsplinterdb.a' if args.static else 'libsplinterdb.so'))
    filename = ('static-' + profile if args.static else 'dynamic') + ('-build.json' if args.build else '-commands.json')
    (HERE / filename).write_text(json.dumps(record, indent=2) + '\n')


if __name__ == '__main__':
    main()
