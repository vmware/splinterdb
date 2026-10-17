#!/usr/bin/env python3
"""Compile the separate insertion diagnostics driver under the parent lock."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

HERE = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', action='store_true')
    args = parser.parse_args()
    original = json.loads((HERE / 'dynamic-build.json').read_text())
    base = original['binaries']['insert_benchmark']
    assert digest(base['source']) == base['source_sha256']
    assert digest(base['binary']) == base['binary_sha256']
    source = HERE / 'harnesses/insert_benchmark_diagnostics.c'
    binary = HERE / 'bin/insert_benchmark_diagnostics'
    command = [str(source) if x == base['source'] else str(binary) if x == base['binary'] else x
               for x in base['command']]
    record = dict(built=args.build, linkage='dynamic', profile=original['profile'],
                  tree=original['tree'], original_driver=base,
                  library_sha256=original['library_sha256'],
                  binaries={'insert_benchmark': dict(command=command, source=str(source),
                                                     source_sha256=digest(source), binary=str(binary))})
    print(shlex.join(command), flush=True)
    if args.build:
        assert not binary.exists(), f'will not replace {binary}'
        subprocess.run(command, check=True)
        record['binaries']['insert_benchmark']['binary_sha256'] = digest(binary)
    suffix = 'build' if args.build else 'commands'
    (HERE / f'diagnostics-{suffix}.json').write_text(json.dumps(record, indent=2) + '\n')


if __name__ == '__main__':
    main()
