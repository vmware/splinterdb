#!/usr/bin/env python3
"""Prepare or build the separate quota driver; the parent owns the machine lock."""
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
    parent_path = HERE / 'diagnostics-build.json'
    parent = json.loads(parent_path.read_text())
    base = parent['binaries']['insert_benchmark']
    assert digest(base['source']) == base['source_sha256']
    assert digest(base['binary']) == base['binary_sha256']
    source = HERE / 'harnesses/insert_benchmark_quota.c'
    binary = HERE / 'bin/insert_benchmark_quota'
    command = [str(source) if x == base['source'] else str(binary) if x == base['binary'] else x
               for x in base['command']]
    record = dict(built=args.build, linkage='dynamic', profile=parent['profile'],
                  tree=parent['tree'], parent_driver=base,
                  parent_build_manifest=str(parent_path),
                  parent_build_manifest_sha256=digest(parent_path),
                  library_sha256=parent['library_sha256'],
                  runner_sha256=digest(HERE / 'run_quota.py'),
                  prepare_sha256=digest(__file__),
                  quota_modes=['fixed', 'shared'], shared_batch_records=1024,
                  binaries={'insert_benchmark': dict(command=command, source=str(source),
                                                     source_sha256=digest(source), binary=str(binary))})
    output = HERE / ('quota-build.json' if args.build else 'quota-commands.json')
    assert not output.exists(), f'will not replace {output}'
    print(shlex.join(command), flush=True)
    if args.build:
        assert not binary.exists(), f'will not replace {binary}'
        subprocess.run(command, check=True)
        record['binaries']['insert_benchmark']['binary_sha256'] = digest(binary)
    with output.open('x') as stream:
        json.dump(record, stream, indent=2)
        stream.write('\n')


if __name__ == '__main__':
    main()
