#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

p = argparse.ArgumentParser()
p.add_argument('tree', type=Path)
a = p.parse_args()
tree = a.tree.resolve()
source = Path(__file__).with_name('barrier_bench.c')
binary = tree / 'build/release/bin/barrier_bench'
binary.parent.mkdir(parents=True, exist_ok=True)
command = ['cc', '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread', '-Werror',
           '-DXXH_STATIC_LINKING_ONLY', '-fPIC', '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
           '-O3', '-flto', '-march=native']
if 'splinterdb_durable_barrier_async' in (tree / 'include/splinterdb/splinterdb.h').read_text():
    command.append('-DHAVE_ASYNC_BARRIER')
command += ['-I', str(tree / 'include'), str(source),
            str(tree / 'build/release/lib/libsplinterdb.a'), '-laio', '-lxxhash', '-lm',
            '-lpthread', '-o', str(binary)]
subprocess.run(command, check=True)
binary.with_suffix('.json').write_text(json.dumps(dict(
    command=command, source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()), indent=2) + '\n')
print(binary)
