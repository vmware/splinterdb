#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

p = argparse.ArgumentParser()
p.add_argument('tree', type=Path)
p.add_argument('--build-root', default='build')
a = p.parse_args()
tree = a.tree.resolve()
root = Path(__file__).resolve().parent.parent
source = root / 'baseline/benchmarks/kvbench.c'
binary = tree / a.build_root / 'release/bin/kvbench'
binary.parent.mkdir(parents=True, exist_ok=True)
command = ['cc', '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread', '-Werror',
           '-DXXH_STATIC_LINKING_ONLY', '-fPIC', '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
           '-O3', '-ffast-math', '-flto', '-march=native', '-DKVBENCH_CLOSE_HAS_FORCE',
           '-I', str(tree / 'include'), str(source),
           str(tree / a.build_root / 'release/lib/libsplinterdb.a'), '-laio', '-lxxhash', '-lm',
           '-lpthread', '-o', str(binary)]
subprocess.run(command, check=True)
provenance = dict(command=command, source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                  binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
binary.with_suffix('.json').write_text(json.dumps(provenance, indent=2) + '\n')
print(binary)
