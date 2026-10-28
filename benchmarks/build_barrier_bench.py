#!/usr/bin/env python3
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

p = argparse.ArgumentParser()
p.add_argument('tree', type=Path)
p.add_argument('--build-root', default='build')
p.add_argument('--source', type=Path, default=Path(__file__).with_name('barrier_bench.c'))
a = p.parse_args()
tree = a.tree.resolve()
source = a.source.resolve()
library = tree / a.build_root / 'release/lib/libsplinterdb.a'
binary = tree / a.build_root / 'release/bin' / source.stem
binary.parent.mkdir(parents=True, exist_ok=True)
command = ['cc', '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread', '-Werror',
           '-DXXH_STATIC_LINKING_ONLY', '-fPIC', '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
           '-O3', '-flto', '-march=native']
header = tree / 'include/splinterdb/splinterdb.h'
if '#define SPLINTERDB_NOTIFICATION_BLOCKING' in header.read_text():
    command += ['-DHAVE_ASYNC_BARRIER', '-DHAVE_SENTINEL_NOTIFICATION']
elif 'splinterdb_durable_barrier_async' in header.read_text():
    command.append('-DHAVE_ASYNC_BARRIER')
if 'message_create(' not in (tree / 'include/splinterdb/data.h').read_text():
    command.append('-Dmessage_create=slice_create')
command += ['-I', str(tree / 'include'), str(source),
            str(library), '-laio', '-lxxhash', '-lm',
            '-lpthread', '-o', str(binary)]
subprocess.run(command, check=True)
binary.with_suffix('.json').write_text(json.dumps(dict(
    command=command, source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
    public_header_sha256=hashlib.sha256(header.read_bytes()).hexdigest(),
    library_sha256=hashlib.sha256(library.read_bytes()).hexdigest(),
    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()), indent=2) + '\n')
print(binary)
