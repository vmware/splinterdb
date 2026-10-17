#!/usr/bin/env python3
"""Build one reusable harness object; root coordinates execution/locking."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

p = argparse.ArgumentParser()
p.add_argument('tree', type=Path, help='private headers matching library ABI')
p.add_argument('library', type=Path, help='libsplinterdb.so or .a')
p.add_argument('--cc', default='cc')
p.add_argument('--output', type=Path, default=Path(__file__).with_name('cache_api_probe'))
a = p.parse_args()
tree, library, binary = a.tree.resolve(), a.library.resolve(), a.output.resolve()
source = Path(__file__).resolve().with_name('cache_api_probe.c')
obj = binary.with_suffix('.o')
metadata = binary.with_suffix('.json')
for target in (binary, obj, metadata):
    if target.exists():
        p.error(f'refusing to overwrite {target}')
binary.parent.mkdir(parents=True, exist_ok=True)
compile_command = [a.cc, '-D_GNU_SOURCE', '-std=gnu11', '-O3', '-g', '-pthread',
                   '-Wall', '-Werror', '-ffast-math', '-fno-lto', '-march=native',
                   '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
                   '-I', str(tree / 'include'), '-I', str(tree / 'src'),
                   '-I', str(tree / 'src/platform_linux'),
                   '-c', str(source), '-o', str(obj)]
dynamic = library.suffix == '.so'
if dynamic:
    # Do not embed a RUNPATH: the same executable uses the caller's selected
    # LD_LIBRARY_PATH. Its private ABI must match the compile-time headers.
    library_args = ['-L', str(library.parent), '-lsplinterdb']
else:
    library_args = [str(library)]
link_command = [a.cc, '-pthread', str(obj), *library_args,
                '-laio', '-lxxhash', '-lm', '-o', str(binary)]
subprocess.run(compile_command, check=True)
subprocess.run(link_command, check=True)
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
headers = sorted({*tree.joinpath('include').rglob('*.h'), *tree.joinpath('src').rglob('*.h')})
metadata.write_text(json.dumps({
    'commands': [compile_command, link_command],
    'source_sha256': digest(source), 'object_sha256': digest(obj),
    'link_library': str(library), 'link_library_sha256': digest(library),
    'binary_sha256': digest(binary), 'dynamic': dynamic,
    'headers_sha256': {str(path.relative_to(tree)): digest(path) for path in headers},
    'compiler': subprocess.check_output([a.cc, '--version'], text=True).splitlines()[0],
}, indent=2) + '\n')
print(binary)
