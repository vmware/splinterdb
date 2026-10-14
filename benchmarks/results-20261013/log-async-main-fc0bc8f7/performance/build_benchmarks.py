"""Build identical harness sources against separately compiled branch libraries."""
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path('/home/rob/projects/splinterdb')
CAMP = ROOT / '.cache/projects-20261011'
OUT = Path(__file__).resolve().parent

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

profiles = {
    'main': (OUT / 'sources/main', 'build'),
    'log-sync': (OUT / 'sources/log-sync', 'build'),
    'log-async': (CAMP / 'worktrees/log-group-limit', 'build-main-fc0bc8f7'),
    'branch-prefix': (CAMP / 'worktrees/branch-prefix', 'build-main-fc0bc8f7'),
    'cache-invalid-addresses': (CAMP / 'worktrees/cache-invalid-addresses', 'build-main-fc0bc8f7'),
}
for variant in ['direct', 'bucket', 'bucket-simd', 'bucket-atomic']:
    profiles['directory-' + variant] = (CAMP / 'worktrees/cache-directory', 'build-main-fc0bc8f7/' + variant)

sources = OUT / 'harnesses'
sources.mkdir(exist_ok=True)
for name, source in {
    'insert_benchmark': OUT / 'sources/log-sync/benchmarks/insert_benchmark.c',
    'kvbench': CAMP / 'coordination/main-7bb5fab/benchmarks/sources/kvbench.c',
    'prefix_kvbench': CAMP / 'coordination/main-7bb5fab/benchmarks/sources/prefix_kvbench.c',
}.items():
    target = sources / (name + '.c')
    if target.exists():
        assert sha(target) == sha(source)
    else:
        shutil.copyfile(source, target)

for profile in sys.argv[1:]:
    tree, build = profiles[profile]
    lib = tree / build / 'release/lib/libsplinterdb.a'
    head = json.loads((tree / 'source.json').read_text())['commit'] if (tree / 'source.json').exists() else subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=tree, text=True).strip()
    record = dict(profile=profile, tree=str(tree), head=head, library=str(lib), library_sha256=sha(lib), source_files_sha256={}, binaries={})
    record['library_build_configs'] = {str(p): {'sha256': sha(p), 'text': p.read_text()} for p in sorted(lib.parent.parent.glob('build-config.*'))}
    for subdir in ['src', 'include']:
        for path in sorted((tree / subdir).rglob('*')):
            if path.is_file():
                record['source_files_sha256'][str(path.relative_to(tree))] = sha(path)
    binaries = ['insert_benchmark'] if profile.startswith('log-') else ['insert_benchmark', 'kvbench']
    if profile in ['main', 'branch-prefix']:
        binaries.append('prefix_kvbench')
    for name in binaries:
        source = sources / (name + '.c')
        exe = OUT / 'bin' / profile / name
        exe.parent.mkdir(parents=True, exist_ok=True)
        macros = []
        if name == 'insert_benchmark' and not profile.startswith('log-'):
            macros += ['-DKVBENCH_NO_LOG_GROUP_LIMIT']
        if name in ['kvbench', 'prefix_kvbench']:
            macros += ['-DKVBENCH_CLOSE_HAS_FORCE']
        if name == 'prefix_kvbench':
            macros += ['-DKVBENCH_SPACE_MODE']
            if profile == 'branch-prefix':
                macros += ['-DKVBENCH_PREFIX_SUPPORT']
        command = ['gcc', '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread', '-Werror', '-DXXH_STATIC_LINKING_ONLY', '-fPIC', '-DSPLINTERDB_PLATFORM_DIR=platform_linux', '-O3', '-ffast-math', '-flto', '-march=native', *macros, '-I', str(tree / 'include'), '-I', str(tree / 'src'), '-I', str(tree / 'src/platform_linux'), str(source), str(lib), '-laio', '-lxxhash', '-lm', '-lpthread', '-o', str(exe)]
        print('Building', profile, name, flush=True)
        subprocess.run(command, check=True)
        record['binaries'][name] = dict(command=command, binary=str(exe), binary_sha256=sha(exe), source=str(source), source_sha256=sha(source))
    (OUT / (profile + '-build.json')).write_text(json.dumps(record, indent=2) + '\n')
