#!/usr/bin/env python3
"""Build one B-tree harness against verified frozen main/current libraries.

Only links the harness; never rebuilds or mutates either source tree/library.
The metadata input is the archived profile map from the prefix campaign. This
helper acquires the shared exclusive machine lock, archives source and compiler
provenance, and refuses existing output artifacts. No benchmarks are launched.
"""

import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def verify_profile(profile):
    tree = Path(profile['tree'])
    library = Path(profile['library'])
    if sha(library) != profile['library_sha256']:
        raise RuntimeError(f'Frozen library changed: {library}')
    sources = profile['source_files_sha256']
    for relative, digest in sources.items():
        if sha(tree / relative) != digest:
            raise RuntimeError(f'Frozen source changed: {tree / relative}')
    return dict(tree=str(tree), library=str(library),
                library_sha256=profile['library_sha256'],
                source_files_sha256=sources)


def stop_group(proc):
    if proc is None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    # Retire descendants even if the original compiler driver already exited.
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def invoke(command, log, environment):
    proc = None
    with log.open('x') as stream:
        try:
            proc = subprocess.Popen(command, stdout=stream,
                                    stderr=subprocess.STDOUT,
                                    env=environment, start_new_session=True)
            result = proc.wait(timeout=600)
            if result:
                raise RuntimeError(f'Compiler exited {result}; see {log}')
        except BaseException:
            stop_group(proc)
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--metadata', type=Path, required=True)
    parser.add_argument('--current-profile', default='highwater')
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--archive-dir', type=Path, required=True)
    parser.add_argument('--machine-lock', type=Path, required=True)
    parser.add_argument('--cc', default='cc')
    args = parser.parse_args()
    args.metadata = args.metadata.resolve()
    args.output_dir = args.output_dir.resolve()
    args.archive_dir = args.archive_dir.resolve()
    args.machine_lock = args.machine_lock.resolve()
    source = Path(__file__).with_name('btree_prefix_bench.c').resolve()
    source_hash = sha(source)
    metadata_hash = sha(args.metadata)
    frozen = json.loads(args.metadata.read_text())
    profiles = {'main': frozen['main'], 'current': frozen[args.current_profile]}
    artifact_dir = args.archive_dir / 'build'
    output = args.archive_dir / 'build.json'
    if output.exists() or artifact_dir.exists():
        parser.error('Build archive/output already exists; choose a fresh archive')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.archive_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir.mkdir()
    source_copy = artifact_dir / source.name
    source_copy.write_bytes(source.read_bytes())
    runner_copy = artifact_dir / Path(__file__).name
    runner_copy.write_bytes(Path(__file__).read_bytes())
    started = now()
    print('Waiting for exclusive machine lock', flush=True)
    # The shared coordination file already exists outside this worktree's
    # writable roots. flock(EX) needs no filesystem mutation or write access.
    with args.machine_lock.open('r') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        print('Acquired exclusive machine lock; linking two harnesses', flush=True)
        identities = {name: verify_profile(p) for name, p in profiles.items()}
        compiler = subprocess.check_output([args.cc, '--version'], text=True)
        results = {}
        for name, identity in identities.items():
            tree = Path(identity['tree'])
            prefix = 'key_compare_is_lexical' in (tree / 'include/splinterdb/data.h').read_text()
            binary = args.output_dir / f'{name}-btree-prefix-bench'
            if binary.exists():
                raise RuntimeError(f'Binary already exists: {binary}')
            tmp = Path(tempfile.mkdtemp(prefix=f'build-{name}-', dir=args.output_dir))
            environment = dict(os.environ, TMPDIR=str(tmp))
            command = [args.cc, '-D_GNU_SOURCE', '-ggdb', '-Wall', '-pthread',
                       '-Werror', '-Wvla', '-DXXH_STATIC_LINKING_ONLY', '-fPIC',
                       '-DSPLINTERDB_PLATFORM_DIR=platform_linux',
                       f'-DBTREE_BENCH_PREFIX_SUPPORT={int(prefix)}',
                       '-O3', '-ffast-math', '-flto', '-march=native',
                       '-I', str(tree / 'include'), '-I', str(tree / 'src'),
                       '-I', str(tree / 'src/platform_linux'), str(source_copy),
                       identity['library'], '-laio', '-lxxhash', '-lm', '-lpthread',
                       '-o', str(binary)]
            log = artifact_dir / f'{name}-compile.log'
            invoke(command, log, environment)
            verify_profile(profiles[name])
            results[name] = dict(identity, binary=str(binary),
                binary_sha256=sha(binary), source=str(source_copy),
                source_sha256=source_hash, prefix_supported=prefix,
                command=command, compiler=compiler, build_log=str(log),
                build_log_sha256=sha(log), tmpdir=str(tmp))
            print(f'Built {name}: {binary}', flush=True)
        for profile in profiles.values():
            verify_profile(profile)
        if sha(source) != source_hash or sha(source_copy) != source_hash:
            raise RuntimeError('Harness source changed during compilation')
        if sha(args.metadata) != metadata_hash:
            raise RuntimeError('Input metadata changed during compilation')
        results['_build'] = dict(started=started, finished=now(),
            input_metadata=str(args.metadata), input_metadata_sha256=metadata_hash,
            runner=str(runner_copy), runner_sha256=sha(runner_copy),
            machine_lock=str(args.machine_lock), machine_lock_mode='exclusive',
            runtime_sources_unchanged=True)
        with output.open('x') as stream:
            json.dump(results, stream, indent=2, allow_nan=False)
            stream.write('\n')
    print(output, flush=True)


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    main()
