#!/usr/bin/env python3
"""Reconstruct source exports from local git objects plus archived patches.

Usage: python3 setup_sources.py --repo /path/to/splinterdb --dest /new/path
No network or compilation. Destination must not exist. Original build/run
commands remain in raw manifests; adapt their absolute paths before rerunning.
"""
import argparse, hashlib, io, json, os, shutil, subprocess, tarfile
from pathlib import Path

def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file()}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--dest', type=Path, required=True)
    args = parser.parse_args()
    archive = Path(__file__).resolve().parent
    profiles = json.loads((archive / 'provenance/profiles.json').read_text())
    destination = args.dest.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    for name in ('main', 'original'):
        metadata = json.loads((archive / f'{name}-source.json').read_text())
        data = subprocess.check_output(['git', '-C', str(args.repo.resolve()),
            'archive', metadata['commit'], '--', *metadata['exported_paths']])
        target = destination / name
        target.mkdir()
        with tarfile.open(fileobj=io.BytesIO(data)) as tar:
            for member in tar.getmembers():
                path = Path(member.name)
                if path.is_absolute() or '..' in path.parts or not (member.isfile() or member.isdir()):
                    raise RuntimeError(f'Unsupported/unsafe archive member: {member.name}')
            tar.extractall(target)
    for name, profile in profiles['sources'].items():
        if name not in ('main', 'original'):
            target = destination / name
            shutil.copytree(destination / 'original', target)
            patch = archive / profile['patch']
            if patch.stat().st_size:
                environment = os.environ.copy()
                for key in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR'):
                    environment.pop(key, None)
                environment['GIT_CEILING_DIRECTORIES'] = str(target.parent)
                subprocess.run(['git', 'apply', '--no-index', str(patch)],
                               cwd=target, env=environment, check=True)
        expected = json.loads((archive / profile['source_inventory']).read_text())
        actual = hashes(destination / name)
        if expected != actual:
            raise RuntimeError(f'Reconstructed source hash mismatch: {name}')
    print(f'Verified {len(profiles["sources"])} source profiles in {destination}')

if __name__ == '__main__':
    main()
