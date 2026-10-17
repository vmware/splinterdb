#!/usr/bin/env python3
"""Archive completed invalid-address A/B evidence, excluding database images."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent.parent
COORD = ROOT / 'coordination/cache-invalid-addresses'
TREE = ROOT / 'worktrees/cache-invalid-addresses'
OUT = TREE / 'benchmarks/results-20261013/cache-invalid-addresses'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def copy_file(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    copied = []
    manifests = []
    for directory in sorted((ROOT / 'benchmarks/cache-invalid-addresses').iterdir()):
        if not directory.is_dir():
            continue
        manifest_path = directory / 'manifest.json'
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text())
        assert manifest['complete'], directory
        assert all(record['correctness_passed'] and record['returncode'] == 0
                   for record in manifest['runs']), directory
        manifests.append(manifest)
        for source in sorted(directory.rglob('*')):
            if source.is_file() and source.name != 'probe.db':
                destination = OUT / directory.name / source.relative_to(directory)
                copy_file(source, destination)
                copied.append(destination)
    for source in sorted(COORD.iterdir()):
        if source.is_file() and source.suffix in {'.json', '.txt', '.patch', '.log'}:
            destination = OUT / 'provenance' / source.name
            copy_file(source, destination)
            copied.append(destination)
    for source in sorted((ROOT / 'coordination').glob('invalid-addresses-*.log')):
        destination = OUT / 'provenance' / source.name
        copy_file(source, destination)
        copied.append(destination)
    for name in ('build_invalid_addresses.py', 'invalid_addresses_compare.py',
                 'invalid_addresses_repeat.py', 'invalid_addresses_insert_repeat.py',
                 'run_invalid_addresses.py', 'archive_invalid_addresses.py', 'report_invalid_addresses.py',
                 'run_job.py', 'audit_serialization.py'):
        source = ROOT / 'coordination' / name
        destination = OUT / 'runners' / name
        copy_file(source, destination)
        copied.append(destination)
    jobs = [json.loads(line) for line in (ROOT / 'coordination/jobs.jsonl').read_text().splitlines()
            if 'invalid-address' in json.loads(line)['name']]
    jobs_path = OUT / 'provenance/jobs.json'
    jobs_path.write_text(json.dumps(jobs, indent=2) + '\n')
    copied.append(jobs_path)
    measured = json.loads((COORD / 'validated-build.json').read_text())
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=TREE, text=True).strip()
    for relative, expected in measured['source_files_sha256'].items():
        assert digest(TREE / relative) == expected, relative
        source = subprocess.check_output(['git', 'show', f'{head}:{relative}'], cwd=TREE)
        assert hashlib.sha256(source).hexdigest() == expected, relative
    inventory = dict(artifact_count=len(copied), process_count=sum(len(m['runs']) for m in manifests),
                     source_verified_at_head=head,
                     files={str(path.relative_to(OUT)): digest(path) for path in sorted(copied)})
    (OUT / 'inventory.json').write_text(json.dumps(inventory, indent=2) + '\n')
    print(json.dumps(dict(output=str(OUT), artifacts=len(copied), processes=inventory['process_count']), indent=2))


if __name__ == '__main__':
    main()
