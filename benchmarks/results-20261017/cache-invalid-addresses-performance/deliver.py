#!/usr/bin/env python3
"""Copy the verified textual archive and append the scoped branch report.

No Git staging, commits, pushes or benchmark execution. Run only after all
campaign work has finished. Refuse a changed branch or unrelated tracked edits.
"""
import fcntl
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time


ROOT = Path(__file__).resolve().parent
TARGET = ROOT.parents[2] / 'cache-invalid-addresses'
LOCK = ROOT.parents[3] / 'coordination/machine.lock'
BASE = 'da5757265109f3a5936e0ef20daff6a955f775a3'
ARCHIVE_REL = Path('benchmarks/results-20261017/cache-invalid-addresses-performance')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*arguments):
    return subprocess.check_output(['git', '-C', str(TARGET), *arguments], text=True).strip()


def main():
    archive = ROOT / 'archive'
    destination = TARGET / ARCHIVE_REL
    appendix = (ROOT / 'delivery-docs.md').read_text()
    assert appendix.strip() and 'Pending:' not in appendix
    with LOCK.open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert git('rev-parse', 'HEAD') == BASE
        assert git('branch', '--show-current') == 'robj/cache-invalid-addresses'
        assert git('diff', '--name-only') == 'src/clockcache.c'
        assert not git('diff', '--cached', '--name-only')
        assert not destination.exists()
        evidence = json.loads((ROOT / 'delivery-source-check.json').read_text())
        for name, expected in evidence['source_sha256'].items():
            assert digest(TARGET / name) == expected, name
        checksums = {}
        for line in (archive / 'SHA256SUMS').read_text().splitlines():
            expected, name = line.split('  ', 1)
            relative = Path(name)
            assert not relative.is_absolute() and '..' not in relative.parts
            assert digest(archive / relative) == expected, name
            checksums[name] = expected
        actual = {str(p.relative_to(archive)) for p in archive.rglob('*') if p.is_file()}
        assert actual == set(checksums) | {'SHA256SUMS'}
        profiles = json.loads((archive / 'provenance/profiles.json').read_text())
        assert profiles['selected_profile'] == evidence['selected_profile'] == 'async-mask'
        selected = profiles['sources']['async-mask']
        selected_hashes = json.loads((archive / selected['source_inventory']).read_text())
        for name, expected in evidence['source_sha256'].items():
            assert selected_hashes[name] == expected, name
        reconstruction = json.loads((archive / 'reconstruction-verification.json').read_text())
        assert reconstruction['passed'] and reconstruction['selected_profile'] == 'async-mask'
        assert reconstruction['source_inventories'] == {
            name: profile['source_inventory_sha256']
            for name, profile in profiles['sources'].items()
        }
        document = TARGET / 'docs/cache-invalid-addresses-results.md'
        old_document = document.read_text()
        old_hash = digest(document)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(archive, destination)
        for name, expected in checksums.items():
            assert digest(destination / name) == expected, name
        assert digest(destination / 'SHA256SUMS') == digest(archive / 'SHA256SUMS')
        document.write_text(old_document.rstrip() + '\n\n' + appendix.rstrip() + '\n')
        subprocess.run(['git', '-C', str(TARGET), 'diff', '--check'], check=True)
        result = dict(target=str(TARGET), base_commit=BASE, archive=str(destination),
                      archive_files=len(checksums) + 1, source_files_checked=len(evidence['source_sha256']),
                      checksum_inventory_sha256=digest(destination / 'SHA256SUMS'),
                      original_document_sha256=old_hash, document_sha256=digest(document),
                      copied_at=time.time(), staged=False, committed=False)
        (ROOT / 'delivery-copy.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
