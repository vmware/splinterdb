#!/usr/bin/env python3
"""Record reproducible invalid-address validation commands and artifact hashes."""
import hashlib
import json
from pathlib import Path
import subprocess
import time
ROOT = Path(__file__).resolve().parent.parent
TREE = ROOT / 'worktrees/cache-invalid-addresses'
OUT = ROOT / 'coordination/cache-invalid-addresses-validation'
def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
records = []
for mode in ('debug', 'release', 'asan'):
    path = OUT / (mode + '-results.json')
    if path.exists():
        records.extend(json.loads(path.read_text()))
source_files = [TREE / 'Makefile', TREE / 'tests/unit/writeback_set_test.c']
for directory in ('src', 'include'):
    source_files.extend(p for p in sorted((TREE / directory).rglob('*')) if p.is_file())
files = [p for p in OUT.iterdir() if p.is_file() and p.name != 'inventory.json']
jobs = [json.loads(line) for line in (ROOT / 'coordination/jobs.jsonl').read_text().splitlines() if line.strip()]
result = dict(created=time.time(), source_head=subprocess.check_output(['git','rev-parse','HEAD'], cwd=TREE, text=True).strip(),
              source_files_sha256={str(p.relative_to(TREE)): digest(p) for p in source_files},
              source_diff_sha256=hashlib.sha256(subprocess.check_output(['git','diff','--binary','HEAD','--','src','include','tests/unit/writeback_set_test.c'], cwd=TREE)).hexdigest(),
              runs=records, unit_tests=sum(r.get('tests',0) for r in records),
              unit_passed=sum(r.get('passed',0) for r in records),
              driver_runs=sum(1 for r in records if 'tests' not in r),
              jobs=[j for j in jobs if j['name'].startswith('invalid-address-validation-') or j['name'].startswith('cache-invalid-addresses-debug-build')],
              artifacts_sha256={p.name:digest(p) for p in sorted(files)},
              scripts_sha256={name:digest(ROOT/'coordination'/name) for name in ('validate_invalid_addresses.py','inventory_invalid_address_tests.py','run_job.py')},
              libraries_sha256={str(p.relative_to(TREE)):digest(p) for p in sorted((TREE/'build').glob('*/lib/libsplinterdb.so'))})
(OUT / 'inventory.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: result[k] for k in ('unit_tests','unit_passed','driver_runs')}, indent=2))
