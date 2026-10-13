#!/usr/bin/env python3
"""Archive the whole-merge handoff query controls."""
import hashlib
import json
from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parent.parent
out = ROOT / 'worktrees/cache-handoff/benchmarks/results-20261012/cache-handoff-mainmerge'
out.mkdir(parents=True, exist_ok=True)
inventory = []


def copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    inventory.append(dict(source=str(source), archived=str(destination.relative_to(out)),
                          sha256=hashlib.sha256(destination.read_bytes()).hexdigest()))


extensions = {'.csv', '.json', '.txt', '.log', '.py', '.c', '.patch'}
for mode in ('resident', 'disk', 'resident-point8-repeat20'):
    source = ROOT / 'benchmarks/cache-handoff-mainmerge' / mode
    assert json.loads((source / 'manifest.json').read_text()).get('complete'), source
    for path in sorted(source.iterdir()):
        if path.is_file() and path.suffix in extensions and path.name != 'kvbench.c':
            copy(path, out / mode / path.name)
source = ROOT / 'coordination/cache-handoff-mainmerge'
for path in sorted(source.iterdir()):
    if (path.is_file() and path.suffix in extensions
            and path.name != 'kvbench-build-prestage-guard.log'):
        copy(path, out / 'provenance' / path.name)
for name in ('handoff_mainmerge_compare.py', 'build_handoff_mainmerge.py',
             'run_handoff_mainmerge.py', 'archive_handoff_mainmerge.py', 'handoff_mainmerge_point8.py',
             'run_job.py', 'build_kvbench.py'):
    copy(ROOT / 'coordination' / name, out / 'scripts' / name)
copy(ROOT / 'baseline/benchmarks/kvbench.c', out / 'scripts/kvbench.c')
(out / 'archive-inventory.json').write_text(json.dumps(inventory, indent=2) + '\n')
(out / '.gitattributes').write_text('*.csv whitespace=cr-at-eol\n*.patch -whitespace\n')
for item in inventory:
    assert hashlib.sha256((out / item['archived']).read_bytes()).hexdigest() == item['sha256']
print(out, len(inventory), 'artifacts')
