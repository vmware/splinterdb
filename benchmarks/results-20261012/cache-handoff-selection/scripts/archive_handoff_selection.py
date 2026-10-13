#!/usr/bin/env python3
"""Archive the selected handoff branch's guard-only query controls."""
import hashlib
import json
from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parent.parent
out = ROOT / 'worktrees/cache-handoff/benchmarks/results-20261012/cache-handoff-selection'
out.mkdir(parents=True, exist_ok=True)
inventory = []


def copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    inventory.append(dict(source=str(source), archived=str(destination.relative_to(out)),
                          sha256=hashlib.sha256(destination.read_bytes()).hexdigest()))


extensions = {'.csv', '.json', '.txt', '.log', '.py', '.c', '.patch'}
for mode in ('resident', 'disk', 'resident-point8-repeat20'):
    source = ROOT / 'benchmarks/cache-handoff-selection' / mode
    assert json.loads((source / 'manifest.json').read_text()).get('complete'), source
    for path in sorted(source.iterdir()):
        if path.is_file() and path.suffix in extensions:
            copy(path, out / mode / path.name)
source = ROOT / 'coordination/cache-handoff-selection'
for path in sorted(source.iterdir()):
    if path.is_file() and path.suffix in extensions:
        copy(path, out / 'provenance' / path.name)
for name in ('handoff_selection_compare.py', 'build_handoff_selection.py',
             'run_handoff_selection.py', 'archive_handoff_selection.py', 'handoff_selection_point8.py',
             'run_job.py', 'build_kvbench.py'):
    copy(ROOT / 'coordination' / name, out / 'scripts' / name)
(out / 'archive-inventory.json').write_text(json.dumps(inventory, indent=2) + '\n')
(out / '.gitattributes').write_text('*.csv whitespace=cr-at-eol\n*.patch -whitespace\n')
for item in inventory:
    assert hashlib.sha256((out / item['archived']).read_bytes()).hexdigest() == item['sha256']
print(out, len(inventory), 'artifacts')
