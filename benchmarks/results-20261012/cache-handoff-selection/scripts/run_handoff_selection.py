#!/usr/bin/env python3
"""Run both selected-handoff guard controls under the campaign's outer lock."""
import json
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parent.parent
out = ROOT / 'coordination/cache-handoff-selection'
records = []
for mode in ('resident', 'disk'):
    command = ['python3', str(ROOT / 'coordination/handoff_selection_compare.py'), mode]
    record = dict(mode=mode, command=command, started=time.time())
    with (out / f'{mode}-benchmark.log').open('w') as stream:
        process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            stream.write(line)
            stream.flush()
            print(line, end='', flush=True)
        record.update(returncode=process.wait(), finished=time.time())
    records.append(record)
    (out / 'campaigns.json').write_text(json.dumps(records, indent=2) + '\n')
    assert record['returncode'] == 0, record
