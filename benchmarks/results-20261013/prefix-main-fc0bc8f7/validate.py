#!/usr/bin/env python3
"""Run after run_job.py acquires the campaign test lock."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

mode = sys.argv[1]
tree = Path.cwd()
archive = tree / 'benchmarks/results-20261013/prefix-main-fc0bc8f7'
scratch = tree.parents[1] / 'coordination/prefix-main-fc0bc8f7' / ('scratch-' + mode)
scratch.mkdir(parents=True, exist_ok=True)
build = tree / 'build-main-fc0bc8f7'
link = scratch / build.name
if not link.exists():
    link.symlink_to(build, target_is_directory=True)
unit = build / mode / 'bin/unit'
records = []
for name in ['btree', 'superblock', 'merge', 'splinterdb_quick', 'splinter']:
    binary = unit / (name + '_test')
    command = [str(binary)]
    if name == 'splinter':
        command += ['--db-location', str(scratch / 'core.db'), '--cache-capacity-mib', '128', '--db-capacity-gib', '64']
    record = dict(name=name, command=command, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), started=time.time())
    log = archive / (mode + '-' + name + '.log')
    with log.open('w') as output:
        result = subprocess.run(command, cwd=scratch, stdout=output, stderr=subprocess.STDOUT)
    content = log.read_text()
    record.update(returncode=result.returncode, finished=time.time(), log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(), summaries=[line for line in content.splitlines() if 'RESULTS:' in line])
    records.append(record)
    (archive / (mode + '-results.json')).write_text(json.dumps(records, indent=2) + '\n')
    print(name, result.returncode, record['summaries'], flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
    assert re.search(r'RESULTS: [1-9][0-9]* tests', content)
