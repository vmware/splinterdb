#!/usr/bin/env python3
"""Link the frozen query harness to the selected handoff release library."""
import hashlib
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parent.parent
tree = ROOT / 'worktrees/cache-handoff'
out = ROOT / 'coordination/cache-handoff-selection'
out.mkdir(exist_ok=True)
binary = tree / 'build-selected/release/bin/kvbench'
assert not binary.exists(), 'Refusing to overwrite a measured harness'
subprocess.run(['python3', str(ROOT / 'coordination/build_kvbench.py'),
                str(tree), '--build-root', 'build-selected'], check=True)
records = {}
configurations = {}
for label, build in [('before', 'build-prefetch'), ('selected', 'build-selected')]:
    executable = tree / build / 'release/bin/kvbench'
    library = tree / build / 'release/lib/libsplinterdb.a'
    record = json.loads(executable.with_suffix('.json').read_text())
    assert record['binary_sha256'] == hashlib.sha256(executable.read_bytes()).hexdigest()
    record['library_sha256'] = hashlib.sha256(library.read_bytes()).hexdigest()
    configs = list((tree / build / 'release').glob('build-config.*'))
    assert len(configs) == 1, configs
    config = configs[0].read_text()
    configurations[label] = [line for line in config.splitlines()
                             if line.startswith(('CFLAGS =', 'LDFLAGS =', 'LIBS =', 'CC ='))]
    (out / f'{label}-build-config.txt').write_text(config)
    symbols = subprocess.check_output(['nm', '-S', '--size-sort', str(executable)], text=True)
    (out / f'{label}-symbols.txt').write_text(symbols)
    (out / f'{label}-cache-get-disassembly.txt').write_text(subprocess.check_output(
        ['objdump', '-drwC', '--disassemble=clockcache_get_virtual', str(executable)], text=True))
    records[label] = record
assert configurations['before'] == configurations['selected'], configurations
assert records['before']['source_sha256'] == records['selected']['source_sha256']
assert records['before']['binary_sha256'] == 'faaaaee52d118c8aa49d2bade20632b3776dbf848da6a14921f729ac85a830d2'
assert subprocess.check_output(['git', 'diff', '65b9123', 'f0641f9', '--',
                                'src', 'include', 'Makefile'], cwd=tree) == b''
patch = subprocess.check_output(['git', 'diff', '--binary', 'HEAD', '--',
                                 'src', 'include', 'Makefile', 'tests/functional/cache_test.c'], cwd=tree)
(out / 'measured-guard-fix.patch').write_bytes(patch)
files = [tree / 'Makefile']
for directory in ('src', 'include'):
    files.extend(path for path in sorted((tree / directory).rglob('*')) if path.is_file())
metadata = dict(before_source='65b9123c237ed27a95e27179564c7ef21dd162fd',
                selected_parent=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=tree, text=True).strip(),
                before_equals_selected_parent_runtime=True, builds=records,
                measured_patch_sha256=hashlib.sha256(patch).hexdigest(),
                source_files_sha256={str(path.relative_to(tree)): hashlib.sha256(path.read_bytes()).hexdigest()
                                     for path in files})
(out / 'builds.json').write_text(json.dumps(metadata, indent=2) + '\n')
(out / 'build_handoff_selection.py').write_bytes(Path(__file__).read_bytes())
print(out, flush=True)
