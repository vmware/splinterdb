#!/usr/bin/env python3
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

tree = Path('/home/rob/projects/splinterdb/.cache/projects-20261011/worktrees/cache-invalid-addresses')
out = tree / 'benchmarks/results-20261017/cache-invalid-addresses-main-8e0f29b8'
manifest = json.loads((out / 'validation.json').read_text())
assert manifest['passed'] and manifest['source_files_unchanged']
assert all(x['returncode'] == 0 for x in manifest['commands'])
passed = sum(x.get('passed', 0) for x in manifest['commands'])
skipped = sum(x.get('skipped', 0) for x in manifest['commands'])
assert (passed, skipped) == (303, 6)
assert 'unpublished prefetch admission race passed' in (out / 'debug-prefetch-admission.log').read_text()

def git(*args):
    return subprocess.check_output(['git', *args], cwd=tree)

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

extension = ['src/cache.h', 'src/clockcache.c', 'tests/unit/writeback_set_test.c']
extension_identical = {p: (tree / p).read_bytes() == git('show', manifest['base_commit'] + ':' + p) for p in extension}
assert all(extension_identical.values())
runtime_diff = git('diff', '--name-only', manifest['merged_main'], '--', 'src', 'include', 'tests', 'Makefile', 'format-check.sh').decode().splitlines()
assert sorted(runtime_diff) == sorted(extension)
summary = {
    'starting_head': manifest['base_commit'],
    'merged_main': manifest['merged_main'],
    'conflicts': [],
    'passed_unit_executions': passed,
    'skipped_unit_executions': skipped,
    'skip_reason': 'Two preexisting memory-fault injection tests in platform_api, skipped in each build because fault injection is disabled.',
    'passed_functional_runs': 1,
    'all_checks_passed': True,
    'extension_source_files_identical_to_start': extension_identical,
    'runtime_test_diff_vs_main': runtime_diff,
    'compiler_versions': {name: subprocess.check_output([name, '--version']).decode().splitlines()[0] for name in ['cc', 'clang-19']},
    'builds': {},
    'benchmarks_rerun': False,
    'source_hashes': 'validation.json',
}
for mode in ['debug', 'release', 'release-msan']:
    base = tree / 'build-main-8e0f29b8' / mode
    configs = list(base.glob('build-config.*'))
    assert len(configs) == 1
    shutil.copy2(configs[0], out / ('build-config-' + mode + '.txt'))
    summary['builds'][mode] = {
        'config': 'build-config-' + mode + '.txt',
        'library_sha256': digest(base / 'lib/libsplinterdb.so'),
        'passed': sum(x.get('passed', 0) for x in manifest['commands'] if x['label'].startswith(mode + '-') and (mode != 'release' or not x['label'].startswith('release-msan-'))),
        'skipped': sum(x.get('skipped', 0) for x in manifest['commands'] if x['label'].startswith(mode + '-') and (mode != 'release' or not x['label'].startswith('release-msan-'))),
    }
(out / 'integration.json').write_text(json.dumps(summary, indent=2) + '\n')
(out / 'README.md').write_text('''# Cache invalid-address checks: main integration, 2026-10-17

Merged main `8e0f29b89598db1d52aa7090404b475001dbf2eb` into
`robj/cache-invalid-addresses`, starting at
`c15f1452b752dd57a505f766b0d2a029348b94e9`. The merge was automatic and had no
conflicts. Main includes the prefix-compression implementation and its MSan
repairs. The project's `cache.h`, `clockcache.c`, and `writeback_set_test.c`
remain byte-for-byte identical to their starting versions; these are the only
runtime/test differences from the merged main.

## Validation

| Check | GCC debug | GCC release | Clang 19 release + MSan |
|---|---:|---:|---:|
| Writeback/cache unit tests | 18 | 18 | 18 |
| Public quick unit tests | 62 | 62 | 62 |
| Platform API unit tests | 11 | 11 | 11 |
| B-tree unit tests | 12 | 12 | — |
| Focused core propagation/version tests | 3 | 3 | — |
| **Passing unit executions** | **106** | **106** | **91** |
| Existing fault-injection-only skips | 2 | 2 | 2 |

All **303 unit executions passed**. The six skipped executions are the same two
preexisting platform memory-fault tests in each build; fault injection was
disabled. The debug deterministic prefetch-admission functional check also
passed. The full maintained-source format check and staged/unstaged Git
whitespace checks passed.

Coverage includes invalid synchronous, nonblocking, asynchronous, and prefetch
addresses; cleared failure handles; absence of I/O for rejected addresses;
cross-thread handle ownership; read-error propagation; and retry behavior.
The quick and platform suites include the newly merged prefix free-gap and
asynchronous-read MSan regressions. The three focused core cases exercise cache
read errors, invalid branch references, and persisted old-version rejection.

Every build and test acquired the shared campaign machine lock exclusively.
Jobs used isolated temporary directories. Test databases were on ext4 backed
by `/dev/mapper/snafu-root_crypt`; each job's disposable database directory was
removed only after successful process exit. The preexisting untracked build
directories were preserved. Production/test source hashes match before and
after validation.

These are integration checks, not new performance measurements. Earlier
benchmark results remain tied to their recorded revisions. The documented
performance uncertainty and inherited asynchronous allocation-progress stall
remain unresolved; this merge changes neither claim.

## Evidence

- [Integration summary](integration.json): revisions, counts, compiler versions,
  library hashes, and exact preservation of the project-specific sources.
- [Validation manifest](validation.json): commands, timing/lock intervals,
  exit statuses, test counts, binary hashes, and source inventories.
- [Validation runner](validate.py), adjacent per-job logs, and build configs.
- [Project extension](invalid-address-extension.patch): current delta from main.
- [Artifact hashes](sha256.json): hashes of this evidence directory.
''')

doc = tree / 'docs/cache-invalid-addresses-results.md'
heading = '## Prefix-compression main integration (2026-10-17)'
assert heading not in doc.read_text()
with doc.open('a') as f:
    f.write('''

## Prefix-compression main integration (2026-10-17)

Merged main `8e0f29b89598db1d52aa7090404b475001dbf2eb` into the active
`robj/cache-invalid-addresses` branch from `c15f1452`. The merge had no conflicts.
The invalid-address implementation and its tests remain byte-for-byte unchanged
in `cache.h`, `clockcache.c`, and `writeback_set_test.c`; all other runtime and
test sources match this main. Main's prefix compression, format-version checks,
and prefix-page/asynchronous-read MemorySanitizer repairs are included.

GCC debug and release each pass **106 unit cases**: 18 writeback/cache,
62 public quick, 11 platform API, 12 B-tree, and three focused core
propagation/version tests. Clang 19 release with MemorySanitizer passes all
18 writeback/cache, 62 public quick, and 11 platform API cases. This is
**303 passing unit executions**, plus the passing debug deterministic
prefetch-admission functional check. Two existing memory-fault-only platform
tests are skipped in each build because fault injection is disabled (six
skipped executions). The full maintained-source formatting check and both Git
whitespace checks pass.

Builds and tests were serialized with the campaign's exclusive machine lock,
used separate temporary directories, and kept test databases on real ext4
storage. Source hashes were unchanged throughout validation. Commands, logs,
counts, compiler settings, binary hashes, and source inventories are in
[the integration evidence](../benchmarks/results-20261017/cache-invalid-addresses-main-8e0f29b8/README.md).

No benchmarks were rerun for this integration. Earlier performance numbers
remain specific to their recorded commits; the previously documented
performance uncertainty and inherited asynchronous allocation-progress stall
remain unresolved. Existing untracked build directories were preserved.
''')

shutil.copy2(__file__, out / 'report.py')
hashes = {str(p.relative_to(out)): digest(p) for p in sorted(out.rglob('*')) if p.is_file() and p.name != 'sha256.json'}
(out / 'sha256.json').write_text(json.dumps(hashes, indent=2) + '\n')
print(json.dumps({'unit_passed': passed, 'unit_skipped': skipped, 'functional_passed': 1, 'files': len(hashes), 'evidence': str(out)}, indent=2))
