#!/usr/bin/env python3
"""Prepare or apply the scoped project-ledger update after the performance commit.

Default: validate the explicit --commit and print a unified diff without writes.
Add --apply to update the two central tracking files and save before/after copies
and a receipt beside this script. No commits, pushes, builds or workloads run.
All saved preimages and selected-source identities must still match exactly.
"""
import argparse
import copy
import difflib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent
CENTRAL = Path('/home/rob/projects/splinterdb/.cache/projects-20261011')
TARGET = CENTRAL / 'worktrees/cache-invalid-addresses'
PROJECTS = CENTRAL / 'coordination/projects.json'
REPORT = CENTRAL / 'REPORT.md'
LOCK = CENTRAL / 'coordination/machine.lock'
BASE = 'da5757265109f3a5936e0ef20daff6a955f775a3'
BRANCH = 'robj/cache-invalid-addresses'
SOURCE_SHA = '7f370079f60ccb1b86744a209c20dd09d0b729d512bdf3e022e04b51d23a777a'
LIBRARY_SHA = 'c7b04dabd8a600500ead9095a04e0a572baf904e256f077043c7191abdd8d228'
PREIMAGES = {
    PROJECTS: 'b3415f62632026ef5d4d99b10a28a6ba72a88fcf31dcadbab7cb6b3a9451a4bc',
    REPORT: 'eb3418c94a88d3324d9e0180ae53c64e4e652bdaebaadbd53fffe73c798e38bf',
}
HISTORY_MARKER = b'## Historical record before 2026-10-17'
ARCHIVE_REL = Path('benchmarks/results-20261017/cache-invalid-addresses-performance')
ARCHIVE_LINK = 'worktrees/cache-invalid-addresses/' + str(ARCHIVE_REL)

PROGRESS = (
    'Local performance follow-up retains main 8e0f29b8 and invalid-address rejection. '
    'Cold diagnostics, async miss validation placement and masked alignment remove avoidable cache costs. '
    'The exact selected source passed 124 targeted unit executions plus debug prefetch admission and format checks; this is not full CI. '
    'Final resident-cache mean throughput losses versus main are at most 1.08%; independent async hits improve about 2% versus the prior branch. '
    'The completed 32-page resident cache-write matrix has mean changes from -0.16% to +0.40% versus main across one/eight workers and disjoint/shared access; this does not establish database insertion neutrality. '
    'Valid nonblocking misses improve 17.84% versus the prior branch but remain 29.83% slower than main because allocator membership must be checked. '
    'Resident database point/range throughput changes -1.51%/-0.03% versus main. '
    'No-WAL through-close insertion remains indeterminate at -4.09% with a wide, sign-changing block interval; WAL insertion is -1.28%. '
    'These measurements do not establish a universal 3% regression bound. '
    'The inherited async batch-reservation/admission-progress limitation remains unresolved. '
    'Raw evidence and prior adverse cohorts are preserved. Local commit only; user handles pushes and PRs.'
)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def git(*arguments):
    return subprocess.check_output(['git', '-C', str(TARGET), *arguments])


def render_json(value):
    return (json.dumps(value, indent=2, ensure_ascii=True) + '\n').encode()


def verify_delivery(commit):
    assert re.fullmatch(r'[0-9a-f]{40}', commit), '--commit requires the full lowercase commit hash'
    assert commit != BASE, 'performance commit has not been created'
    assert git('branch', '--show-current').decode().strip() == BRANCH
    assert git('rev-parse', 'HEAD').decode().strip() == commit, '--commit must equal target HEAD'
    subprocess.run(['git', '-C', str(TARGET), 'merge-base', '--is-ancestor', BASE, commit], check=True)
    assert not git('diff', '--name-only').strip(), 'target has tracked worktree edits'
    assert not git('diff', '--cached', '--name-only').strip(), 'target has staged edits'
    assert sha((TARGET / 'src/clockcache.c').read_bytes()) == SOURCE_SHA
    assert sha(git('show', commit + ':src/clockcache.c')) == SOURCE_SHA
    evidence = json.loads((ROOT / 'delivery-source-check.json').read_text())
    assert evidence['target_branch'] == BRANCH and evidence['base_commit'] == BASE
    assert evidence['selected_profile'] == 'async-mask'
    assert evidence['selected_clockcache_sha256'] == SOURCE_SHA
    assert evidence['validated_source_files_match'] and evidence['checked_files'] == 172
    for name, expected in evidence['source_sha256'].items():
        assert sha((TARGET / name).read_bytes()) == expected, name
        assert sha(git('show', commit + ':' + name)) == expected, name
    archive = TARGET / ARCHIVE_REL
    required = ['README.md', 'SHA256SUMS', 'validation-async-mask/validation.json',
                'database/analysis/mask-final-hits-audited.json',
                'database/analysis/mask-final-hot32-audited.json',
                'database/analysis/mask-final-writes-hot32-audited.json',
                'database/analysis/mask-final-misses-audited.json',
                'database/analysis/mask-final-resident-audited.json',
                'database/analysis/mask-final-quota-audited.json',
                'database/analysis/mask-final-db-conclusion.json',
                'review/mask-churn-audited.json',
                'review/mask-final-writes-evidence-review.json',
                'harness/controlled-writes/validation-status.json']
    recorded = {}
    for line in (archive / 'SHA256SUMS').read_text().splitlines():
        expected, name = line.split('  ', 1)
        relative = Path(name)
        assert not relative.is_absolute() and '..' not in relative.parts
        assert name not in recorded and re.fullmatch('[0-9a-f]{64}', expected)
        recorded[name] = expected
    for name in required:
        data = (archive / name).read_bytes()
        assert git('show', commit + ':' + str(ARCHIVE_REL / name)) == data, name
        if name != 'SHA256SUMS':
            assert sha(data) == recorded[name], name
    validation = json.loads((archive / 'validation-async-mask/validation.json').read_text())
    assert validation['passed'] and validation['sources_unchanged']
    assert validation['passed_unit_executions'] == validation['expected_unit_executions'] == 124
    assert validation['profile']['clockcache_sha256'] == SOURCE_SHA
    resident = json.loads((archive / 'database/analysis/mask-final-resident-audited.json').read_text())
    insertion = json.loads((archive / 'database/analysis/mask-final-quota-audited.json').read_text())['campaigns'][0]
    assert resident['validation']['complete'] and insertion['validation']['complete']
    assert resident['validation']['library_hashes']['async-mask'] == LIBRARY_SHA
    assert insertion['validation']['library_hashes']['async-mask'] == LIBRARY_SHA
    for name, expected_rows in [('mask-final-hot32-audited.json', 36),
                                ('mask-final-writes-hot32-audited.json', 72)]:
        result = json.loads((archive / 'database/analysis' / name).read_text())
        assert result['validation']['complete']
        assert result['validation']['observed_rows'] == result['validation']['expected_rows'] == expected_rows
        assert result['validation']['libraries']['async-mask'] == LIBRARY_SHA
    churn = json.loads((archive / 'review/mask-churn-audited.json').read_text())
    assert churn['passed'] and churn['library_sha256'] == LIBRARY_SHA
    assert churn['source_clockcache_sha256'] == SOURCE_SHA
    return sha((archive / 'SHA256SUMS').read_bytes())


def prepare_projects(original, commit, archive_inventory_sha):
    before = json.loads(original)
    assert render_json(before) == original, 'unexpected existing JSON formatting'
    after = copy.deepcopy(before)
    matches = [p for p in after['followups'] if p['name'] == 'cache-invalid-addresses']
    assert len(matches) == 1
    project = matches[0]
    assert project['branch'] == BRANCH and project['commit'] == BASE
    assert project['worktree'] == str(TARGET) and project['status'] == 'locally_committed'
    assert 'pre_performance_20261017_progress' not in project
    project['pre_performance_20261017_progress'] = project['progress']
    project['pre_performance_20261017_commit'] = project['commit']
    project['commit'] = commit
    project['progress'] = PROGRESS
    project['performance_revalidated'] = True
    project['performance_date'] = '2026-10-17'
    project['performance_evidence'] = ARCHIVE_LINK + '/README.md'
    project['performance_commit'] = commit
    project['performance_selected_clockcache_sha256'] = SOURCE_SHA
    project['performance_limitations'] = [
        'No universal 3% equivalence claim; some resident/query intervals allow larger losses.',
        'Valid nonblocking miss retains required allocation-check cost versus main.',
        'No-WAL insertion wall timing remains indeterminate; CPU/op does not establish wall neutrality.',
        'Inherited async batch-reservation/admission-progress limitation remains unresolved.',
        'Targeted exact-source validation is not full CI; bounded churn is not a general progress proof.',
    ]
    after['deliverable'] = (
        'Two active local branches retain main 8e0f29b8. Cache-invalid-address handling now includes '
        'a locally committed, exact-source-tested performance follow-up and archived cache/database evidence; '
        'no-WAL wall timing and inherited async progress remain unresolved. Cache-directory status and '
        'its experimental alternatives are unchanged. Seven merged project histories and four archived '
        'experiments are preserved. User handles pushes and PRs.'
    )
    after['performance_evidence'] = {
        'project': 'cache-invalid-addresses', 'date': '2026-10-17', 'branch': BRANCH,
        'commit': commit, 'report': ARCHIVE_LINK + '/README.md',
        'archive_sha256sums_sha256': archive_inventory_sha,
        'selected_clockcache_sha256': SOURCE_SHA, 'selected_release_library_sha256': LIBRARY_SHA,
        'targeted_unit_executions': 124,
        'cache_write_evidence': ARCHIVE_LINK + '/database/analysis/mask-final-writes-hot32-audited.json',
        'cache_write_measured_rows': 72,
        'scope': 'Cache CPU attribution and bounded database confirmation; no universal equivalence or progress proof.',
    }
    # Existing integration/history fields, other followups and every project stay exact.
    assert after['projects'] == before['projects']
    for old, new in zip(before['followups'], after['followups']):
        if old['name'] != 'cache-invalid-addresses':
            assert old == new
        else:
            for key in old:
                if key not in {'commit', 'progress', 'performance_revalidated'}:
                    assert old[key] == new[key], key
    for key in before:
        if key not in {'followups', 'deliverable', 'performance_evidence'}:
            assert after[key] == before[key], key
    return render_json(after)


def prepare_report(original, commit):
    assert original.count(HISTORY_MARKER) == 1
    history = original[original.index(HISTORY_MARKER):]
    prefix = f'''# SplinterDB project branches

Current main: `8e0f29b8` (branch prefixes, PR #692). Log-group limits also merged as PR #691 (`6105c49f`).

Both remaining active branches include this main. Cache-invalid-address handling now has a local performance follow-up; cache-directory remains at its existing integration commit. Seven merged project histories and four archived experiments are preserved.

| Active branch | Local commit | Validation |
| --- | --- | --- |
| `robj/cache-directory` | `03dde401` | [838 unit executions + 4 admission checks](worktrees/cache-directory/benchmarks/results-20261017/cache-directory-main-8e0f29b8/README.md) |
| `robj/cache-invalid-addresses` | `{commit[:8]}` | [124 additional exact-source unit executions, debug prefetch admission and formatting]({ARCHIVE_LINK}/validation-async-mask/validation.json) |

Cache-directory validation and status are unchanged: all four implementations passed debug/release integration checks; direct remains the default and alternatives remain experimental. No new cache-directory performance claim is made. The previous invalid-address main integration and its 303 unit executions remain recorded in the [integration report](coordination/main-8e0f29b8/REPORT.md).

The invalid-address follow-up moves cold diagnostics and async miss checks off the resident path and uses a mask for page alignment. Invalid addresses still fail before cache reservation or I/O. The [performance archive]({ARCHIVE_LINK}/README.md) preserves exact sources, commands, hashes, raw results and prior adverse cohorts. Its 124 exact-source unit executions are targeted validation, not full CI.

The final resident-cache matrix's largest mean loss versus main is 1.08%; independent async hits improve about 2% versus the unoptimized branch. A 32-page independent blocking probe improves 8.10% versus main. Valid nonblocking misses improve 17.84% versus the unoptimized branch, while remaining 29.83% slower than main because allocation-membership validation adds required work. These are workload-specific comparisons, not a universal 3% equivalence claim. [Resident-cache evidence]({ARCHIVE_LINK}/database/analysis/mask-final-hits-audited.json) · [Miss evidence]({ARCHIVE_LINK}/database/analysis/mask-final-misses-audited.json)

The completed 32-page resident cache-write matrix covers get/claim/lock/update/unlock/unclaim/unget with one/eight workers and disjoint/shared access. Its 72 measured samples pass identity, protected-counter, placement and zero timed I/O checks. Mean throughput changes versus main range from -0.16% to +0.40%; all four pointwise 95% interval lower bounds are above -3%. This limited cache experiment does not establish database insertion neutrality or explain away the no-WAL timing concern. [Write evidence]({ARCHIVE_LINK}/database/analysis/mask-final-writes-hot32-audited.json)

Final resident database point/long-range throughput changes are -1.51%/-0.03% versus main. No-WAL insertion through close remains indeterminate at -4.09% (95% block interval -24.51% to +21.84%); the WAL estimate is -1.28%. Lower measured CPU/op does not clear the wall-time concern. Some point-query intervals also extend beyond a 3% loss. [Database evidence]({ARCHIVE_LINK}/database/analysis/mask-final-db-conclusion.json)

The inherited async batch-reservation/admission-progress limitation remains unresolved. Successful bounded churn does not establish progress across all pressure and thread-lifecycle interleavings. [Progress scope]({ARCHIVE_LINK}/review/isolated-miss-review.json)

These are local commits. The user handles pushes and PRs.

[Current integration report](coordination/main-8e0f29b8/REPORT.md) · [Current integration audit](coordination/main-8e0f29b8/integration-audit.json) · [Project ledger](coordination/projects.json)

'''.encode()
    updated = prefix + history
    assert updated[updated.index(HISTORY_MARKER):] == history
    return updated


def atomic_replace(path, expected, replacement):
    assert path.read_bytes() == expected, f'preimage changed before write: {path}'
    mode = stat.S_IMODE(path.stat().st_mode)
    descriptor, name = tempfile.mkstemp(prefix='.' + path.name + '.', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(replacement)
            stream.flush()
            os.fsync(stream.fileno())
        assert path.read_bytes() == expected, f'preimage changed while staging: {path}'
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--apply', action='store_true', help='apply after all guards; default only prints diff')
    args = parser.parse_args()
    with LOCK.open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original = {path: path.read_bytes() for path in PREIMAGES}
        assert all(sha(data) == PREIMAGES[path] for path, data in original.items()), 'central tracking preimage changed'
        inventory_sha = verify_delivery(args.commit)
        replacement = {PROJECTS: prepare_projects(original[PROJECTS], args.commit, inventory_sha),
                       REPORT: prepare_report(original[REPORT], args.commit)}
        patch = ''.join(''.join(difflib.unified_diff(original[path].decode().splitlines(True),
                      replacement[path].decode().splitlines(True), fromfile=str(path), tofile=str(path)))
                        for path in [PROJECTS, REPORT])
        if not args.apply:
            print(patch, end='')
            return
        assert all(path.read_bytes() == data for path, data in original.items()), 'central tracking changed during validation'
        backup = ROOT / ('tracking-update-' + args.commit[:12])
        backup.mkdir(exist_ok=False)
        for path in [PROJECTS, REPORT]:
            (backup / ('before-' + path.name)).write_bytes(original[path])
            (backup / ('after-' + path.name)).write_bytes(replacement[path])
        (backup / 'update.patch').write_text(patch)
        changed = []
        try:
            for path in [PROJECTS, REPORT]:
                atomic_replace(path, original[path], replacement[path])
                changed.append(path)
        except BaseException:
            # Roll back only our exact new bytes; never overwrite another edit.
            for path in reversed(changed):
                atomic_replace(path, replacement[path], original[path])
            raise
        receipt = dict(commit=args.commit, branch=BRANCH, selected_clockcache_sha256=SOURCE_SHA,
                       preimages={str(p): sha(v) for p, v in original.items()},
                       postimages={str(p): sha(v) for p, v in replacement.items()},
                       historical_report_tail_preserved=True, cache_directory_unchanged=True,
                       integration_and_history_fields_preserved=True, commits_created=False, pushes=False)
        (backup / 'receipt.json').write_bytes(render_json(receipt))
        assert all(path.read_bytes() == data for path, data in replacement.items())
        print(backup / 'receipt.json')


if __name__ == '__main__':
    main()
