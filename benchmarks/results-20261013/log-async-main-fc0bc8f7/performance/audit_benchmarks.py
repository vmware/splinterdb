import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent.parent/'benchmarks/main-fc0bc8f7'
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
checks = []
for mode, count, driver in [('branch-screen', 26, 'run_benchmarks-screen.py'), ('log-async', 42, 'run_benchmarks-matrix.py'), ('log-disabled-repeat', 8, 'run_benchmarks.py')]:
    folder = OUT/mode
    manifest = json.loads((folder/'manifest.json').read_text())
    assert manifest['complete'] and len(manifest['runs']) == count, mode
    assert manifest['storage'].startswith('ext4 /dev/'), mode
    assert manifest.get('execution_driver_sha256', manifest.get('policy_runner_sha256')) == sha(HERE/driver), mode
    for run in manifest['runs']:
        assert run['verified'] and run['returncode'] == 0, run['label']
        assert sha(folder/(run['label']+'.log')) == run['log_sha256'], run['label']
        profile = json.loads((HERE/(run['profile']+'-build.json')).read_text())
        assert profile['library_sha256'] == run['library_sha256']
        assert sha(profile['library']) == profile['library_sha256']
        assert any(x['binary_sha256'] == run['binary_sha256'] and sha(x['binary']) == x['binary_sha256'] for x in profile['binaries'].values())
        for filename, value in profile['source_files_sha256'].items():
            assert sha(Path(profile['tree'])/filename) == value, (run['profile'], filename)
    checks.append(dict(mode=mode, verified_invocations=count, driver_sha256=sha(HERE/driver)))
result = dict(verified_invocations=sum(c['verified_invocations'] for c in checks), campaigns=checks, serialization=json.loads((HERE.parent/'serialization-audit.json').read_text()))
(HERE/'benchmark-audit.json').write_text(json.dumps(result, indent=2)+'\n')
print('Verified', result['verified_invocations'], 'real-disk invocations, source/library/binary identity and logs.')
