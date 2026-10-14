Fresh integration screen on main fc0bc8f7

26 serialized O_DIRECT real-disk invocations:5 fixture loads plus21 query runs.
Databases:ext4 /dev/mapper/snafu-root_crypt. CPU0-11,200000 records,128MiB cache,
3-second measured query phases,8threads. Two samples per candidate;main controls
bookend/interleave. Values and range ordering validated, zero errors/violations.
This resident screen is not a full performance matrix or tail-neutrality claim.

Alternative cache directories retain substantial point/short-range costs:
scalar/SIMD point around25% slower;atomic point6.25%,short scans9.10% slower.
Invalid-address results overlap main ranges (point-0.25%,short-4.39%).
Prefix common-key point/short scans about2% higher;long scans lower with wide
overlapping ranges. Random-key results also overlap main. Preserve earlier
workload-dependent caveats;this screen does not isolate the cost of merging.

Each build record hashes compiled source,library,harness,executable and config,
and verified_runtime_commit identifies the committed runtime exactly. The head
field may precede commit because builds occurred before evidence was committed.
Runtime hash checks against committed source all passed. Archive omits binaries
and databases;original commands/paths are retained.

The executed driver is run_benchmarks-screen.py (hash in runs/manifest.json).
It imports imported-run_matrix.py from the recorded campaign location. Later
build helper records include build-config metadata. PREFIX_PATTERN and disabled
state were audited from every prefix harness log;compression_disabled=false.
