# Final controlled-cache runner

`cache_api_probe`, `.c`, `.o`, its build `.json`, and `build_probe.py` are
byte-for-byte copies of frozen `../controlled-v3b/`. No recompilation or C
instrumentation change was made. `provenance.json` records each copied hash.
The only executable behavior changed is Python scheduling in
`run_controlled.py`.

The runner completes every mirrored block for one API / thread count / access
mode before proceeding to the next scenario. Before each scenario it runs one
predetermined excluded preconditioning process with the same binary and CPU
assignment, main's shared library, a 30-second warmup and a one-second measured
phase. This process establishes the workload's package-temperature regime;
its result is never included in the comparison. It is excluded by design,
regardless of speed. Measured blocks retain the v3b names, columns, two-second
warmup and four-second phases by default. Profiles and all durations remain
configurable.

A three-profile final matrix can be invoked with:

```sh
python3 .cache/invalid-address-perf-20261017/harness/controlled-final/run_controlled.py final-hits \
  --profiles main original CANDIDATE --blocks 3 --apis blocking async \
  --threads 1 8 --modes independent dependent
```

Replace `CANDIDATE` with the frozen candidate build's profile name. Use a new
label for every distinct matrix. The separate nonblocking matrix can use
`--apis nonblocking --threads 1 --modes independent dependent miss`.

All processes, including preconditions, hold the same suite machine lock.
`manifest.json` records `scenario_order` and separate `precondition_runs`.
Each excluded record includes the normal job record, telemetry, workers,
profile, scenario index, scenario tuple, `excluded_from_results: true`, and
its raw CSV row. Preconditions never enter `results.csv` or measured `runs`.
Lock intervals are recorded even on exceptions. A resumed partial scenario
gets a fresh precondition with a new attempt suffix; completed scenarios are
skipped. Interrupted scratch/jobs are preserved and not silently overwritten.

No sample is automatically discarded for thermal drift. Raw throttle counter
increases, frequency/temperature snapshots and individual results remain
available for the report's interpretation. These controls reduce transitions
between different package-power regimes; they do not guarantee a fixed CPU
frequency or eliminate all machine noise.
