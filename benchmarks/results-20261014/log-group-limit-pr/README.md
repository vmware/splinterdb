# Log-group default and PR validation

The candidate is `robj/log-group-limit` at parent `dd0d59c5` plus
[`default1024.patch`](default1024.patch). Main is `fc0bc8f7`, already included
in the branch. The only runtime change in this patch is selecting 1024 pages
in both default-setting paths through a shared constant. The public config
test asserts the effective default; the flat-blob recovery fixture now exceeds
1024 pages per insert. Earlier explicitly selected 1024-page measurements
remain in the 2026-10-13 asynchronous-policy archive.

## Correctness and build checks

Debug and release builds pass. Each mode passes 44 core cases, 56 public API
cases and one process-exit regression with three data shapes, followed by the
functional log suite replaying 500,000/500,000 records. The process-exit fixture
drains already-submitted work, exits without close or an explicit barrier, and
verifies every value after reopening. It covers small records, flat blobs and
multilevel blobs. Fault tests cover submission failure, durability failure and
retry, and automatic barriers overlapping explicit barriers without making
inserts wait for the earlier device barrier.

Raw logs, commands, binary hashes and counts are in [`tests/`](tests).
`run_validation.py` records the four commands for each mode. Build logs use
`make -j4 BUILD_ROOT=build-main-fc0bc8f7 BUILD_MODE=debug|release all`, adding
`benchmarks` for release. Full source formatting and the final diff whitespace
checks pass. The full CI compiler/sanitizer/stress matrix was not run here.

## Fresh benchmark screen

Twelve ten-second closed-loop insertion runs use CPU affinity 0-11, a 256 MiB
cache, a 16 GiB logical database, 8 MiB memtables, one memtable worker, three
normal workers, and 16-byte keys and values. WAL is enabled; automatic
checkpoints and explicit periodic barriers are disabled. All databases are
opened with `O_DIRECT` on ext4 `/dev/mapper/snafu-root_crypt`. Campaign locks
serialize benchmarks and exclude concurrent campaign builds/tests; the
serialization audit found no overlap. Successful scratch databases are deleted.

For each of one and eight writers, the order is main, default, explicit 1024,
explicit 1024, default, main. Candidate default means the API config contains
zero. Main has no size-triggered barrier policy. Both use the same frozen
insertion harness source, with the main-only compatibility macro; the harness
snapshot retains its historical help text. It does not define the default.
`build_benchmarks.py` records commands and library/source/binary hashes in the
two build manifests. The original main executable is verified and reused.

| Writers | Policy | Inserts/s | Change vs main | Including close |
| ---: | --- | ---: | ---: | ---: |
| 1 | Main | 863,309 | 0.00% | 0.00% |
| 1 | Default 1024 | 860,522 | -0.32% | -0.08% |
| 1 | Explicit 1024 | 861,242 | -0.24% | -0.10% |
| 8 | Main | 1,825,188 | 0.00% | 0.00% |
| 8 | Default 1024 | 1,810,417 | -0.81% | -2.02% |
| 8 | Explicit 1024 | 1,824,948 | -0.01% | -2.26% |

Cells are arithmetic means of two samples. Including-close throughput divides
each run's inserts by its insertion duration plus close duration before
averaging. Eight-writer close times average 1.23 seconds for main, 1.36 for
default and 1.47 for explicit; individual main closes range 0.71-1.74 seconds.
With only two short samples, these differences do not establish exact overhead
or tail-latency neutrality. No substantial insertion-throughput regression is
apparent. Earlier larger policy comparisons and their control-drift caveats
remain relevant. The guarantee differs from main: added automatic durability
has real storage work to perform.

All runs completed with positive insertion counts, successful API returns and
clean close, with no automatic-barrier warning. The benchmark does not read
back every key; separate tests validate correctness and recovery. Raw time
series, latency summaries, commands and manifests are in
[`performance/`](performance); [`summary.json`](summary.json) also reports
CPU cost and latency percentiles. No timings or samples were discarded.

## Reproduction

`build_benchmarks.py`, `run_benchmarks.py`, `runner_core.py` and
`insert_benchmark.c` preserve the executed sources. They refer to this
campaign's absolute paths and verify input hashes before each run. Adjust
paths and use a new output directory for a new campaign; existing result
directories and database files are deliberately refused.

Invoke the benchmark through the campaign coordinator:

```sh
python3 .cache/projects-20261011/coordination/run_job.py \
  --name log-pr-1024-default-screen \
  --db-dir /home/rob/projects/splinterdb/.cache/projects-20261011/benchmarks/log-pr-1024-20261014 \
  benchmark -- python3 .cache/projects-20261011/coordination/log-pr-1024-20261014/run_benchmarks.py
```

`sha256.json` covers the archived files other than itself. Original binary
hashes are in manifests; binaries and database images are not committed.
