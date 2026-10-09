# Random insertion and point-query benchmark

The initial investigation and measured results are in
[insert_spikes.md](insert_spikes.md).

Build with `make benchmarks` (use the same compiler as the rest of your build).
The defaults reproduce an insertion-spike investigation: 8 inserters, 8 point
query threads, 16-byte keys + 16-byte values, a 3 GiB Splinter cache, 1 memtable
background thread, 4 normal background threads, logging enabled, automatic
checkpoints disabled, and 60 GiB of logical key+value data. The memtable size is
24 MiB and the foreground task queue threshold is 100 percent.

```sh
mkdir -p .cache/insert-run
build/release/bin/insert_benchmark \
  --db .cache/insert-run/image.db \
  --csv .cache/insert-run/throughput.csv \
  > .cache/insert-run/run.log 2>&1
python3 benchmarks/analyze_insert.py .cache/insert-run/throughput.csv \
  --json .cache/insert-run/analysis.json \
  --plot .cache/insert-run/throughput.png
```

Plotting requires Matplotlib; JSON analysis uses only the Python standard
library. `--help` lists all workload controls. A smaller reproduction uses
`--data-gib 6 --disk-gib 64`. Existing output files are refused. The default
512 GiB image capacity is sparse, but a 60 GiB load with unreclaimed logging
needs substantially more than 60 GiB of physical storage. The initial 6 GiB
run used approximately 25 GiB. The sampler requests a stop when free space
falls below 8 GiB; this is a periodic guard, not a hard reservation, and final
drain/close can consume additional space. Choose a scratch filesystem with
sufficient headroom. Images can be removed after analysis; retain CSVs/logs.

Keys are unique, deterministically scrambled 64-bit IDs padded to the requested
key size, so inserts have random key order. Values contain fixed bytes. By
default a reader randomly chooses an inserter, then a key that inserter has
completed; expected hit rate is 100%. Use `--query-range all` for queries over
the eventual key set, including keys not inserted yet. The default uses buffered
I/O, so the filesystem page cache is additional memory beyond Splinter's cache.
Use `--direct` to measure direct I/O. Record bytes exclude database encoding,
indexes and log overhead. These are assumptions to match to the original run.

The main thread samples once per second independently of worker progress;
`--report-ms` changes this interval. CSVs contain insertion/query rates, hit
percentage, per-thread rates, maximum completed-operation latency, oldest
currently active operation age, process CPU consumption in cores, peak RSS,
process storage read/write bytes, sparse image length, available filesystem
space and the number of inserters still running. Storage byte counters come
from `/proc/self/io`; they do not measure all device activity. Timestamps and
latencies use the monotonic clock. Interval hit percentages can have a tiny
sampling skew because counts are sampled independently.

Creation and final drain/close are outside load measurements. Workers have fixed
per-thread insertion quotas; exclude the completion tail when comparing rates.
The analyzer excludes that tail and the first five seconds, then marks samples
below half the median of up to five neighboring samples on either side. This
is a descriptive dip detector, not a significance test. Interrupts, space
limits, incomplete fixed-count loads and operation errors return a nonzero
status. Reaching an explicitly configured time limit is successful.

## Open-loop arrivals and paired reports

`--target-insert-ops-s RATE --seconds SECONDS` changes the inserters to a paced
open-loop workload. A shared arrival sequence assigns each scheduled request to
the next available worker. The optional `--summary FILE.csv` records aggregate
service and response-latency percentiles. Service latency measures only the
library call; response latency starts at the scheduled arrival and includes any
backlog caused by a system-wide stall. This distinction avoids coordinated
omission. Paced mode currently requires zero query threads.

`--open --id-base N` opens an existing clean image and inserts new logical IDs
starting at N. The comparison runner uses this to give every point on a
throughput/latency curve the same preloaded database state. A run stopped by its
configured `--seconds` limit is successful; fixed-count runs still fail if they
do not insert every requested record.

The reusable branch-versus-main runner and standard-library SVG/HTML report
generator are documented in [comparison/README.md](comparison/README.md).

## Internal traces

Add `--trace .cache/insert-run/trace.csv` to record tasks, exceptional memtable
waits, trunk incorporation preparation and leaf pivot selection. Save symbols
from the **exact binary used for that run**:

```sh
nm -an build/release/bin/insert_benchmark > .cache/insert-run/symbols.txt
python3 benchmarks/analyze_insert.py .cache/insert-run/throughput.csv \
  --trace .cache/insert-run/trace.csv --log .cache/insert-run/run.log \
  --symbols .cache/insert-run/symbols.txt \
  --json .cache/insert-run/analysis.json --plot .cache/insert-run/throughput.png
```

The process-wide trace is disabled unless explicitly opened. Open it before
starting database workers and close it after database shutdown. Records are
buffered and written when an event completes, so a killed process may have an
incomplete trace. Tracing adds clock reads and serialized output at these sites;
keep an untraced baseline. It does not change scheduling or flushing decisions.

`start_ns,duration_ns,tid,event,arg0,arg1` records use the same clock as
`start_monotonic_ns` in the run log. Worker metadata maps library thread IDs to
benchmark workers. For `task_fg`/`task_bg`, arguments are task group (1=memtable,
2=normal) and callback address; the analyzer resolves ASLR using `main_address`.
For `memtable_wait`, arg0 is the retry count; for `trunk_prepare`, the memtable
generation; for `leaf_pivots`, arguments are iterator height and entries scanned.
Nested events overlap: do not add memtable waits, tasks and trunk preparation
times together as if they were independent.

`--queue-scale 18446744073709551615` is a diagnostic control that disables
ordinary opportunistic foreground task assistance when background workers exist.
Memtable exhaustion can still force foreground assistance. This changes the
workload's scheduling and may delay background work; it is not a proposed fix.
