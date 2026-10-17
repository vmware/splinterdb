# Controlled production-cache probe v3

This version derives from frozen v2, adding explicit per-worker CPU affinity,
configurable warmup, and measured worker CPU time. It leaves v1, v2 and the
root runner unchanged. The batch loop and production library are unchanged.
Use a single new executable for all compared libraries; never mix v1/v2/v3
throughput results as though their generated worker code were identical.

## Why another control is needed

The existing suites establish dynamic instruction changes more convincingly
than small throughput differences:

| API | Original extra instructions/hit vs main | sequential | isolated-miss |
| --- | ---: | ---: | ---: |
| Blocking | +4 | +4 | +3 |
| Async | +8 | -1 | +4 |

These deltas repeat for independent and dependent access. The original sync
path also retires one fewer branch/hit, while async branch counts match main.
Branch misses remain approximately 0.017/op in all one-worker samples, so
misprediction is not an apparent major contributor. These counts include the
common consumer loop, but that common overhead cancels in the instruction
difference. Neither identical branch misses nor instruction counts rule out
frontend or memory effects; cycles alone do not prove instruction layout.

The two-sample `isolated-counters` means for isolated-miss versus main are
-0.42% blocking independent, +1.89% blocking dependent, -0.43% async independent,
and +2.34% async dependent. These are screening results. Main's two dependent
blocking samples themselves drift +5.51%; its dependent async samples drift
+4.29%. Original dependent async drifts +4.33%. That variation exceeds the
2–3% regression budget and prevents a defensible tiny-regression conclusion.
The earlier eight-worker main blocking-independent sample changes from 111.0
Mops/s to 96.6 Mops/s (-12.9%). Those workers were allowed to migrate among
logical CPUs 0–11, which have only six physical P-cores. Different sibling
placement is an uncontrolled variable. Early `dispatch-counters` single-worker
main independent samples also have an effective active frequency proxy rising
from about 3.43 GHz to 3.79 GHz; comparing raw cycle costs without frequency
context can be misleading for memory-latency-heavy loops.

The outlined variant has a much larger blocking hit loss in the initial suite
(about 14% independent and 18% dependent versus main) and deserves rejection
unless a separately demonstrated benefit warrants it. `mask-original` does
not reduce blocking retired instructions, and its async reduction is only one
instruction. `sequential` and `isolated-miss` are the useful next candidates.
All arithmetic summaries are saved in `preliminary-analysis.json`; they are
not confidence intervals or independent replication evidence.

## Source controls

Required environment variables:

- `CACHE_PROBE_WORKER_CPUS`: ordered comma-separated logical CPU list. Each
  worker pins itself to exactly its assigned CPU before warmup. Duplicate CPUs
  or sharing the controller's logical CPU is rejected. The actual CPU at both
  measured boundaries is checked. For several CLI thread counts, the list must
  contain at least the largest count; each phase uses its first N entries.
- `CACHE_PROBE_CONTROL_CPU`: logical CPU for setup and the control thread.
- `CACHE_PROBE_WARMUP_SECONDS`: positive warmup duration, default five seconds.
- `CACHE_PROBE_PERF`: v2's `0`/`1` switch, enabled by default. The pinned hardware
  group rejects multiplexing instead of scaling counts.

No affinity, frequency, governor, boost, turbo, power or cooling policy is
changed outside the benchmark process. The runner reads diagnostic sysfs
values before and after each complete process; those are not continuously
sampled or exact measured-phase frequency. Raw per-worker counters, CPU time,
identity and CPU assignment are logged as `WORKER` lines. CSV adds warmup,
controller CPU and sum of measured worker CPU time. `perf_cycles / worker_cpu_ns`
is an average user-cycle/worker-CPU-time proxy, not a calibrated frequency
measurement; kernel CPU time and interrupts can still affect it.

The worker's thread CPU timestamps and affinity checks are at measurement
boundaries, outside operation batches. Starting/stopping counters and timing
still include a small constant boundary cost. Default async runs require
zero asynchronous waits, and resident/miss runs require zero measured storage
I/O and major faults, no identity/status errors, and final successful eviction.

## Runner

Build with this directory's `build_probe.py`, using the same main headers and
main shared library used for v2. The build is left for root's scheduled slot.
One source/object/binary and immutable per-variant shared libraries are used
throughout. `run_controlled.py` acquires `machine.lock` for its complete suite,
uses distinct job and scratch paths, checks library selection with `ldd`, logs
all commands/hashes, and removes only its own successful run's database file.
It does not perform builds or mutate production sources.

Default screen:

```sh
python3 .cache/invalid-address-perf-20261017/harness/controlled/run_controlled.py controlled-screen
```

This compares main, original, sequential and isolated-miss. Each scenario uses
three rotating mirrored blocks, giving six samples/profile. A block with order
A B C D runs A B C D D C B A; the next runs B C D A A D C B. Four blocks give
complete balance across positions; three are a shorter screen. Scenario order
also rotates between blocks. Each run measures one access mode only, so a
variant's dependent mode cannot consistently inherit the independent mode's
immediately preceding workload. Defaults are warmup two seconds, measurement
four seconds, APIs blocking and async, one and eight workers, independent and
dependent accesses. This is 192 runs, at least 19.2 minutes plus setup/teardown.
It is a screen, not the final confirmation of a 2–3% effect.

The one-worker CPU is 2. Eight workers use 0,2,4,6,8,10,1,3: six physical
P-cores with two explicitly selected SMT siblings. The controller uses CPU12,
an E-core. Perf defaults to single-worker cases only, avoiding PMU group
competition between SMT siblings. `--threads 6` uses 0,2,4,6,8,10 and is the
cleaner physical-core scaling measurement. All sets are configurable; the
manifest records physical core IDs and sibling topology.

Nonblocking confirmation before accepting a candidate:

```sh
python3 .cache/invalid-address-perf-20261017/harness/controlled/run_controlled.py controlled-nonblocking \
  --profiles main original sequential isolated-miss --apis nonblocking \
  --threads 1 --modes independent dependent miss --blocks 4
```

`miss` retains allocated but evicted extents, expects STATUS_BUSY and a null
handle and performs no timed demand I/O. Main and candidate therefore have
matching specified behavior. Invalid/unallocated addresses belong in
correctness tests because baseline behavior differs. Allocation validation
on a miss may add real unavoidable work; measure and report it separately
from resident-hit costs rather than weakening validation.

For stronger confirmation, select a candidate and run main/original/candidate
with `--blocks 6 --warmup 5 --seconds 10`, first on one worker and then on six
and fixed-placement eight workers. Keep the same executable and build options.
Use block-level candidate/main log ratios, with mirror-pair means inside each
block, rather than treating adjacent samples as independent. Publish each
block ratio and its spread; if intervals still straddle the 2–3% threshold,
collect more balanced blocks instead of declaring neutrality. Check whether
late blocks reproduce early ones and use thermal/frequency diagnostics to
explain shifts without selecting only favorable samples. Any thermal-throttle
counter increase is recorded on the sample; if it makes a block unsuitable,
exclude/rerun the entire balanced block with the reason retained.

`--resume` verifies unchanged arguments, runner, executable and libraries and
skips completed rows. It deliberately refuses a partially started job whose
scratch path already exists: inspect and retain that failed job before choosing
a new label. No silent overwrite or automatic deletion of interrupted evidence.
