Automatic asynchronous log barriers: performance on main fc0bc8f7

Main=fc0bc8f7; synchronous policy=9bc5ebf4; asynchronous policy=72a1044e.
Identical frozen insertion harness, matched GCC14 release flags, CPU0-11.
O_DIRECT on ext4 /dev/mapper/snafu-root_crypt; all benchmarks serialized and
excluded builds/tests. 42 matrix runs:three8-second samples per candidate
at1/8 writers with six interleaved main controls per thread count.
Eight additional12-second ABBA runs investigate the disabled policy.
All50 insertion runs passed API-return/count/clean-close checks. Recovery
validation is separate in the parent directory; the insertion harness does
not itself read back every inserted key.

64-page async versus sync throughput improves39.92% with1writer and32.62%
with8writers. Including shutdown drain retains39.3%/32.7% gains. Mean
async64 throughput versus main is-2.74%/-9.43%; async1024 is-1.26%/+0.60%.
These are short samples on one machine, not precise universal costs.
Main does not perform size-triggered durability, so it is a throughput
reference with weaker automatic durability, not an equivalent policy.

Control drift limits small comparisons. Disabled policy is-0.40%/-3.51%
in the main matrix. In the follow-up main falls from2.178M to1.515Mops/s.
The two ABBA rounds report-6.95% and-0.05%; retain both. The second round
has stable controls, but we do not attribute all first-round variation or
claim an exactly zero disabled-policy cost.

The gains cannot be explained only by deferred shutdown work: async mean
close time is0.171s/0.276s, versus sync0.139s/0.285s at64pages. Existing
queue assistance can still execute durability on an inserting thread.
No new scheduler, coalescer, or retry engine is present. The64-page default
is retained;1024 is an existing configuration choice evaluated for sensitivity.

Source/library/binary/log hashes verified. verified_runtime_commit identifies
exact committed runtime even if the build record head precedes local commit.
The source snapshots of main/sync came from git archive (see source commit
and commands in build records). Binaries/databases are omitted from archive.
Scripts retain original campaign paths; imported helper is archived alongside
the runner. benchmark-audit.json also covers the26 separate branch-screen
invocations, whose raw evidence is on the three other active branches.
