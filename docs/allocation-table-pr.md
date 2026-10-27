# Replace dense allocation counts with fixed four-bit counters

The allocation table now uses four-bit counters and a sharded hash table for
exact unsigned 64-bit overflow counts. This replaces the dense implementation
completely: there is no allocator backend or counter-width build/runtime option.
At 1 TiB configured capacity with 128 KiB extents, the array shrinks from 32 MiB
to 4 MiB, plus container and overflow storage.

A mini allocator adopts its initial head owner, so singly owned branches and
filters remain at `AL_ONE_REF`. Retain APIs return status; callers acquire before
publication and unwind successful prefixes on failure. Existing public API
signatures carry these errors without a signature change.

Allocator format **3** persists four-bit tags and a linked list of sorted
`(extent_number, uint64_raw_count)` pairs at clean shutdown. The disk encoding
is independent of hash buckets. Failure to allocate/write the snapshot leaves
recovery to rebuild allocation state. Loaded chain storage is reclaimed only
after durable invalidation. This does not add concurrent allocation checkpoints.
**Allocator formats 1 and 2 are rejected; no migration reader is provided.**

Validation passed 606 integration cases, 128 focused allocator/container cases
(with six optional-interposer skips), and 32 untimed microbenchmark correctness
cases. Fresh ordinary builds cover debug, release and ASan on private/shared
heaps; focused allocator/container ASan+LSan also passed. All debug test targets
and tools compile.

The final source completed 48 controlled macro runs against main `6e550e1e`,
with 720M inserts and reopened scans. Paired insertion estimates range from
−1.37% to +4.45%; every 95% interval crosses zero. The intervals do not establish
±3% equivalence in every case, and clean reopen medians increased by about
17–112 ms. No throughput speedup or universal neutrality is claimed. Measurements
used exclusive coordinated locks with all other agents idle, but clocks were not
fixed and sampled temperatures reached about 99°C. Full results,
confidence intervals, workload limits and validation digests are in
[the final report](../benchmarks/allocation_table/FINAL_RESULTS.md).
