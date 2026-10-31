# Scan4096 profiling after rebasing onto main

Latest scan follow-up: [shared-prefix Scan4096 profiling and retained fix](SHARED_SCAN4096_PROFILE.md)
makes these scans 12.8–13.4% faster than original main across all four key sizes.
The retained fix also removes the intermediate random-scan tradeoff. All 882
runs of the final full matrix verified; custom insertion, overwrite, and hits
retain roughly 2% overhead.

The subsequent [full format-4 matrix](SMALL_KEYS_V4.md) includes a fresh Scan4096
sweep after the additional iterator fixes and compact contiguous statistics.
The diagnosis and measurements below remain the history of this scan fix.

The large inline-key scan regression was caused by a store-to-load forwarding
stall in the code GCC 14 generated for `btree_iterator_curr()`. A small source
change removes the stalled aggregate copy. It preserves the blob path,
entry validation, prefix compression, and the native format.

The branch was first rebased onto main `d9a61a9a`. The uncommitted native work
was saved in a local commit; `backup/blob-keys-before-main-20261028` retains the
pre-rebase state. Rebase integration preserves main's smaller async state,
fallible reference ownership, and new message API. The rebased native candidate
before the performance fix is `fdbffde0`. No changes were pushed.

## Diagnosis

Cycle sampling during the measured scan puts 36.9% of the rebased candidate's
CPU samples in `btree_iterator_curr()`, versus 14.9% on main. After the fix,
that function returns to 14.9%. The confirmation profile uses one sample per
1,000,003 user cycles and reports no lost records. An earlier, higher-rate
profile lost a few ring-buffer chunks and is retained only as exploratory
evidence. The installed `perf annotate` crashed, so instruction locations were
obtained with `perf script` and matched against `objdump`.

For an ordinary inline key, this source expression:

```c
*curr_key = key_create(FALSE, length, data);
```

generated a temporary with this sequence (other instructions omitted):

```asm
movq   $0,   0x20(%rsp)       # first 8 bytes of the key
mov    %rax, 0x28(%rsp)       # next 8 bytes
movdqa 0x20(%rsp), %xmm0      # immediately reload both stores as 16 bytes
movups %xmm0, (%r12)          # copy to the output key
```

On the tested Intel processor, the wide load cannot be forwarded from those
two narrower stores without a stall. The load and its dependent instruction
are the dominant sampled instruction region. This happens on every tuple,
so a 4096-key range pays it roughly 4096 times.

The fix assigns `kind`, `is_query_key`, `is_blob`, and `user_slice` directly
through the output pointer. GCC then writes the result directly, eliminating
the temporary and wide reload. This is a code-generation problem observed
with this compiler and CPU, not evidence that the logical aggregate assignment
is incorrect C.

## Causal check

The following are medians of three paired runs of 24-byte random keys.
Counters cover only the scan interval; setup and verification are excluded.

| Metric per tuple | Main | Rebased native | Assignment fix |
|---|---:|---:|---:|
| Time, ns | 16.15 | 28.40 | 16.60 |
| User cycles | 40.22 | 70.70 | 41.34 |
| Retired instructions | 221.07 | 239.33 | 236.33 |
| Blocked store forwarding | 0.014 | 1.008 | 0.008 |
| Branch misses | 0.019 | 0.019 | 0.019 |

The relevant hardware event is `cpu_core/ld_blocks.store_forward/u`. The single
source change removes about one blocked forwarding event and 29 cycles per
tuple while retiring only three fewer instructions. It removes about 96% of
the excess scan time in this case. Shared-prefix 24-byte keys show the same
counter change: 1.008 to 0.008 blocked events per tuple, with time falling from
30.86 to 19.02 ns (main: 19.88 ns).

These are actual blob-capable binaries with small keys kept inline. No checks
were disabled for the experiment, and no hypothetical blob I/O was removed.

## Unprofiled confirmation

Five paired repetitions per case, alternating main/fixed run order and using
the same seed within each pair. Positive time change means slower than main.
The range shows the smallest and largest paired change, not a confidence
interval. Small remaining differences should not be described as neutrality.

| Key bytes | Distribution / comparator | Median time change | Paired range |
|---:|---|---:|---:|
| 24 | Random lexical | +2.8% | +2.7% to +3.8% |
| 24 | Shared prefix | -4.8% | -5.1% to -4.4% |
| 64 | Random lexical | +3.0% | +2.9% to +3.6% |
| 64 | Shared prefix | -3.2% | -3.6% to -3.0% |
| 64 | Custom comparator | +2.8% | +2.8% to +3.1% |
| 256 | Random lexical | +2.9% | -3.8% to +4.6% |
| 256 | Shared prefix | -3.8% | -5.6% to -2.9% |
| 512 | Random lexical | +1.9% | -8.7% to +14.2% |
| 512 | Shared prefix | +1.7% | -2.9% to +6.4% |

All 90 unprofiled runs passed value, order, checksum, residency and reclamation
checks. Measured read/write I/O and major faults were zero. Leaf counts,
compressed-leaf counts and prefix bytes matched main for every paired seed.
The 512-byte cases were noisier; their full ranges are retained above.

Configuration: Intel Core i7-12800H, pinned to CPU 2; GCC 14.2.0 with matching
`-O3 -ffast-math -flto -march=native` builds and identical harness sources.
Each run uses a 512 MiB cache, a 2 GiB configured device, 32-byte values,
4096 scan starts per batch, and scans 4096 keys at each start. The final timing
runs use four measured batches (67,108,864 returned tuples), with full warmup
and verification outside timing. There are 65,536 keys for 24/64-byte cases
and 32,768 keys for 256/512-byte cases.

The hardware-counter experiment used eight measured batches. The three
counter variants were main, the rebased candidate, and the single assignment
change. The last differs from the final source only by an explanatory comment.

## Validation and scope

- Full debug unit suite: 299 total, 294 passed, zero failed, five skipped.
- Release native blob-key tests: 3/3 passed.
- Release B-tree format/configuration tests: 14/14 passed.
- Formatting and whitespace checks passed.
- All 90 final timing runs and all three confirmation profiles verified.

The large Scan4096 slowdown is resolved. Random-key scans still show roughly
2–3% median overhead in this sweep. This investigation does not establish that
point lookups, packing, overwrites or the public API have recovered: those
workloads were not rebenchmarked after this fix. The earlier
[small-key report](SMALL_KEYS.md) remains historical evidence for those paths.
The subsequent [Scan32 remeasurement](SCAN32_RESULTS.md) confirms that this fix
also helps short scans, but leaves up to 14.1% median time overhead versus main.
The later [Scan32 profiling report](SCAN32_PROFILE.md) records additional fixes
and a fresh Scan4096 confirmation sweep.

The user confirmed no other agents were running. Each measurement window still
held the shared benchmark and machine locks exclusively. No compilation,
correctness tests or analysis ran concurrently with measurement.

## Evidence

- [Per-run hardware counters](scan4096-results-20261028/counters.csv)
- [Unprofiled paired comparison](scan4096-results-20261028/scan-comparison.csv)
- [Binary hashes and validation](scan4096-results-20261028/evidence.json)
- [Raw profiling artifacts](../../.cache/scan4096-profile-20261028/):
  manifests, commands, identical harness, frozen main source, binaries,
  build logs, perf recordings, per-symbol reports, disassemblies, isolated
  fix patch, stdout/stderr, and correctness logs.

The raw `run.py` is a session-specific runner recording the user's explicit
no-other-agents confirmation. Before reusing it, re-establish quiescence;
its recorded assumption must not replace coordination with future agents.
Its `--mode stat|record|plain`, `--variants`, `--patterns`,
`--key-bytes`, and `--repetitions` arguments reproduce each recorded window.
`PREFIX_PERF_CONTROL` and `PREFIX_PERF_ACK` FIFOs enable perf only inside
the harness's measured intervals.
