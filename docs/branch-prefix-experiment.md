# Branch leaf prefix experiment

`data_config.key_compare_is_lexical` opts construction into prefix compression.
It promises unsigned-byte lexicographic ordering for stored keys and query
keys, including the rule that a shorter equal prefix sorts first. The default
data configuration opts in. Applications replacing its comparator must clear
the flag unless their replacement preserves this contract.

When a branch leaf first fills, packing extracts the longest common prefix of
its first and last keys, rebuilds entries as suffixes, and continues filling the
page. A later key outside that prefix starts another leaf. Prefix bytes occupy
the page tail, so offset-table growth never moves them. A final partially full
leaf is left uncompressed. Index pivots and mutable memtable keys stay complete.

Point reads compare the prefix once, then binary-search suffixes. A mismatch
can place the query on either side of the leaf. Range iterators reconstruct
full keys into owned buffers, copying the prefix only when changing leaves.
Equal-length successive keys retain the existing buffer, and suffix copies of
at most 16 bytes use bounded inline copies.
Positioning operations perform any allocation before returning; observing the
current tuple stays infallible. Bounds, seeks, asynchronous lookup, diagnostics,
parent pivots and logical key-byte accounting use the full key semantics.

This independent prototype reserves superblock format `0x8007`. Its format
number must be reconciled with other format-changing projects before combining
them. It does not depend on the separate on-disk metadata project.

Debug validation passed all 48 public quick tests. The new regression checks
compression enabled/disabled, actual compressed pages, close/reopen, point hits
and misses, asynchronous lookup, zero-length suffixes, forward/reverse ranges,
repeated seeks, full-key-only lexical callbacks, and prefix mismatches on both
sides. All 15 superblock tests pass, including rejection of the other independent
experiment formats. After the reconstruction optimization, all eight B-tree
tests (including bounded suffix copies) and the prefix roundtrip pass.
Performance acceptance is pending the follow-up measurements below.

`benchmarks/build_prefix_bench.py /path/to/tree` compiles identical generated
kvbench source against baseline and candidate libraries and records source and
binary hashes. The campaign uses the same generator from its coordination
directory. It produces `build/release/bin/prefix_kvbench` with the existing
kvbench phase arguments and additional environment controls:

* `PREFIX_PATTERN=common`: 64-byte keys with 48 fixed leading bytes.
* `PREFIX_PATTERN=random`: uniformly dispersed first words with random tails.
* `PREFIX_PATTERN=nonlexical`: a comparator orders the final eight-byte rank;
  compression stays disabled.
* `PREFIX_DISABLE=1`: disable candidate compression for an overhead control.

Use the same pattern, seed and key count on reopen. The random workload keeps
two eight-byte words per logical key in the harness to generate ordered ranks;
this memory is identical in baseline and candidate. Space mode reports live
allocation statistics and filesystem allocation after close. Report load plus
drain, point throughput, short/long range throughput and space together: saved
leaf bytes can trade off against rebuilding and key reconstruction.

Initial ABBA common-prefix resident measurements reduced allocated file bytes
by 30.2% and mounted live extents by 25.1%. Point throughput changed by +1.2%
with one reader and -6.1% with eight; short ranges improved 4.3–8.6%. Disk and
nonlexical controls showed severe isolated device-latency outliers. Fixed-image
comparisons of the final reconstruction path and those controls are pending.
This implementation milestone is experimental and does not claim performance
neutrality.
