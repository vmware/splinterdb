# Branch leaf prefix experiment

Persisted branch version 1 is rejected before branch-page interpretation.
A clean mount remains lazy and rejects when an affected branch is accessed;
recovery rejects during mount when rebuilding allocation state. No format
migration or eager tree walk is added.

`data_config.key_compare_is_lexical` opts construction into prefix compression.
It promises unsigned-byte lexicographic ordering for stored keys and query
keys, including the rule that a shorter equal prefix sorts first. The default
data configuration opts in. Applications replacing its comparator must clear
the flag unless their replacement preserves this contract.

When a branch leaf first fills, packing extracts the longest common prefix of
its first and last keys, rebuilds entries as suffixes, and continues filling the
page only if that prefix is at least four bytes long. This decision is per leaf;
readers still accept previously written shorter prefixes. A later key outside
that prefix starts another leaf. Prefix bytes occupy
the page tail, so offset-table growth never moves them. A final partially full
leaf is left uncompressed. Index pivots and mutable memtable keys stay complete.

Point reads compare the prefix once, then binary-search suffixes. A mismatch
can place the query on either side of the leaf. On compressed leaves, range
iterators reconstruct full keys into owned buffers, copying the prefix only
when changing leaves. Uncompressed leaves return keys directly from the page,
including nonlexical scans; they skip prefix reconstruction.
Prefix scratch state is allocated only after encountering a compressed leaf,
reused across positioning operations, and released during teardown. Iterators
that visit only uncompressed pages do not initialize or allocate these buffers.
The decode buffer retains its largest logical length, so shorter subsequent
keys do not trigger resizing; the returned key still has its exact length.
Allocation, growth and replacement of the per-leaf prefix stay outside the
ordinary per-key path, and suffix copies of at most 16 bytes use bounded inline
copies. Uncompressed branch-leaf iterators borrow keys from pinned pages;
mutable memtable iterators retain their existing copied-page behavior.
Positioning operations perform any allocation before returning; observing the
current tuple stays infallible. Bounds, seeks, asynchronous lookup, diagnostics,
parent pivots and logical key-byte accounting use the full key semantics.
Iterator boundary and seek probes compare checked prefix/suffix representations
directly, without reconstructing a separate boundary key. Uncompressed leaves
can use direct lexical comparison during checked binary search when the lexical
contract permits it. Nonlexical comparisons, infinity targets on uncompressed
leaves, boundary comparisons and index pivots use the configured comparator.

This prototype now uses branch format version `2` within main's superblock
format `8`. Branch readers reject format `1` before interpreting its different
page header. Earlier standalone prefix images used superblock `0x8007` and are
rejected. Page encoding is determined from the page itself; existing compressed
leaves remain readable after changing the construction opt-in on reopen.

Regression coverage includes compression enabled/disabled, actual compressed
pages, close/reopen, point hits and misses, asynchronous lookup, zero-length
suffixes, forward/reverse ranges, repeated seeks, full-key-only lexical
callbacks, prefix mismatches on both sides, bounded suffix copies, and rejection
of incompatible experimental formats.

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

Boundary and seek comparison avoids a separate `bound_key` reconstruction
buffer. Entry validation and comparison directions are preserved. Regression
coverage includes nearby and cross-leaf seeks, an aliased current-key seek
target, prefix mismatches, and finite inclusive/exclusive bounds with present
and absent endpoints in both directions.
