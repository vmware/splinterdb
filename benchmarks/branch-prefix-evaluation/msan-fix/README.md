# MSan follow-up: compressed-page free space and async read tracking

The threaded sequential-key/random-value insertion workload exposed two
independent initialization defects. The original runtime at `8c192839` failed
locally under Clang 19 release MSan in 2.68 seconds, with the same
`btree_pack_compress_leaf` scratch allocation and whole-page-copy origin as CI.
The first local poisoned consumer was `btree_read_leaf_entry`; CI reported
`routing_get_bucket_counts`.

Compression rebuilt the header, offsets, tuples and prefix in a stack page,
then copied the whole page back. Its unused gap remained uninitialized. The
fix clears exactly that gap before copying, once per successfully compressed
leaf. It prevents stack garbage from entering the persisted free space and
leaves the nonlexical early-return paths unchanged.

Separately, libaio kernel reads replaced cache-slot bytes without updating
MSan shadow. A slot containing poisoned scratch bytes could later hold a valid
filter or B-tree page while retaining the old initialization origin. Routing
filter construction initializes its live encoding correctly. The async-read
fix marks only the returned bytes initialized, across iovecs, before invoking
the client callback. Partial reads retain poisoned tails; EOF, failures and
writes do not initialize buffers. This code compiles only in MSan builds.

The new direct-pack regression creates one compressed 4 KiB leaf containing
64 tuples and a 2,842-byte free gap, checking shadow before reading the gap or
reloading the page. Five I/O regressions inspect initialization inside the
completion callback and verify the data actually returned. They cover three
full iovecs, a short read ending 37 bytes into the second iovec, EOF, failed
submission, and write-source shadow. The standalone platform test target now
links its existing platform-I/O object group.

The test overlay against original runtime fails the gap, full-read and
short-read checks. The EOF, failed-read and write controls pass. Each failing
baseline case runs in a separate process because CTest assertion failure skips
fixture teardown. Initial build/fixture attempts remain in the evidence.

The repaired release MSan build passes the complete reported command:

```sh
build/release-msan/bin/unit/large_inserts_stress_test --num-inserts 1000000 --db-capacity-gib 25
```

It completes in 116.1 seconds with 11 passed, 0 failed and 2 existing skips.
The six focused regressions, all 12 B-tree tests and all 62 quick tests also
pass under both Clang 19 release MSan and GCC release builds: 171 successful
test executions in total, including the stress suite. No MSan suppression or
disabled check is used. Repository formatting and whitespace checks pass, and
live sources match both tested snapshots.

See [validation.json](validation.json) for final commands, test counts and
source identities, and [evidence.tar.gz](evidence.tar.gz) with its
[SHA256 manifest](evidence-manifest.json) for raw logs, source snapshots and
runner scripts. This validation reruns the reported CI command locally;
hosted CI status is separate. The GitHub connector still requires VMware SSO
authorization to inspect hosted runs.

The earlier throughput results predate the compressed-gap clearing change.
They were not rerun for this correctness fix. The production work added is
one bounded clear per compressed leaf; the async bookkeeping is MSan-only.
