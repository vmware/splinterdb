# Fallible cache reads and transferable page handles

Cache read failures now propagate through database operations using explicit
status returns and caller-owned page handles. Failed loads release their
references and remain retryable. If reclamation fails after partially freeing
an object, it emits a warning, abandons the remaining space, and suppresses the
clean allocation-state marker so reopening rebuilds reference accounting.

Each handle records the acquisition thread ID used to select its read-reference
counter. A registered receiving thread can release or upgrade the handle using
that counter, even after the acquiring thread exits or its ID is reused. Public
iterators can likewise move between threads with ordinary synchronization;
only one thread may use an iterator at a time. Copying a handle does not acquire
another reference; each acquisition must be released exactly once.

The retained handle contains a shared page-identity pointer and the acquisition
ID (16 bytes on this platform). Reference-count atomic operations and ordering
are unchanged. Release builds exclude the deterministic prefetch regression
hooks, while debug builds retain their tests.

After merging current main's multi-level blob support, validation passed 239
debug unit cases, with three existing skips, plus 65 release cases and 69
AddressSanitizer cases. Functional prefetch, asynchronous cache, B-tree, log,
and database tests also passed.
The error tests cover synchronous/asynchronous failures, waiting readers,
upstream propagation, and recovery after incomplete reclamation. Handoff tests
cover page allocation, sync/async acquisition, lock upgrade, thread exit and ID
reuse, and public iterator movement. LeakSanitizer was disabled under the
sandbox; address checks were enabled.

Multi-level blob traversal also propagates cache errors and retains transferable
data and pointer-page handles. A height-three regression covers failed reads at
both pointer levels and data pages, cleanup and retry, and iterator handoff
across a subtree boundary.

Performance results and their limitations are recorded in
[the cache-error report](cache-errors-results.md) and
[the handoff report](cache-handoff-results.md), with current-main integration
checks in [the merge report](cache-handoff-mainmerge.md). The final checks use
serialized physical-disk benchmarks with explicitly fsynced fixture copies.

Submit `robj/cache-handoff` against `main` as one PR: the branch already
contains `robj/cache-errors` and includes both features. The NULL-return and
24-byte direct-field alternatives remain separate experimental branches.
