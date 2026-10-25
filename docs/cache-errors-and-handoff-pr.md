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

Error regressions cover synchronous/asynchronous failures, waiting readers,
upstream propagation and recovery after incomplete reclamation. Handoff
regressions cover allocation, sync/async acquisition, lock upgrade, thread exit
and ID reuse, and public iterator movement.

Multi-level blob traversal also propagates cache errors and retains transferable
data and pointer-page handles. A height-three regression covers failed reads at
both pointer levels and data pages, cleanup and retry, and iterator handoff
across a subtree boundary.
