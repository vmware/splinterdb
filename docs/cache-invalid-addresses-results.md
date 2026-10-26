# Invalid cache address errors

`robj/cache-invalid-addresses` is based on the compact caller-owned handle branch
`robj/cache-errors` at `2907e97`. Commit `d3a4488` makes synchronous and
asynchronous demand reads return `STATUS_BAD_PARAM` (`EINVAL`) for misaligned,
out-of-range, and unallocated disk addresses. This includes zero and the reserved
first extent. The output handle is cleared and no reference or I/O request is
created on these failures.

Nonblocking reads reject invalid addresses with the same error. A valid cache
miss still returns `STATUS_BUSY`. Both prefetch APIs drop invalid hints, allowing
the subsequent demand read to report an error. Extent prefetch also drops
addresses that are not extent-aligned.

The implementation retains the existing allocation invariant: a resident mapping
belongs to an allocated extent, and extent discard removes that mapping before
the extent becomes free. Allocation membership is checked on misses, including
nonblocking misses. Valid hits do not gain an allocator lookup or any stronger
atomic operations. Callers must retain the extent throughout acquisition and the
handle's lifetime.

Internal operations on existing cache mappings retain their assertions. This
change handles invalid addresses passed to cache reads and prefetch; it is not a
general malformed-database validator. For example, existing caller-level
assertions in btree access and allocator recovery can still abort on a corrupt
pointer before it reaches the cache.

## Lookup implementation

The selected `async-mask` implementation keeps invalid-address rejection while moving diagnostic formatting out of ordinary lookup paths, placing async miss allocation validation in the existing disk-load coroutine, and replacing the hot alignment remainder with a mask. Initialization now asserts the already required power-of-two page-size invariant before shift/division setup. Synchronous allocation validation remains inline; error, retry, callback and handle-ownership contracts are preserved.

The inherited asynchronous cache admission/batch-reservation progress limitation
remains unresolved; bounded churn does not prove progress across all
cache-pressure and worker-lifecycle interleavings.
