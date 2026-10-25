# Predicate filtering with saved routing-filter configuration

The current `robj/filtered-ranges` PR candidate targets main `1d6ea2a3`, where
disk-format landed as #689. The earlier companion branch at `36183807`
supplied the predicate integration code and regression tests now included here.

The iterator initializers take three separate additional arguments: a
`splinterdb_key_predicate`, its borrowed `void *` context, and an optional
`routing_filter_key`. No `data_config` callback is added. A predicate without a
routing key scans all relevant branches; a routing key without a predicate is
invalid and returns `EINVAL`. Initialization copies the routing key, while the
caller keeps the predicate context alive through iterator destruction and any
thread handoff.

A filtered iterator still applies its exact key predicate before merging
messages. Its optional routing key can prune a branch only under the public
contract that every accepted key hashes like that routing key for every possible
seed. This is a requirement on the application's predicate and hash function,
not a test against the seeds encountered during a scan. The exact predicate
determines which keys are returned, including when distinct keys have identical
hashes.

The routing-filter module validates each saved maplet descriptor and computes
the query hash with that descriptor's seed. It uses the maplet's saved encoding
parameters to read the filter. Its four-entry per-iterator hash cache may
recompute hashes after eviction. No hash callback is invoked for a malformed or
canonical empty descriptor. New-filter defaults do not alter existing filters.

Branch collection carries the saved B-tree version into query initialization.
Selected branch roots are validated by the B-tree module before acquiring their
references; a failure leaves the previously collected references available for
normal caller cleanup. The trunk does not decide which B-tree versions are
supported.

The integration regressions cover six saved seeds, exact callback seed use,
cache eviction, invalid-descriptor rejection, and a reopened database containing
multiple saved seeds. The database test compares predicate-only and pruned scans
with expected full values across trunk spans, including updates, deletions,
reverse traversal, and hashes that collide under just one saved seed.
