# Predicate filtering with saved routing-filter configuration

This companion branch is an integration proof for the separate filtered-ranges
and disk-format proposals. It combines their code and tests; it is not a third
feature proposal or a performance claim about the combined implementation.

A filtered iterator still applies its exact key predicate before merging
messages. Its optional routing key can prune a branch only under the public
contract that every accepted key hashes like that routing key for every seed.
The routing-filter module validates each saved descriptor and computes the query
hash with that descriptor's seed. Its four-entry per-iterator hash cache may
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
reverse traversal, and hashes that collide under just one saved seed. Validation
records and commands are archived with this branch's integration results.
