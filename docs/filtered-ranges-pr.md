Allow range iterators to apply an exact key predicate, with optional routing-filter branch pruning. `splinterdb_iterator_init()` takes optional `.predicate`, `.predicate_context` and `.routing_filter_key` parameters. A predicate alone scans all relevant branches; a representative routing key requires a predicate and asserts hash compatibility under every possible seed. Pruning without a predicate returns `EINVAL`.

Rejected keys are skipped before application merge/finalize callbacks, preserving complete update histories for accepted keys even when routing filters admit collisions. The iterator owns its routing-key copy across trunk spans, direction changes and thread handoff; predicate context remains caller-owned. Ordinary iterator APIs remain unchanged and do not allocate filter state. No callback is added to `data_config`.

This PR targets main `1d6ea2a3`, which includes disk-format (#689). Routing-filter lookup validates each descriptor and uses its saved seed and encoding settings; changing construction defaults cannot reinterpret an existing maplet. A four-entry per-iterator seed/hash cache avoids repeated hashing and safely recomputes after eviction. B-tree version validation stays in the B-tree module.

Regression coverage includes persisted mixed hash seeds, seed/hash-cache
replacement, full-width and seed-specific collisions, noncommutative updates,
deletes, bounds, reverse traversal, thread handoff and copied-key ownership
across trunk spans.
