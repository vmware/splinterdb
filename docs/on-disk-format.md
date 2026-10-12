# On-disk versions and read configuration

Format 8 rejects files written by earlier formats. There is no migration reader.
Creation writes both superblock slots. Mount and bootstrap select the newest
checksummed record before checking its structure versions, so an unsupported
newest version cannot silently select an older checkpoint. A damaged slot still
permits recovery from its valid peer. Recognition of a future superblock layout
is limited to the current checksum layout.

| Structure | Version location | Persisted configuration required to read |
| --- | --- | --- |
| Superblock | Superblock header | Device size, page size, extent size |
| Trunk | Superblock | None beyond device geometry and application callbacks |
| Branch B-tree | Each branch reference in its trunk bundle | None beyond device geometry and application callbacks |
| Mini allocator | Each metadata page header | Entries already carry extent number, page type and batch; geometry is global |
| Routing filter | Descriptor in its trunk bundle | Fingerprint width, index width, hash seed, value width, counts and metadata address |
| Shard log | Log head in the superblock | Checksum seed and stream nonce |
| Reference-count allocator | Superblock | Device geometry; map address is already persisted |
| Blob | Existing blob descriptor version | Length and checksum already persisted; descriptor length and geometry determine the extent list |

Memtables are reconstructed and have no persistent configuration record.
Fanout, incorporation thresholds, scheduling, cache size, prefetching and
statistics settings control construction or execution, not interpretation of
existing pages. Blob allocation rounding rules are part of its versioned
encoding, rather than a mutable reader setting.

Existing routing filters use their descriptor's widths and seed for synchronous
and asynchronous reads. Extending a maplet retains its encoding and hash seed;
compaction hashes replacement branches with that seed. New maplets use current
construction settings. Split sizing respects the smallest capacity among the
existing maplets it may extend. Cardinality estimation across different seeds
uses a conservative count because their fingerprint samples cannot be compared.

When combined with optional hash-filtered ranges, an old maplet whose seed
differs from the range predicate's seed must conservatively include branches;
the exact range predicate then uses one common query seed. Hashing the query
under each old seed alone can exclude keys that collide only under the query
seed. The independent range-filtering branch requires this integration change.

Each recovered log reads with its saved checksum seed. Newly constructed logs
may use the current creation configuration.

The application must supply matching key comparison, key hash algorithm, merge
semantics and key/value encodings on reopen. Function pointers and application
schemas cannot be persisted as portable identities. The persisted hash seed is
passed to the supplied hash callback. This change does not introduce a portable
byte-order encoding or promise compatibility between arbitrary architectures.

# Validation and measurements

Debug validation: 16 superblock tests, 33 internal Splinter tests, 47 public quick
tests, full functional routing-filter and log suites. Added regressions cover
unknown versions, changed routing widths/seeds across reopen and further
compaction, synchronous/asynchronous filter reads, and changed log checksum
creation defaults.

Initial resident ABBA measurements found a 2.8–4.0% point-read regression.
After removing redundant validation, a second campaign used a single loader,
200,000 keys in a 128 MiB cache and 7-second read phases. Point throughput was
still 3.4–3.6% lower; range results ranged from -2.5% to +2.6%. The 2-million-key,
64 MiB-cache campaign reported +2.0–2.4% point throughput and +2.7–6.6% range
throughput, with comparable I/O per operation. Device/run variability and
independently constructed trees prevent attributing those gains to metadata.
All samples had zero correctness violations and errors. Brief load samples do
not establish write-performance neutrality.

A subsequent reader change removes a full routing-config copy from each filter
lookup, passing the saved index width and hash seed directly. Version and bounds
validation remains at the same boundaries. The full functional filter suite and
changed-configuration reopen/compaction regression pass after that change.
Its profiling and fixed-image old/new reader measurements are pending; this
implementation milestone does not claim performance acceptance.

Campaign artifacts: `.cache/projects-20261011/benchmarks/disk-format/` in the
main checkout. The common harness and build provenance are retained there;
benchmark database files were on a verified disk filesystem, with the shared
benchmark lock excluding other benchmarks, builds and tests.
