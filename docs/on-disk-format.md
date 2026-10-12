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

The benchmark used identical generated kvbench source, O_DIRECT files on ext4,
CPU affinity 0–11, 24-byte keys, 100-byte values and WAL disabled. A single
loader constructed each image; one memtable and three normal workers were
enabled, so incompatible baseline/candidate formats still have a concurrent
tree-shape caveat. Resident: 200,000 keys / 128 MiB cache. Disk regime: 2 million
keys / 64 MiB cache. ABBA read phases lasted seven seconds.

| Pre-scalar-reader campaign | Point, 1 reader | Point, 8 readers | Range throughput change |
| --- | ---: | ---: | ---: |
| Resident | -3.4% | -3.6% | -2.5% to +2.6% |
| Disk | +2.0% | +2.4% | +2.7% to +6.6% |

The disk samples had comparable I/O per operation. Device/run variation and
independently constructed trees prevent attributing their apparent gains to
metadata. Initial shorter resident samples also showed a 2.8–4.0% point-read
regression. All runs had zero correctness violations and errors; brief load
samples do not establish write-performance neutrality.

The final reader removes a full routing-config copy from each filter lookup,
passing the saved index width and hash seed directly. Version and bounds
validation stays at the same boundaries. A six-run, ten-second fixed-image
comparison used baseline / old reader / final reader / final reader / old
reader / baseline order. Both candidate readers used copies of exactly the same
format-8 image; baseline used its own format-7 image.

| Final reader, resident | Versus old reader | Versus baseline |
| --- | ---: | ---: |
| Point, 1 reader | +1.2% | -1.5% |
| Point, 8 readers | +2.0% | -6.5% |
| Length-1 range, 1 reader | -2.2% | -1.5% |
| Length-1 range, 8 readers | -1.7% | -1.0% |

The final functional filter suite and changed-configuration reopen/compaction
regression pass. Profiling the earlier reader places most samples in cache
acquisition/release and B-tree/trunk lookup. It does not isolate a single cause
for the residual point-read cost. Each bundle decode now checks its branch
versions; that added work remains a possible cost. Performance neutrality has
**not** been established. The final scalar reader has not repeated the full
disk or write matrix; the disk figures above describe the earlier reader.

Compact CSVs, manifests, profiling summaries, runners and build provenance are
committed under `benchmarks/results-20261011/`. Full campaign artifacts remain
at `.cache/projects-20261011/benchmarks/disk-format/` in the main checkout;
benchmark database files were on a verified disk filesystem, with the shared
benchmark lock excluding other benchmarks, builds and tests.
