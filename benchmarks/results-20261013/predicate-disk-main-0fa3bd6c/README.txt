Fresh filtered-predicate and disk-format screening against main 0fa3bd6c.
OPEN PERFORMANCE CONCERN: disk-format resident point8 throughput fell 4.39% in the broad screen and 7.75% in the longer repeat, with non-overlapping sample ranges and zero disk reads.
The cause is unresolved. Code review is appropriate; performance-neutrality and merge-ready claims are not supported. The broad and repeat samples remain separate.
See report.txt for methodology, sample ranges and limits; summary.json for CPU/I/O and query counts.
Each profile build JSON records source, library, executable hashes and compile commands.
Each workload manifest records exact invocation, storage, verification and process resource usage.
Sources are frozen measured bytes. Historical absolute paths identify the original workspace.
Databases, libraries and executables are intentionally not embedded. Fixture creation logs and
digests are retained; every copied fixture was checked and fsynced before use.
