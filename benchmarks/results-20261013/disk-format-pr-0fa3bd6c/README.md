# Disk-format PR preparation against main 0fa3bd6c

Merged the async durable-barrier main commit while preserving module-owned format
checks, stored filter configuration and log checksum seeds. Runtime source merged
without textual conflicts; an independent review confirmed that the main barrier,
checkpoint and notification paths remain intact.

Debug and release each passed 150 unit cases, with two expected skips of the
allocation-failure case because these builds disable allocation fault injection.
Both also passed the functional routing-filter suite and the complete log suite
(500,000-record round trip). The invocation requested 10,000 inserts, but this
functional driver hardcodes 500,000; raw logs confirm the actual count. The initially strict validation runner stopped at that expected skip;
its log and original runner are retained. After recognizing only that named skip,
the debug run resumed with the remaining suites; no database case failed.

Both modes built all targets, tools and benchmarks. Their inspection tools read
the prior format-8 fixture successfully. The release tool rejected the main
format-7 fixture with an explicit unsupported-version message. The exact CI
format command passed on a clean tracked export. The full compiler/sanitizer CI
matrix was not rerun. No new timing measurements were taken in this preparation;
the earlier comparison against main 7bb5fab remains historical evidence.

All 488 pre-merge disk-format archive files and all 563 main archive files were
preserved byte for byte. Conflicting disk-format integration evidence moved from
`main-7bb5fab` to the sibling `disk-format-main-7bb5fab`; its conflicting 20261011
README is now `disk-format-README.md`. `archive-relocations.json` maps old paths
to their preserved copies. The main archives retain their original paths.

Commands, log hashes, source/binary hashes and counts are recorded beside this
file. Shared campaign locks coordinated builds and tests. Databases used ext4 on
the real disk. Existing benchmark artifacts and their inventories were not edited. A local
`.gitattributes` excludes raw perf text snapshots from whitespace diagnostics.
