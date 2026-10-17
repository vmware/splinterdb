# Cache invalid-address checks: main integration, 2026-10-17

Merged main `8e0f29b89598db1d52aa7090404b475001dbf2eb` into
`robj/cache-invalid-addresses`, starting at
`c15f1452b752dd57a505f766b0d2a029348b94e9`. The merge was automatic and had no
conflicts. Main includes the prefix-compression implementation and its MSan
repairs. The project's `cache.h`, `clockcache.c`, and `writeback_set_test.c`
remain byte-for-byte identical to their starting versions; these are the only
runtime/test differences from the merged main.

## Validation

| Check | GCC debug | GCC release | Clang 19 release + MSan |
|---|---:|---:|---:|
| Writeback/cache unit tests | 18 | 18 | 18 |
| Public quick unit tests | 62 | 62 | 62 |
| Platform API unit tests | 11 | 11 | 11 |
| B-tree unit tests | 12 | 12 | — |
| Focused core propagation/version tests | 3 | 3 | — |
| **Passing unit executions** | **106** | **106** | **91** |
| Existing fault-injection-only skips | 2 | 2 | 2 |

All **303 unit executions passed**. The six skipped executions are the same two
preexisting platform memory-fault tests in each build; fault injection was
disabled. The debug deterministic prefetch-admission functional check also
passed. The full maintained-source format check and staged/unstaged Git
whitespace checks passed.

Coverage includes invalid synchronous, nonblocking, asynchronous, and prefetch
addresses; cleared failure handles; absence of I/O for rejected addresses;
cross-thread handle ownership; read-error propagation; and retry behavior.
The quick and platform suites include the newly merged prefix free-gap and
asynchronous-read MSan regressions. The three focused core cases exercise cache
read errors, invalid branch references, and persisted old-version rejection.

Every build and test acquired the shared campaign machine lock exclusively.
Jobs used isolated temporary directories. Test databases were on ext4 backed
by `/dev/mapper/snafu-root_crypt`; each job's disposable database directory was
removed only after successful process exit. The preexisting untracked build
directories were preserved. Production/test source hashes match before and
after validation.

These are integration checks, not new performance measurements. Earlier
benchmark results remain tied to their recorded revisions. The documented
performance uncertainty and inherited asynchronous allocation-progress stall
remain unresolved; this merge changes neither claim.

## Evidence

- [Integration summary](integration.json): revisions, counts, compiler versions,
  library hashes, and exact preservation of the project-specific sources.
- [Validation manifest](validation.json): commands, timing/lock intervals,
  exit statuses, test counts, binary hashes, and source inventories.
- [Validation runner](validate.py), adjacent per-job logs, and build configs.
- [Project extension](invalid-address-extension.patch): current delta from main.
- [Artifact hashes](sha256.json): hashes of this evidence directory.
