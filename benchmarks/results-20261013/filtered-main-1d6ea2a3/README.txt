Filtered-ranges PR update after disk-format lands on main

PR head branch: robj/filtered-ranges
PR base branch: main, at 1d6ea2a3e2056127c38b54a582075c7eaf7acf4d (#689)
Main merge: 6b0a035c2ed53a11c440c574deae9889deb4751b
Previous candidate: 33af695feb50b702af080e73cfacffff6ff7b9c6
Backup: backup/filtered-ranges-before-main-1d6ea2a3

Main's entire tree equals former disk-format head 7a0c8171. The main merge's
entire tree equals the previous candidate, including runtime, tests and build
inputs. Squash-history overlaps in the two format documents and routing_filter.c,
plus an automatically duplicated trunk validation block, were resolved to that
already-tested tree. No upstream behavior was lost. Subsequent edits only
refresh PR documentation and this evidence; the API and implementation are
unchanged.

The runtime PR diff against main exactly matches the old diff against the
disk-format branch. The new merge base is main itself, so the predicate PR
does not reintroduce the landed disk-format changes.

Validation

check.py verifies tree identity, ancestry, runtime PR diff identity, no pending
conflicts, exact CI formatting over a staged tracked export, and whitespace.
checks.json and its logs record the result and tested index. Final result logs
and JSON are added after that check; no C/header/runtime source changes occur.
No binaries, databases or exported checkouts are included here.

The implementation is identical to the previously validated companion 36183807
and candidate 33af695f. Its prior 125 debug/125 release integration cases, 66
focused release and 16 ASan cases, full routing-filter and C/C++ public-header
checks remain applicable. The previous 66 serialized real-disk benchmarks also
compare the same runtime against the same baseline now on main. No additional
functional tests or benchmarks were run solely for this history/documentation
update. The full compiler/sanitizer/stress CI matrix remains a merge gate.

Existing performance limitations are unchanged: a severe disk-backed short-scan
outlier did not recur in targeted repeats, and long-scan p99.9 averaged 15.6%
higher with wide overlapping ranges. Tail neutrality is not established.
See ../filtered-disk-pr-0fa3bd6c/README.md and its benchmarks/README.txt for the
complete evidence, controls, raw samples and limitations. Earlier archive
instructions to target disk-format describe the PR base before #689 landed;
the current PR should target main.

Reproduction from the repository root (paths may be adapted):

Stage the intended documentation/evidence, then run through the shared
coordination wrapper in test mode to exclude concurrent timed benchmarks:

python3 .cache/projects-20261011/coordination/run_job.py --name filtered-main-pr-check test -- python3 .cache/projects-20261011/coordination/filtered-main-1d6ea2a3/check.py --tree .cache/projects-20261011/worktrees/filtered-ranges --output .cache/projects-20261011/coordination/filtered-main-1d6ea2a3/checks

The archived check.py is the exact script used. The wrapper is also preserved
in ../filtered-disk-pr-0fa3bd6c/validation/run_job.py. inventory.json hashes this
archive, excluding itself. Original source commits and historical evidence are
preserved rather than rewritten.
