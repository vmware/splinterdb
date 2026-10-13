# disk-format: correctness evidence for main integration

Branch: `robj/disk-format`. Merge head when archived: `d21b87f5cfac709fb0fa74c3585fb38936c08852`. Integrated main: `7bb5fab305b9c6179e31f75998dabea833537a08`.

`test_integration.py debug` and `test_integration.py release` selected btree, superblock, core, quick, trunk async, filter, and log suites. `validation-debug.json` and `validation-release.json` record final commands and passing outcomes. Initial compilation failures during merge resolution remain in the build logs. `*-4gib.*` preserves the first core suite runs: the new large blob test exceeded the configured 4 GiB logical capacity. The corrected 64 GiB logical fixtures passed in debug and release.

Build logs describe compilation; JSON records retain the exact executed commands and available source, binary, and log hashes. The archive includes intermediate failures as well as successful final validation; an early failed log is not a claim that the final build failed. Tests with databases used real disk storage under the workspace. `shared/run_job.py` is the coordination wrapper used to serialize timed work against builds and tests. Its syntax is `python3 run_job.py --name NAME test -- COMMAND`; build jobs use the `build` mode.

`shared/before.json` records the original project heads and backup refs. The scripts are archived byte for byte for command provenance; they contain original workspace paths and may depend on their original directory layout. Do not run them directly from this archive without adapting paths and choosing fresh scratch locations.

Original evidence directory: `/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/main-7bb5fab/disk-format`. Only regular files with `.json`, `.log`, `.py`, `.gdb`, or `.md` extensions and at most 1 MiB each were copied. Scratch directories, databases, executables, core dumps, symlinks, bytecode, and shared benchmark outputs are excluded. Raw logs retain their original bytes. `archive-manifest.json` hashes every archived file except itself; it does not hash database files.
