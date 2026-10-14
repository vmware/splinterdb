# filtered-ranges: correctness evidence for main integration

Branch: `robj/filtered-ranges`. Merge head when archived: `63e237b92395023337098b6220615b27aeef4383`. Integrated main: `7bb5fab305b9c6179e31f75998dabea833537a08`.

`shared/test_root_branches.py filtered-ranges MODE` selected quick, merge, filter, cache error, and blob coverage. Final commands and outcomes are in `validation-debug.json` and `validation-release.json`. `*.initial.*` preserves the initial core invocation that put its filter before configuration arguments and failed argument parsing. Corrected invocations passed with nonzero coverage.

Build logs describe compilation; JSON records retain the exact executed commands and available source, binary, and log hashes. The archive includes intermediate failures as well as successful final validation; an early failed log is not a claim that the final build failed. Tests with databases used real disk storage under the workspace. `shared/run_job.py` is the coordination wrapper used to serialize timed work against builds and tests. Its syntax is `python3 run_job.py --name NAME test -- COMMAND`; build jobs use the `build` mode.

`shared/before.json` records the original project heads and backup refs. The scripts are archived byte for byte for command provenance; they contain original workspace paths and may depend on their original directory layout. Do not run them directly from this archive without adapting paths and choosing fresh scratch locations.

Original evidence directory: `/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/main-7bb5fab/filtered-ranges`. Only regular files with `.json`, `.log`, `.py`, `.gdb`, or `.md` extensions and at most 1 MiB each were copied. Scratch directories, databases, executables, core dumps, symlinks, bytecode, and shared benchmark outputs are excluded. Raw logs retain their original bytes. `archive-manifest.json` hashes every archived file except itself; it does not hash database files.
