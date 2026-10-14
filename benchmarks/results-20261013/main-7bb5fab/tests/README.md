# async-durable-barrier: correctness evidence for main integration

Branch: `robj/async-durable-barrier`. Merge head when archived: `7c2fee09f88290d249be6bcdf4ae6ab2527b167f`. Integrated main: `7bb5fab305b9c6179e31f75998dabea833537a08`.

Commands and outcomes are in `debug-validation.json`, `release-validation.json`, and `debug-asan-validation.json`. `run_validation.py` selected notification and optimize tests with and without shared memory, plus quick and core suites; the ASan run selected notification and optimize suites. Notification suite skips are retained in the raw results.

Build logs describe compilation; JSON records retain the exact executed commands and available source, binary, and log hashes. The archive includes intermediate failures as well as successful final validation; an early failed log is not a claim that the final build failed. Tests with databases used real disk storage under the workspace. `shared/run_job.py` is the coordination wrapper used to serialize timed work against builds and tests. Its syntax is `python3 run_job.py --name NAME test -- COMMAND`; build jobs use the `build` mode.

`shared/before.json` records the original project heads and backup refs. The scripts are archived byte for byte for command provenance; they contain original workspace paths and may depend on their original directory layout. Do not run them directly from this archive without adapting paths and choosing fresh scratch locations.

Original evidence directory: `/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/main-7bb5fab/async-durable-barrier`. Only regular files with `.json`, `.log`, `.py`, `.gdb`, or `.md` extensions and at most 1 MiB each were copied. Scratch directories, databases, executables, core dumps, symlinks, bytecode, and shared benchmark outputs are excluded. Raw logs retain their original bytes. `archive-manifest.json` hashes every archived file except itself; it does not hash database files.
