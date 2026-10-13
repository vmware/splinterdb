# log-group-limit: correctness evidence for main integration

Branch: `robj/log-group-limit`. Merge head when archived: `b4bc273a1beda264aa1220b32978ba9d66ba4050`. Integrated main: `7bb5fab305b9c6179e31f75998dabea833537a08`.

`run_validation.py MODE` selected process-exit recovery, quick, log driver, and core suites. `run_validation.py MODE recovery` selected the final strengthened process-exit case after rebuild; its commands and successful outcomes are in `*-recovery-final-validation.json`. The first final debug rebuild used a nonexistent shorthand Make target and exited 2 before compiling. Its log is preserved beside the corrected explicit target build. All executed test runs had nonzero coverage and returned 0.

Build logs describe compilation; JSON records retain the exact executed commands and available source, binary, and log hashes. The archive includes intermediate failures as well as successful final validation; an early failed log is not a claim that the final build failed. Tests with databases used real disk storage under the workspace. `shared/run_job.py` is the coordination wrapper used to serialize timed work against builds and tests. Its syntax is `python3 run_job.py --name NAME test -- COMMAND`; build jobs use the `build` mode.

`shared/before.json` records the original project heads and backup refs. The scripts are archived byte for byte for command provenance; they contain original workspace paths and may depend on their original directory layout. Do not run them directly from this archive without adapting paths and choosing fresh scratch locations.

Original evidence directory: `/home/rob/projects/splinterdb/.cache/projects-20261011/coordination/main-7bb5fab/log-group-limit`. Only regular files with `.json`, `.log`, `.py`, `.gdb`, or `.md` extensions and at most 1 MiB each were copied. Scratch directories, databases, executables, core dumps, symlinks, bytecode, and shared benchmark outputs are excluded. Raw logs retain their original bytes. `archive-manifest.json` hashes every archived file except itself; it does not hash database files.
