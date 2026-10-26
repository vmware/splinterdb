#!/usr/bin/env python3
"""Link frozen comparison binaries; caller holds the campaign shared locks.

First build release libraries for the original source snapshot and the topic
worktree with BUILD_RC_COMPACT=0/1 in separate build roots. No benchmarks run.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    tree = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    baseline = args.baseline.resolve()
    variants = {
        "original": (baseline, baseline / "build/release/lib/libsplinterdb.a", 0),
        "dense": (tree, tree / "build-rc-dense/release/lib/libsplinterdb.a", 0),
        "compact": (tree, tree / "build-rc-compact/release/lib/libsplinterdb.a", 1),
    }
    flags = ["cc", "-D_GNU_SOURCE", "-DXXH_STATIC_LINKING_ONLY",
             "-DSPLINTERDB_PLATFORM_DIR=platform_linux", "-O3", "-ffast-math",
             "-flto", "-march=native", "-Wall", "-Werror", "-pthread"]
    manifest = {"compiler": subprocess.check_output(["cc", "--version"], text=True),
                "variants": {}, "source_sha256": {}}
    for relative in ["Makefile", "src/allocator.h", "src/rc_allocator.c",
                     "src/rc_allocator.h", "src/rc_refcount.c", "src/rc_refcount.h",
                     "src/mini_allocator.c", "src/mini_allocator.h",
                     "benchmarks/kvbench.c", "benchmarks/allocation_table/refcount_bench.c"]:
        manifest["source_sha256"][relative] = sha(tree / relative)
    for name, (source_tree, library, compact) in variants.items():
        binary = output / ("kvbench-" + name)
        close_flags = (["-DKVBENCH_CLOSE_HAS_FORCE"]
                       if "splinterdb_close(splinterdb **kvs, bool32 force)" in
                       (source_tree / "include/splinterdb/splinterdb.h").read_text()
                       else [])
        command = flags + close_flags + ["-DKVBENCH_SPACE_MODE", "-DSPLINTER_RC_COMPACT=" + str(compact),
                          "-I" + str(source_tree / "include"),
                          str(tree / "benchmarks/kvbench.c"), str(library),
                          "-laio", "-lxxhash", "-lm", "-o", str(binary)]
        subprocess.run(command, check=True)
        manifest["variants"][name] = dict(binary=str(binary), sha256=sha(binary),
                                          library=str(library), library_sha256=sha(library),
                                          command=command)
    binary = output / "refcount-bench"
    library = variants["compact"][1]
    command = flags + ["-DSPLINTER_RC_COMPACT=1", "-I" + str(tree / "include"),
                      "-I" + str(tree / "src"), "-I" + str(tree / "src/platform_linux"),
                      str(tree / "benchmarks/allocation_table/refcount_bench.c"),
                      str(library), "-laio", "-lxxhash", "-lm", "-o", str(binary)]
    subprocess.run(command, check=True)
    manifest["micro"] = dict(binary=str(binary), sha256=sha(binary), command=command)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(output / "manifest.json")


if __name__ == "__main__":
    main()
