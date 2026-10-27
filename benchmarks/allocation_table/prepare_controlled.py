#!/usr/bin/env python3
"""Link the same controlled insertion harness against all comparison libraries.

Caller holds shared benchmark/machine locks. No measurements are run.
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
    parser.add_argument("--libraries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--packed-tree", type=Path)
    args = parser.parse_args()
    tree = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    definitions = json.loads(args.libraries.read_text())
    variants = definitions["variants"]
    if args.packed_tree:
        packed = args.packed_tree.resolve()
        for bits in [2, 4, 8]:
            library = packed / ("build-rc-expanded-" + str(bits)) / "release/lib/libsplinterdb.a"
            variants["compact" + str(bits)] = dict(
                source=str(packed), library=str(library), library_sha256=sha(library),
                bits=bits, compact=True,
                source_sha256={str(p.relative_to(packed)): sha(p)
                               for directory in [packed / "src", packed / "include"]
                               for p in sorted(directory.rglob("*")) if p.is_file()},
                makefile_sha256=sha(packed / "Makefile"))
    harness = tree / "benchmarks/allocation_table/kvbench_controlled.c"
    flags = ["cc", "-D_GNU_SOURCE", "-DXXH_STATIC_LINKING_ONLY",
             "-DSPLINTERDB_PLATFORM_DIR=platform_linux", "-O3", "-ffast-math",
             "-flto", "-march=native", "-Wall", "-Werror", "-pthread",
             "-DKVBENCH_SPACE_MODE"]
    manifest = {"compiler": subprocess.check_output(["cc", "--version"], text=True),
                "harness": str(harness), "harness_sha256": sha(harness), "variants": {}}
    (output / "kvbench_controlled.c").write_bytes(harness.read_bytes())
    for name, item in variants.items():
        source = Path(item["source"])
        library = Path(item["library"])
        assert sha(library) == item["library_sha256"]
        binary = output / ("kvbench-" + name)
        close_flags = (["-DKVBENCH_CLOSE_HAS_FORCE"]
                       if "splinterdb_close(splinterdb **kvs, bool32 force)" in
                       (source / "include/splinterdb/splinterdb.h").read_text() else [])
        command = flags + close_flags + ["-I" + str(source / "include"),
                                         str(harness), str(library), "-laio", "-lxxhash", "-lm",
                                         "-o", str(binary)]
        subprocess.run(command, check=True)
        manifest["variants"][name] = dict(item, binary=str(binary), sha256=sha(binary),
                                          link_command=command)
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(name, "linked", flush=True)
    print(output / "manifest.json", flush=True)


if __name__ == "__main__":
    main()
