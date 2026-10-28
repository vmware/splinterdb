#!/usr/bin/env python3
"""Link the same controlled insertion harness against all comparison libraries.

Caller holds shared benchmark/machine locks. No measurements are run.
Input manifests identify explicit frozen libraries; this script does not select
production allocator variants or infer removed build knobs. For the final
original-versus-four-bit comparison use prepare_final.py. Historical width
libraries must come from their archived source/manifests.
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
    args = parser.parse_args()
    tree = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    definitions = json.loads(args.libraries.read_text())
    variants = definitions["variants"]
    harness = output / "kvbench_controlled.c"
    harness.write_bytes((tree / "benchmarks/allocation_table/kvbench_controlled.c").read_bytes())
    flags = ["cc", "-D_GNU_SOURCE", "-DXXH_STATIC_LINKING_ONLY",
             "-DSPLINTERDB_PLATFORM_DIR=platform_linux", "-O3", "-ffast-math",
             "-flto", "-march=native", "-Wall", "-Werror", "-pthread",
             "-DKVBENCH_SPACE_MODE"]
    manifest = {"compiler": subprocess.check_output(["cc", "--version"], text=True),
                "harness": str(harness), "harness_sha256": sha(harness), "variants": {}}
    for name, item in variants.items():
        source = Path(item["source"])
        library = Path(item["library"])
        assert sha(library) == item["library_sha256"]
        binary = output / ("kvbench-" + name)
        close_flags = (["-DKVBENCH_CLOSE_HAS_FORCE"]
                       if "splinterdb_close(splinterdb **kvs, bool32 force)" in
                       (source / "include/splinterdb/splinterdb.h").read_text() else [])
        value_flags = ([] if "message_create(" in
                       (source / "include/splinterdb/data.h").read_text()
                       else ["-Dmessage_create=slice_create"])
        command = flags + close_flags + value_flags + [
            "-I" + str(source / "include"), str(harness), str(library),
            "-laio", "-lxxhash", "-lm", "-o", str(binary)]
        subprocess.run(command, check=True)
        manifest["variants"][name] = dict(item, binary=str(binary), sha256=sha(binary),
                                          link_command=command)
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(name, "linked", flush=True)
    print(output / "manifest.json", flush=True)


if __name__ == "__main__":
    main()
