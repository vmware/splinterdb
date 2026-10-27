#!/usr/bin/env python3
"""Create controlled dense-allocator source snapshots and build release libraries.

Run under shared campaign locks. No performance measurements are made here.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args()
    tree = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = {"compiler": subprocess.check_output(["cc", "--version"], text=True),
                "variants": {}}
    definitions = [
        ("original", "5733e6d9", "Original ownership and allocator implementation"),
        ("ownership", "5733e6d9", "Only mini ownership normalization and format2"),
        ("dense", "efde7cf1", "Full prototype, dense32 map"),
        ("dense_direct", "efde7cf1", "Full dense prototype, original page-valid dispatch"),
    ]
    for name, revision, description in definitions:
        source = output / name
        source.mkdir()
        archive = subprocess.check_output(["git", "archive", revision], cwd=tree)
        with tarfile.open(fileobj=io.BytesIO(archive)) as files:
            files.extractall(source, filter="data")
        changes = {}
        if name == "ownership":
            for relative in ["src/mini_allocator.c", "src/mini_allocator.h"]:
                (source / relative).write_bytes(subprocess.check_output(
                    ["git", "show", "efde7cf1:" + relative], cwd=tree))
                changes[relative] = "copied from efde7cf1"
            header = source / "src/rc_allocator.h"
            original = header.read_text()
            revised = original.replace("RC_ALLOCATOR_FORMAT_VERSION (1)",
                                       "RC_ALLOCATOR_FORMAT_VERSION (2)")
            assert revised != original
            header.write_text(revised)
            changes["src/rc_allocator.h"] = "allocator format version1 to2 only"
        if name == "dense_direct":
            header = source / "src/allocator.h"
            original = header.read_text()
            revised = original.replace(
                "bool32 rfc = allocator_is_allocated(al, base_addr);",
                "refcount rfc = allocator_get_refcount(al, base_addr);")
            assert revised != original
            header.write_text(revised)
            changes["src/allocator.h"] = "page-valid uses exact refcount directly"
        # Same definition in all archives avoids checkout-path version noise.
        command = ["make", "-j" + str(args.jobs), "BUILD_MODE=release",
                   "BUILD_RC_COMPACT=0", "GIT_VERSION=allocation-comparison", "libs"]
        with (output / (name + "-build.log")).open("w") as log:
            subprocess.run(command, cwd=source, stdout=log, stderr=subprocess.STDOUT,
                           check=True)
        library = source / "build/release/lib/libsplinterdb.a"
        manifest["variants"][name] = dict(
            source=str(source), revision=revision, description=description,
            changes=changes, library=str(library), library_sha256=sha(library),
            build_command=command,
            source_sha256={str(p.relative_to(source)): sha(p)
                           for directory in [source / "src", source / "include"]
                           for p in sorted(directory.rglob("*")) if p.is_file()},
            makefile_sha256=sha(source / "Makefile"))
        (output / "libraries.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(name, "built", flush=True)
    print(output / "libraries.json", flush=True)


if __name__ == "__main__":
    main()
