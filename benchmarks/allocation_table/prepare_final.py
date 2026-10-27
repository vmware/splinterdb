#!/usr/bin/env python3
"""Freeze and build the original and current fixed-four-bit macro comparison.

Caller must hold the shared benchmark and machine locks. This script builds and
links only: it never runs a database load or a timed benchmark. The current
working source is copied before building, including uncommitted tracked changes;
its complete source digests identify the measured implementation independently
of HEAD. The baseline is exactly the pinned original commit.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile

BASELINE = "6e550e1e"
VERSION = "allocation-final-comparison"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def source_hashes(source):
    files = [source / "Makefile"]
    files += [p for directory in [source / "src", source / "include"]
              for p in sorted(directory.rglob("*")) if p.is_file()]
    return {str(p.relative_to(source)): sha(p) for p in files}


def archive(tree, revision, destination):
    destination.mkdir()
    raw = subprocess.check_output(["git", "archive", revision], cwd=tree)
    with tarfile.open(fileobj=io.BytesIO(raw)) as files:
        files.extractall(destination, filter="data")


def freeze_current(tree, destination):
    archive(tree, "HEAD", destination)
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=tree)
    for raw in paths.split(b"\0"):
        if not raw:
            continue
        relative = Path(raw.decode())
        src, dst = tree / relative, destination / relative
        if src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        elif not src.exists() and dst.exists():
            dst.unlink()
    # Newly added production files also belong to the frozen implementation.
    paths = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z", "src", "include"],
        cwd=tree)
    for raw in paths.split(b"\0"):
        if raw:
            relative = Path(raw.decode())
            dst = destination / relative
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(tree / relative, dst)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args()
    if not 1 <= args.jobs <= 4:
        parser.error("jobs must be between 1 and 4")
    tree = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    compiler = subprocess.check_output(["cc", "--version"], text=True)
    baseline_commit = subprocess.check_output(
        ["git", "rev-parse", BASELINE], cwd=tree, text=True).strip()
    current_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=tree, text=True).strip()
    status = subprocess.check_output(
        ["git", "status", "--porcelain=v1"], cwd=tree, text=True)
    (output / "current-working-tree-status.txt").write_text(status)
    (output / "current-working-tree.patch").write_bytes(
        subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=tree))
    harness = output / "kvbench_controlled.c"
    shutil.copy2(tree / "benchmarks/allocation_table/kvbench_controlled.c", harness)
    micro_source = output / "refcount_bench.c"
    shutil.copy2(tree / "benchmarks/allocation_table/refcount_bench.c", micro_source)
    flags = ["cc", "-D_GNU_SOURCE", "-DXXH_STATIC_LINKING_ONLY",
             "-DSPLINTERDB_PLATFORM_DIR=platform_linux", "-O3", "-ffast-math",
             "-flto", "-march=native", "-Wall", "-Werror", "-pthread"]
    manifest = {"purpose": "full PR versus original; not a packing-only ablation",
                "baseline_commit": baseline_commit, "current_head": current_commit,
                "current_working_tree_status": status, "compiler": compiler,
                "compiler_sha256": sha(Path(shutil.which("cc")).resolve()),
                "harness": str(harness), "harness_sha256": sha(harness),
                "variants": {}}
    for name in ["original", "current"]:
        source = output / (name + "-source")
        if name == "original":
            archive(tree, baseline_commit, source)
        else:
            freeze_current(tree, source)
            makefile = (source / "Makefile").read_text()
            if "BUILD_RC_COMPACT" in makefile or "BUILD_RC_BITS" in makefile:
                raise RuntimeError("current source still has selectable allocator knobs")
            header = (source / "src/rc_refcount.h").read_text()
            if "SPLINTER_RC_BITS" in header:
                raise RuntimeError("current refcount header still has selectable tag width")
        before = source_hashes(source)
        if name == "current" and source_hashes(tree) != before:
            raise RuntimeError("working production source changed during freezing")
        command = ["make", "-j" + str(args.jobs), "BUILD_MODE=release",
                   "GIT_VERSION=" + VERSION, "libs"]
        with (output / (name + "-build.log")).open("w") as log:
            run(command, cwd=source, stdout=log, stderr=subprocess.STDOUT)
        if source_hashes(source) != before:
            raise RuntimeError("production source changed during build: " + name)
        if name == "current" and source_hashes(tree) != before:
            raise RuntimeError("working production source changed during build")
        library = source / "build/release/lib/libsplinterdb.a"
        binary = output / ("kvbench-" + name)
        close_flags = (["-DKVBENCH_CLOSE_HAS_FORCE"]
                       if "splinterdb_close(splinterdb **kvs, bool32 force)" in
                       (source / "include/splinterdb/splinterdb.h").read_text() else [])
        link = flags + close_flags + ["-DKVBENCH_SPACE_MODE", "-I" + str(source / "include"),
                                      str(harness), str(library), "-laio", "-lxxhash", "-lm",
                                      "-o", str(binary)]
        run(link)
        manifest["variants"][name] = dict(
            source=str(source), revision=baseline_commit if name == "original" else current_commit,
            includes_working_tree_changes=name == "current" and bool(status),
            source_sha256=before, build_command=command, build_cwd=str(source),
            build_log=str(output / (name + "-build.log")),
            library=str(library), library_sha256=sha(library),
            binary=str(binary), sha256=sha(binary), link_command=link)
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(name, "frozen and linked", flush=True)
    source = Path(manifest["variants"]["current"]["source"])
    library = Path(manifest["variants"]["current"]["library"])
    micro = output / "refcount-bench"
    command = flags + ["-I" + str(source / "include"), "-I" + str(source / "src"),
                       "-I" + str(source / "src/platform_linux"), str(micro_source),
                       str(library), "-laio", "-lxxhash", "-lm", "-o", str(micro)]
    run(command)
    manifest["micro"] = dict(binary=str(micro), sha256=sha(micro), command=command,
                              source=str(micro_source), source_sha256=sha(micro_source),
                              dense_role="standalone dense64 checked-CAS control")
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    configs = {
        "baseline": {"keys": 10000000, "cache_mib": 128, "disk_gib": 8},
        "capacity1t": {"keys": 10000000, "cache_mib": 1024, "disk_gib": 1024},
        "large30m": {"keys": 30000000, "cache_mib": 4096, "disk_gib": 1024}}
    for filename, selected in [("configs.json", configs),
                               ("baseline-config.json", {"baseline": configs["baseline"]}),
                               ("resource-configs.json", {k: v for k, v in configs.items()
                                                          if k != "baseline"})]:
        (output / filename).write_text(json.dumps(selected, indent=2) + "\n")
    print(output / "manifest.json", flush=True)


if __name__ == "__main__":
    main()
