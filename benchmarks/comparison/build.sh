#!/bin/bash
# Build identical public-API benchmarks against pinned copies of HEAD and main.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT=${1:?usage: build.sh OUTPUT_DIRECTORY}
HEAD_COMMIT=${HEAD_COMMIT:-$(git -C "$ROOT" rev-parse HEAD)}
MAIN_COMMIT=${MAIN_COMMIT:-$(git -C "$ROOT" rev-parse main)}
mkdir -p "$OUT/src" "$OUT/bin" "$OUT/results"

checkout() {
   local name=$1 commit=$2
   local dir=$OUT/src/$name
   if [ ! -d "$dir/.git" ]; then
      git clone --quiet --no-hardlinks "$ROOT" "$dir"
   fi
   git -C "$dir" checkout --quiet --detach "$commit"
}

checkout head "$HEAD_COMMIT"
checkout main "$MAIN_COMMIT"

if [ "${SKIP_LIBS:-0}" != 1 ]; then
   make -C "$OUT/src/head" -j"$(nproc)" CC=cc libs
   make -C "$OUT/src/main" -j"$(nproc)" CC=cc libs
fi

CF='-D_GNU_SOURCE -ggdb -Wall -pthread -Wfatal-errors -Werror -Wvla -DXXH_STATIC_LINKING_ONLY -fPIC -DSPLINTERDB_PLATFORM_DIR=platform_linux -O3 -ffast-math -flto -march=native'
LF='-ggdb -pthread -O3 -ffast-math -flto'
LIBS='-laio -lxxhash -lm -lpthread'

cc $CF $LF -DKVBENCH_CLOSE_HAS_FORCE \
   -I "$OUT/src/head/include" "$ROOT/benchmarks/kvbench.c" \
   "$OUT/src/head/build/release/lib/libsplinterdb.a" $LIBS \
   -o "$OUT/bin/kvbench-head"
cc $CF $LF \
   -I "$OUT/src/main/include" "$ROOT/benchmarks/kvbench.c" \
   "$OUT/src/main/build/release/lib/libsplinterdb.a" $LIBS \
   -o "$OUT/bin/kvbench-main"

cc $CF $LF -I "$OUT/src/head/include" -I "$OUT/src/head/src" \
   -I "$OUT/src/head/src/platform_linux" "$ROOT/benchmarks/insert_benchmark.c" \
   "$OUT/src/head/build/release/lib/libsplinterdb.a" $LIBS \
   -o "$OUT/bin/insert_benchmark-head"
cc $CF $LF -DKVBENCH_LEGACY_CLOSE -DKVBENCH_NO_TRACE \
   -DKVBENCH_NO_DURABLE_BARRIER -I "$OUT/src/main/include" \
   -I "$OUT/src/main/src" -I "$OUT/src/main/src/platform_linux" \
   "$ROOT/benchmarks/insert_benchmark.c" \
   "$OUT/src/main/build/release/lib/libsplinterdb.a" $LIBS \
   -o "$OUT/bin/insert_benchmark-main"

python3 - "$OUT/results/provenance.json" "$HEAD_COMMIT" "$MAIN_COMMIT" <<'PY'
import json, os, platform, subprocess, sys, time
path, head, main = sys.argv[1:]
def output(*args):
    return subprocess.check_output(args, text=True).strip()
data = {
    "created_epoch": time.time(),
    "head_commit": head,
    "main_commit": main,
    "head_description": output("git", "-C", os.getcwd(), "describe", "--always", "--dirty"),
    "kernel": platform.release(),
    "machine": platform.machine(),
    "compiler": output("cc", "--version").splitlines()[0],
    "cpu_count": os.cpu_count(),
    "benchmark_source_sha256": {
        name: output("sha256sum", name).split()[0]
        for name in ("benchmarks/kvbench.c", "benchmarks/insert_benchmark.c")
    },
}
with open(path, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
PY

echo "Built benchmarks in $OUT/bin"
