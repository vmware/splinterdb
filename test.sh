#!/bin/bash

set -euxo pipefail

SEED="${SEED:-135}"
DRIVER="${DRIVER:-"${BINDIR:-bin}/driver_test"}"
TEST_RUST="${TEST_RUST:-false}"
UNIT_TEST_DRIVER="${UNIT_TEST_DRIVER:-"${BINDIR:-bin}/unit_test"}"

# Run all the unit-tests first, to get basic coverage
"$UNIT_TEST_DRIVER"

"$DRIVER" kvstore_test --seed "$SEED"

"$DRIVER" kvstore_basic_test --seed "$SEED"

"$DRIVER" splinter_test --functionality 1000000 100 --seed "$SEED"

"$DRIVER" splinter_test --perf --max-async-inflight 0 --num-insert-threads 4 --num-lookup-threads 4 --num-range-lookup-threads 0 --tree-size-gib 2 --cache-capacity-mib 512

"$DRIVER" cache_test --seed "$SEED"

"$DRIVER" btree_test --seed "$SEED"

"$DRIVER" log_test --seed "$SEED"

"$DRIVER" filter_test --seed "$SEED"

"$DRIVER" util_test --seed "$SEED"

if [ "$TEST_RUST" = "true" ]; then
   pushd rust
      cargo fmt --all -- --check
      cargo build
      cargo test
      cargo clippy -- -D warnings
      cargo build --release
      cargo test --release
   popd
fi

echo ALL PASSED
