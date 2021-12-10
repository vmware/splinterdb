#!/bin/bash

set -euxo pipefail

SEED="${SEED:-135}"
DRIVER="${DRIVER:-"${BINDIR:-bin}/driver_test"}"
TEST_RUST="${TEST_RUST:-false}"
UNIT_TEST_DRIVER="${UNIT_TEST_DRIVER:-"${BINDIR:-bin}/unit_test"}"

# Run all the unit-tests first, to get basic coverage
echo
"$UNIT_TEST_DRIVER"
UNIT_TESTS_DB_DEV="unit_tests_db"
if [ -f ${UNIT_TESTS_DB_DEV} ]; then
    rm ${UNIT_TESTS_DB_DEV}
fi

echo
"$DRIVER" kvstore_test --seed "$SEED"

echo
"$DRIVER" kvstore_basic_test --seed "$SEED"

echo
"$DRIVER" splinter_test --functionality 1000000 100 --seed "$SEED"

echo
"$DRIVER" splinter_test --perf --max-async-inflight 0 --num-insert-threads 4 --num-lookup-threads 4 --num-range-lookup-threads 0 --tree-size-gib 2 --cache-capacity-mib 512

echo
"$DRIVER" cache_test --seed "$SEED"

echo
"$DRIVER" btree_test --seed "$SEED"

echo
"$DRIVER" log_test --seed "$SEED"

echo
"$DRIVER" filter_test --seed "$SEED"

"$DRIVER" util_test --seed "$SEED"
echo

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
