#!/bin/bash

set -euxo pipefail

SEED="${SEED:-135}"

WITH_RUST="${WITH_RUST:-false}"
TEST_CLI="${TEST_CLI:-false}"

# Run all the unit-tests first, to get basic coverage
bin/unit_test
UNIT_TESTS_DB_DEV="unit_tests_db"
if [ -f ${UNIT_TESTS_DB_DEV} ]; then
    rm ${UNIT_TESTS_DB_DEV}
fi

bin/driver_test kvstore_test --seed "$SEED"

bin/driver_test kvstore_basic_test --seed "$SEED"

bin/driver_test splinter_test --functionality 1000000 100 --seed "$SEED"

bin/driver_test splinter_test --perf --max-async-inflight 0 --num-insert-threads 4 --num-lookup-threads 4 --num-range-lookup-threads 0 --tree-size-gib 2 --cache-capacity-mib 512

bin/driver_test cache_test --seed "$SEED"

bin/driver_test btree_test --seed "$SEED"

bin/driver_test log_test --seed "$SEED"

bin/driver_test filter_test --seed "$SEED"

bin/driver_test util_test --seed "$SEED"

if [ "$WITH_RUST" = "true" ]; then
   pushd rust
      cargo fmt --all -- --check
      cargo build
      cargo test
      cargo clippy -- -D warnings
      cargo build --release
      cargo test --release
   popd
fi

if [ "$TEST_CLI" = "true" ]; then
   bin/splinterdb-cli -f /tmp/splinterdb-rust-test perf -t 4 -w 10000
fi

echo ALL PASSED
