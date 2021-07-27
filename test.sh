#!/bin/bash

set -euxo pipefail

SEED="${SEED:-135}"
DRIVER="${DRIVER:-"${BINDIR:-bin}/driver_test"}"

"$DRIVER" splinter_test --functionality 1000000 100 --seed "$SEED"

"$DRIVER" cache_test --seed "$SEED"

"$DRIVER" btree_test --seed "$SEED"
