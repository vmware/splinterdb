#!/bin/bash

# Script to regenerate the Rust declarations based on C header files

set -e -u -o pipefail
cd "$(dirname "$0")"

splinter_src="$(cd ../../.. && pwd)"
include_dir="${splinter_src}/include"

bindgen include.h -o generated.rs \
    --size_t-is-usize \
    --no-copy 'kvstore_basic.*' \
    --allowlist-type 'kvstore_basic.*' \
    --allowlist-type key_comparator_fn \
    --allowlist-function 'kvstore_basic.*' \
    --allowlist-var 'KVSTORE_BASIC.*' \
    --allowlist-var '.*_SIZE' \
    -- -I "${include_dir}"
