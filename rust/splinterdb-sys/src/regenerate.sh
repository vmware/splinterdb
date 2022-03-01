#!/bin/bash

# Script to regenerate the Rust declarations based on C header files

set -e -u -o pipefail
cd "$(dirname "$0")"

splinter_src="$(cd ../../.. && pwd)"
include_dir="${splinter_src}/include"

bindgen include.h -o generated.rs \
    --size_t-is-usize \
    --no-copy 'splinterdb.*' \
    --no-copy 'writable_buffer' \
    --no-copy 'data_config' \
    --allowlist-type 'splinterdb.*' \
    --allowlist-function 'splinterdb.*' \
    --allowlist-function 'default_data_config.*' \
    --allowlist-var 'SPLINTERDB.*' \
    --allowlist-var '.*_SIZE' \
    -- -I "${include_dir}"
