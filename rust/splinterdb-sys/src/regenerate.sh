#!/bin/bash

# Script to regenerate the Rust declarations based on C header files
# You'll need to first
#   cargo install bindgen-cli

set -e -u -o pipefail
cd "$(dirname "$0")"

splinter_src="$(cd ../../.. && pwd)"
include_dir="${splinter_src}/include"

bindgen include.h -o generated.rs \
    --no-copy 'splinterdb.*' \
    --no-copy 'writable_buffer' \
    --no-copy 'data_config' \
    --allowlist-type 'splinterdb.*' \
    --allowlist-function 'splinterdb.*' \
    --allowlist-function 'default_data_config.*' \
    --allowlist-function 'platform_.*' \
    --allowlist-var 'stderr' \
    --allowlist-var 'stdout' \
    -- -I "${include_dir}" \
    -D SPLINTERDB_PLATFORM_DIR=platform_linux
