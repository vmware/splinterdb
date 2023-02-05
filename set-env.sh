export CC=clang
export LD=clang
export LD_LIBRARY_PATH=$(pwd)/build/release/lib:$(pwd)/third-party/iceberghashtable:${LD_LIBRARY_PATH}
export LIBRARY_PATH=$(pwd)/build/release/lib:$(pwd)/third-party/iceberghashtable:${LIBRARY_PATH}
export CPATH=$(pwd)/include:$(pwd)/third-party/iceberghashtable/include:${CPATH}
