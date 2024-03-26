# Execute this file to build everything.
export COMPILER=gcc
export CC=$COMPILER
export LD=$COMPILER
make clean
make BUILD_MODE=debug