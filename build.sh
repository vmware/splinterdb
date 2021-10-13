export COMPILER=gcc-9

export CC=$COMPILER
export LD=$COMPILER
make clean
make

rm db
rm /mnt/pmem0/splinter

./bin/driver_test splinter_test --perf\
				--max-async-inflight 0\
				--db-capacity-gib 60\
				--cache-file "/mnt/pmem0/splinter"\
				--stats

