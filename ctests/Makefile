# ############################################################################
# Makefile: CTest test code files
#
# History: Originated from the ctest repo at https://github.com/bvdberg/ctest
# customized for SplinterDB use.
# ############################################################################
#
# Expect that CC env-variable is set externally to either clang or gcc-version
CCFLAGS=-Wall -Wextra -Wconversion -Wredundant-decls -Wshadow -Wno-unused-parameter

all: test

remake: clean all

%.o: %.c ctest.h
	$(CC) -g $(CCFLAGS) -c -o $@ $<

test: main.o ctest.h mytests.o kvstore_basic_test.o
	$(CC) $(LDFLAGS)            \
    main.o mytests.o            \
    kvstore_basic_test.o        \
    -o test

clean:
	rm -f test *.o
