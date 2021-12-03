# ############################################################################
# Makefile: CTest test code files
#
# History: Originated from the ctest repo at https://github.com/bvdberg/ctest
# customized for SplinterDB use.
# ############################################################################
#
# Expect that CC env-variable is set externally to either clang or gcc-version
CCFLAGS=-Wall -Wextra -Wconversion -Wredundant-decls -Wshadow -Wno-unused-parameter -O3

all: test

remake: clean all

%.o: %.c ctest.h
	$(CC) $(CCFLAGS) -c -o $@ $<

test: main.o ctest.h mytests.o
	$(CC) $(LDFLAGS)            \
    main.o mytests.o            \
    -o test

clean:
	rm -f test *.o
