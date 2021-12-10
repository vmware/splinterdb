# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := release

PLATFORM = linux

#*************************************************************#
# DIRECTORIES, SRC, OBJ, ETC
#
SRCDIR               = src
FUNCTIONAL_TESTSDIR  = tests/functional
UNITDIR              = unit
UNIT_TESTSDIR        = tests/unit
OBJDIR               = obj
BINDIR               = bin
LIBDIR               = lib
INCDIR               = include

SRC := $(shell find $(SRCDIR) -name "*.c")
FUNCTIONAL_TESTSRC := $(shell find $(FUNCTIONAL_TESTSDIR) -name "*.c")
UNITSRC := $(shell find $(UNITDIR) -name "*.c")
UNIT_TESTSRC := $(shell find $(UNIT_TESTSDIR) -name "*.c")

OBJ := $(SRC:%.c=$(OBJDIR)/%.o)

# Objects from test sources in tests/functional/ sub-dir
FUNCTIONAL_TESTOBJ= $(FUNCTIONAL_TESTSRC:%.c=$(OBJDIR)/%.o)

# Objects from unit-test sources in tests/unit/ sub-dir
# Resolves to a list: obj/tests/unit/a.o obj/tests/unit/b.o ...
UNIT_TESTOBJS= $(UNIT_TESTSRC:%.c=$(OBJDIR)/%.o)

# Binaries from unit-test sources in tests/unit/ sub-dir
# Although the sources are in, say, tests/unit/kvstore_basic_test.c, and so on ...
# the binaries are named bin/unit/kvstore_basic_test (Drop the 'tests'.)
# Resolves to a list: bin/unit/a bin/unit/b ...
UNIT_TESTBINS= $(UNIT_TESTSRC:tests/%.c=$(BINDIR)/%)

UNITBINS= $(UNITSRC:%.c=%)

# Automatically create directories, based on
# http://ismail.badawi.io/blog/2017/03/28/automatic-directory-creation-in-make/
.SECONDEXPANSION:

.SECONDARY:

$(OBJDIR)/. $(BINDIR)/. $(LIBDIR)/.:
	mkdir -p $@

$(OBJDIR)/%/.:
	mkdir -p $@

$(BINDIR)/%/.:
	mkdir -p $@

#*************************************************************#
# CFLAGS, ETC
#

INCLUDE = -I $(INCDIR) -I $(SRCDIR) -I $(SRCDIR)/platform_$(PLATFORM)

#######BEGIN libconfig
# Get output of `pkg-config --(cflags|libs) libconfig` every time we run make,
# but if the pkg-config call fails we need to quit make.
.PHONY: .libconfig.mk
.libconfig.mk:
	@rm -f $@
	@echo -n "LIBCONFIG_CFLAGS = " >> $@
	@pkg-config --cflags libconfig >> $@
	@echo -n "LIBCONFIG_LIBS = " >> $@
	@pkg-config --libs libconfig >> $@
include .libconfig.mk
#######END libconfig

DEFAULT_CFLAGS += -D_GNU_SOURCE -ggdb3 -Wall -pthread -Wfatal-errors -Werror
DEFAULT_CFLAGS += -DXXH_STATIC_LINKING_ONLY -fPIC

# track git ref in the built library
GIT_VERSION := "$(shell git describe --abbrev=8 --dirty --always --tags)"
DEFAULT_CFLAGS += -DGIT_VERSION=\"$(GIT_VERSION)\"

cpu_arch := $(shell uname -p)
ifeq ($(cpu_arch),x86_64)
  # not supported on ARM64
  DEFAULT_CFLAGS += -msse4.2 -mpopcnt
  CFLAGS += -march=native
endif
#DEFAULT_CFLAGS += -fsanitize=memory -fsanitize-memory-track-origins
#DEFAULT_CFLAGS += -fsanitize=address
#DEFAULT_CFLAGS += -fsanitize=integer
DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS)


CFLAGS += $(DEFAULT_CFLAGS) -Ofast -flto
DEFAULT_LDFLAGS = -ggdb3 -pthread
#DEFAULT_LDFLAGS += -fsanitize=memory
#DEFAULT_LDFLAGS += -fsanitize=address
#DEFAULT_LDFLAGS += -fsanitize=integer
LDFLAGS = $(DEFAULT_LDFLAGS) -Ofast -flto
LIBS = -lm -lpthread -laio -lxxhash $(LIBCONFIG_LIBS)


#*********************************************************#
# Targets to track whether we have a release or debug build
#
all: $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a $(BINDIR)/driver_test $(UNITBINS) \
        unit_test

# Any libraries required to link test code will be built, if needed.
tests: $(BINDIR)/driver_test $(UNITBINS) unit_test

release: .release all
	rm -f .debug
	rm -f .debug-log

debug: CFLAGS = -g -DSPLINTER_DEBUG $(DEFAULT_CFLAGS)
debug: LDFLAGS = -g $(DEFAULT_LDFLAGS)
debug: .debug all
	rm -f .release
	rm -f .debug-log

debug-log: CFLAGS = -g -DDEBUG -DCC_LOG $(DEFAULT_CFLAGS)
debug-log: LDFLAGS = -g $(DEFAULT_LDFLAGS)
debug-log: .debug-log all
	rm -f .release
	rm -f .debug

.release:
	$(MAKE) clean
	touch .release

.debug:
	$(MAKE) clean
	touch .debug

.debug-log:
	$(MAKE) clean
	touch .debug-log


#*************************************************************#
# RECIPES
#

$(BINDIR)/driver_test : $(FUNCTIONAL_TESTOBJ) $(LIBDIR)/libsplinterdb.so | $$(@D)/.
	$(LD) $(LDFLAGS) -o $@ $^ $(LIBS)

# Target will build everything needed to generate bin/unit_test along with all
# the individual binaries for each unit-test case.
unit_test: $(UNIT_TESTBINS) $(LIBDIR)/libsplinterdb.so
	$(LD) $(LDFLAGS) -o $(BINDIR)/$@ $(UNIT_TESTOBJS) $(LIBDIR)/libsplinterdb.so $(LIBS)

$(LIBDIR)/libsplinterdb.so : $(OBJ) | $$(@D)/.
	$(LD) $(LDFLAGS) -shared -o $@ $^ $(LIBS)

# -c: Create an archive if it does not exist. -r, replacing objects
# -s: Create/update an index to the archive
$(LIBDIR)/libsplinterdb.a : $(OBJ) | $$(@D)/.
	$(AR) -crs $@ $^

$(BINDIR)/unit/%: $(OBJDIR)/unit/%.o | $$(@D)/.
	$(LD) $(LDFLAGS) -o $@ $^ $(LIBS)

DEPFLAGS = -MMD -MT $@ -MP -MF $(OBJDIR)/$*.d

COMPILE.c = $(CC) $(DEPFLAGS) $(CFLAGS) $(INCLUDE) $(TARGET_ARCH) -c

$(OBJDIR)/%.o: %.c | $$(@D)/.
	$(COMPILE.c) $< -o $@

-include $(SRC:%.c=$(OBJDIR)/%.d) $(TESTSRC:%.c=$(OBJDIR)/%.d)

# ###########################################################################
# Unit test dependencies
#
# Each unit test is a self-contained binary.
# It links only with its needed .o files
# ###########################################################################

# List the individual unit-tests that can be run standalone and are also
# rolled-up into a single unit_test binary.

$(OBJDIR)/unit/variable_length_btree-test.o: src/variable_length_btree.c
unit/variable_length_btree-test: $(OBJDIR)/tests/functional/test_data.o     \
                                 $(OBJDIR)/src/util.o                       \
                                 $(OBJDIR)/src/data_internal.o              \
                                 $(OBJDIR)/src/mini_allocator.o             \
                                 $(OBJDIR)/src/rc_allocator.o               \
                                 $(OBJDIR)/src/config.o                     \
                                 $(OBJDIR)/src/clockcache.o                 \
                                 $(OBJDIR)/src/platform_linux/platform.o    \
                                 $(OBJDIR)/src/task.o                       \
                                 $(OBJDIR)/src/platform_linux/laio.o        \
                                 $(OBJDIR)/src/platform_linux/platform.o
	mkdir -p $(BINDIR)/unit;
	$(LD) $(LDFLAGS) -shared $^ -o $(BINDIR)/$@

# ---- main() is needed to drive each standalone unit-test binary.
$(BINDIR)/unit/main: $(OBJDIR)/tests/unit/main.o
$(OBJDIR)/tests/unit/main.o: tests/unit/main.c

# ---- Here onwards, list the rules to build each standalone unit-test binary.
$(BINDIR)/unit/kvstore_basic_test: unit/kvstore_basic_test
unit/kvstore_basic_test: $(OBJDIR)/tests/unit/kvstore_basic_test.o        \
                         $(OBJDIR)/tests/unit/main.o                      \
                         $(LIBDIR)/libsplinterdb.so
	mkdir -p $(BINDIR)/unit;
	$(LD) $(LDFLAGS) -o $(BINDIR)/$@ $^ $(LIBS)

# ----
$(BINDIR)/unit/kvstore_basic_stress_test: unit/kvstore_basic_stress_test
unit/kvstore_basic_stress_test: $(OBJDIR)/tests/unit/kvstore_basic_stress_test.o       \
                                $(OBJDIR)/tests/unit/main.o                      \
                                $(LIBDIR)/libsplinterdb.so
	mkdir -p $(BINDIR)/unit;
	$(LD) $(LDFLAGS) -o $(BINDIR)/$@ $^ $(LIBS)

#*************************************************************#

.PHONY : clean tags
clean :
	rm -rf $(OBJDIR)/* $(BINDIR)/* $(LIBDIR)/*

tags:
	ctags -R src


#*************************************************************#
# Testing
#

.PHONY: test install

run-tests: $(BINDIR)/driver_test $(BINDIR)/unit_test
	./test.sh

test-results: $(BINDIR)/driver_test $(BINDIR)/unit_test
	(./test.sh > ./test-results.out 2>&1 &) && echo "tail -f ./test-results.out "

INSTALL_PATH ?= /usr/local

install: $(LIBDIR)/libsplinterdb.so
	mkdir -p $(INSTALL_PATH)/include/splinterdb $(INSTALL_PATH)/lib

	# -p retains the timestamp of the file being copied over$
	cp -p $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a $(INSTALL_PATH)/lib
	cp -p $(INCDIR)/splinterdb/*.h $(INSTALL_PATH)/include/splinterdb/
