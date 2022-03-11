# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := all

PLATFORM = linux
PLATFORM_DIR = platform_$(PLATFORM)

#*************************************************************#
# DIRECTORIES, SRC, OBJ, ETC
#
SRCDIR               = src
TESTS_DIR            = tests
INCDIR               = include
FUNCTIONAL_TESTSDIR  = $(TESTS_DIR)/functional
UNITDIR              = unit
UNIT_TESTSDIR        = $(TESTS_DIR)/$(UNITDIR)

SRC := $(shell find $(SRCDIR) -name "*.c")

# Generate list of common test source files, only from tests/ dir. Hence '-maxdepth 1'.
# These objects are shared between functional/ and unit/ test binaries.
COMMON_TESTSRC := $(shell find $(TESTS_DIR) -maxdepth 1 -name "*.c")

FUNCTIONAL_TESTSRC := $(shell find $(FUNCTIONAL_TESTSDIR) -name "*.c")

# Symbol for all unit-test sources, from which we will build standalone
# unit-test binaries.
UNIT_TESTSRC := $(shell find $(UNIT_TESTSDIR) -name "*.c")

# Some unit-tests which are slow will be skipped from this list, as we want the
# resulting unit_test to run as fast as it can. For now, we are just skipping one
# test, which will have to be run stand-alone.
FAST_UNIT_TESTSRC := $(shell find $(UNIT_TESTSDIR) -name "*.c" | egrep -v -e"splinter_test")

#*************************************************************#
# CFLAGS, ETC
#

INCLUDE = -I $(INCDIR) -I $(SRCDIR) -I $(SRCDIR)/platform_$(PLATFORM) -I $(TESTS_DIR)

DEFAULT_CFLAGS += -D_GNU_SOURCE -ggdb3 -Wall -pthread -Wfatal-errors -Werror -Wvla
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

# use += here, so that extra flags can be provided via the environment
DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS)
DEFAULT_LDFLAGS += -ggdb3 -pthread

# ##########################################################################
# To set sanitiziers, use environment variables, e.g.
#   DEFAULT_CFLAGS="-fsanitize=address" DEFAULT_LDFLAGS="-fsanitize=address" make debug
#
# Note(s):
#  - Address sanitizer builds: -fsanitize=address
#     - Ctests will be silently skipped with clang builds. (Known issue.)
#       Use gcc to build in Asan mode to run unit-tests.
#     - Tests will run slow in address sanitizer builds.
#
#  - Memory sanitizer builds: -fsanitize=memory
#     - Builds will fail with gcc due to compiler error. Use clang instead.
#     - Tests will run even slower in memory sanitizer builds.
#
LIBS      = -lm -lpthread -laio -lxxhash $(LIBCONFIG_LIBS)
DEPFLAGS  = -MMD -MP

#*************************************************************#
# Flags to select release vs debug builds

ifdef D
CFLAGS  += -g -DSPLINTER_DEBUG $(DEFAULT_CFLAGS)
LDFLAGS += -g $(DEFAULT_LDFLAGS)
BUILD_SUFFIX=-debug
else ifdef DL
CFLAGS  += -g -DDEBUG -DCC_LOG $(DEFAULT_CFLAGS)
LDFLAGS += -g $(DEFAULT_LDFLAGS)
BUILD_SUFFIX=-debug
else
CFLAGS   += $(DEFAULT_CFLAGS) -Ofast -flto
LDFLAGS  += $(DEFAULT_LDFLAGS) -Ofast -flto
BUILD_SUFFIX=
endif

ifdef V
VERBOSE=
SUMMARY=@ >/dev/null echo
FORMATTED_SUMMARY=@ >/dev/null echo
PARTIAL=@ >/dev/null echo
else
VERBOSE=@
SUMMARY=@echo
FORMATTED_SUMMARY=@printf
PARTIAL=@echo -n
endif

###################################################################
# Put all build objects into a directory based on the exact parameters
# to this build.
#
OBJDIR    = obj$(BUILD_SUFFIX)
BINDIR    = bin$(BUILD_SUFFIX)
LIBDIR    = lib$(BUILD_SUFFIX)

OBJ := $(SRC:%.c=$(OBJDIR)/%.o)

# Objects from test sources in tests/ that are shared by functional/ and unit/ tests
COMMON_TESTOBJ= $(COMMON_TESTSRC:%.c=$(OBJDIR)/%.o)

# Objects from test sources in tests/functional/ sub-dir
FUNCTIONAL_TESTOBJ= $(FUNCTIONAL_TESTSRC:%.c=$(OBJDIR)/%.o)

# Objects from unit-test sources in tests/unit/ sub-dir, for fast unit-tests
# Resolves to a list: obj/tests/unit/a.o obj/tests/unit/b.o obj/tests/unit/c.o
FAST_UNIT_TESTOBJS= $(FAST_UNIT_TESTSRC:%.c=$(OBJDIR)/%.o)

# ----
# Binaries from unit-test sources in tests/unit/ sub-dir
# Although the sources are in, say, tests/unit/splinterdb_quick_test.c, and so on
# the binaries are named bin/unit/splinterdb_quick_test.
# Also, there may be other shared .c files that don't yield a standalone
# binary. Hence, only build a list from files named *_test.c
# Resolves to a list: bin/unit/a_test bin/unit/b_test bin/unit/c_test ...
UNIT_TESTBIN_SRC=$(filter %_test.c, $(UNIT_TESTSRC))
UNIT_TESTBINS=$(UNIT_TESTBIN_SRC:$(TESTS_DIR)/%_test.c=$(BINDIR)/%_test)

####################################################################
# The main targets
#

all: libs tests $(EXTRA_TARGETS)

libs: $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a

tests: $(BINDIR)/driver_test $(BINDIR)/unit_test $(UNIT_TESTBINS)

##########################################################
# Now define our compile, linking, and archive commands

COMPILE.c = $(CC) $(DEPFLAGS) -MT $@ -MF $(OBJDIR)/$*.d $(CFLAGS) $(INCLUDE) $(TARGET_ARCH) -c

# LD and AR are fine as-is

#######################################################################
# Save a hash of the config we used to perform the build and check for
# any mismatched config from a prior build, so we can ensure we never
# accidentially build using a mixture of configs

CONFIG_HASH=$(shell echo $(CC) $(DEPFLAGS) $(CFLAGS) $(INCLUDE) $(TARGET_ARCH) $(LD) $(LDFLAGS) $(LIBS) $(AR) | md5sum | cut -f1 -d" ")
CONFIG_FILE_PREFIX=.Makefile.config$(BUILD_SUFFIX).
CONFIG_FILE=$(CONFIG_FILE_PREFIX)$(CONFIG_HASH)

.PHONY: mismatched_config_file_check
mismatched_config_file_check:
	$(PARTIAL) Checking for mismatched config...
	$(VERBOSE) ls $(CONFIG_FILE_PREFIX)* 2>/dev/null | \
						 grep -v $(CONFIG_FILE) | \
						 xargs -ri sh -c 'echo "Mismatched config file \"{}\" detected.  " \
																	 "You need to \"make clean\"."; false'
	$(SUMMARY) No mismatched config found


$(CONFIG_FILE): | mismatched_config_file_check
	$(SUMMARY) Saving config to $@
	$(VERBOSE) echo CC          = $(CC)          >> $@
	$(VERBOSE) echo DEPFLAGS    = $(DEPFLAGS)    >> $@
	$(VERBOSE) echo CFLAGS      = $(CFLAGS)      >> $@
	$(VERBOSE) echo INCLUDE     = $(INCLUDE)     >> $@
	$(VERBOSE) echo TARGET_ARCH = $(TARGET_ARCH) >> $@
	$(VERBOSE) echo LD          = $(LD)          >> $@
	$(VERBOSE) echo LDFLAGS     = $(LDFLAGS)     >> $@
	$(VERBOSE) echo LIBS        = $(LIBS)        >> $@
	$(VERBOSE) echo AR          = $(AR)          >> $@


#************************************************************#
# Automatically create directories, based on
# http://ismail.badawi.io/blog/2017/03/28/automatic-directory-creation-in-make/
.SECONDEXPANSION:

.SECONDARY:

%/.: $(CONFIG_FILE)
	$(VERBOSE) mkdir -p $@

# These targets prevent circular dependencies arising from the
# recipe for building binaries
$(BINDIR)/.: $(CONFIG_FILE)
	$(VERBOSE) mkdir -p $@

$(BINDIR)/%/.: $(CONFIG_FILE)
	$(VERBOSE) mkdir -p $@

#*************************************************************#
# RECIPES
#

$(OBJDIR)/%.o: %.c | $$(@D)/.
	$(FORMATTED_SUMMARY) "%-20s %-40s [%s]\n" COMPILING $< $@
	$(VERBOSE) $(COMPILE.c) $< -o $@

$(BINDIR)/%: | $$(@D)/.
	$(FORMATTED_SUMMARY) "%-20s %s\n" LINKING $@
	$(VERBOSE) $(LD) $(LDFLAGS) -o $@ $^ $(LIBS)

$(LIBDIR)/libsplinterdb.so : $(OBJ) | $$(@D)/.
	$(FORMATTED_SUMMARY) "%-20s %s\n" LINKING $@
	$(VERBOSE) $(LD) $(LDFLAGS) -shared -o $@ $^ $(LIBS)

# -c: Create an archive if it does not exist. -r, replacing objects
# -s: Create/update an index to the archive
$(LIBDIR)/libsplinterdb.a : $(OBJ) | $$(@D)/.
	$(FORMATTED_SUMMARY) "%-20s %s\n" "BUILDING ARCHIVE" $@
	$(VERBOSE) $(AR) -crs $@ $^

#################################################################
# Dependencies
#

# Automatically generated .o dependencies on .c and .h files
-include $(SRC:%.c=$(OBJDIR)/%.d) $(TESTSRC:%.c=$(OBJDIR)/%.d)

# Dependencies for the main executables
$(BINDIR)/driver_test: $(FUNCTIONAL_TESTOBJ) $(COMMON_TESTOBJ) $(LIBDIR)/libsplinterdb.so
$(BINDIR)/unit_test: $(FAST_UNIT_TESTOBJS) $(COMMON_TESTOBJ) $(LIBDIR)/libsplinterdb.so $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o

#################################################################
# Dependencies for the mini unit tests
# Each mini unit test is a self-contained binary.
# It links only with its needed .o files
#

# all the mini unit tests depend on these files
$(UNIT_TESTBINS): $(OBJDIR)/$(UNIT_TESTSDIR)/main.o

# Every unit test of the form bin/unit/<test> depends on obj/tests/unit/<test>.o
#
# We can't use pattern rules to state this dependency pattern because
# dependencies specified using pattern rules get overwritten by
# dependencies in explicit rules, which we will use below to give the
# dependencies of each individual unit test.  So we use "eval" to
# turn the dependency pattern into explicit dependencies.
#
# Given <x>, this macro generates a line of Makefile of the form
# bin/unit/<x>: obj/unit/<x>.o
define unit_test_self_dependency =
$(1): $(patsubst $(BINDIR)/$(UNITDIR)/%,$(OBJDIR)/$(UNIT_TESTSDIR)/%.o, $(1))
endef
# See https://www.gnu.org/software/make/manual/html_node/Eval-Function.html
$(foreach unit,$(UNIT_TESTBINS),$(eval $(call unit_test_self_dependency,$(unit))))

# Variables defining the dependency graph of the .o files from src.
# This is used to specify the dependencies of the individual unit
# tests.
#
# These will need to be fleshed out for filters, io subsystem, trunk,
# etc. as we create mini unit test executables for those subsystems.
PLATFORM_SYS = $(OBJDIR)/$(SRCDIR)/$(PLATFORM_DIR)/platform.o

PLATFORM_IO_SYS = $(OBJDIR)/$(SRCDIR)/$(PLATFORM_DIR)/laio.o

UTIL_SYS = $(OBJDIR)/$(SRCDIR)/util.o $(PLATFORM_SYS)

CLOCKCACHE_SYS = $(OBJDIR)/$(SRCDIR)/clockcache.o	  \
                 $(OBJDIR)/$(SRCDIR)/rc_allocator.o \
                 $(OBJDIR)/$(SRCDIR)/task.o         \
                 $(UTIL_SYS)                        \
                 $(PLATFORM_IO_SYS)

BTREE_SYS = $(OBJDIR)/$(SRCDIR)/btree.o           \
            $(OBJDIR)/$(SRCDIR)/data_internal.o   \
            $(OBJDIR)/$(SRCDIR)/mini_allocator.o  \
            $(CLOCKCACHE_SYS)

#################################################################
# The dependencies of each mini unit test.
#
# Note each test bin/unit/<x> also depends on obj/unit/<x>.o, as
# defined above using unit_test_self_dependency.
#
$(BINDIR)/$(UNITDIR)/misc_test: $(UTIL_SYS)

$(BINDIR)/$(UNITDIR)/util_test: $(UTIL_SYS)

$(BINDIR)/$(UNITDIR)/btree_test: $(OBJDIR)/$(UNIT_TESTSDIR)/btree_test_common.o \
                                 $(OBJDIR)/$(TESTS_DIR)/config.o                \
                                 $(OBJDIR)/$(TESTS_DIR)/test_data.o             \
                                 $(BTREE_SYS)

$(BINDIR)/$(UNITDIR)/btree_stress_test: $(OBJDIR)/$(UNIT_TESTSDIR)/btree_test_common.o  \
                                        $(OBJDIR)/$(TESTS_DIR)/config.o                 \
                                        $(OBJDIR)/$(TESTS_DIR)/test_data.o              \
                                        $(BTREE_SYS)

$(BINDIR)/$(UNITDIR)/splinter_test: $(COMMON_TESTOBJ)                             \
                                    $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                    $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/splinterdb_quick_test: $(COMMON_TESTOBJ)                             \
                                      $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                      $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/splinterdb_stress_test: $(COMMON_TESTOBJ)                             \
                                                $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                                $(LIBDIR)/libsplinterdb.so

########################################
# Convenience targets
unit/util_test:                    $(BINDIR)/$(UNITDIR)/util_test
unit/misc_test:                    $(BINDIR)/$(UNITDIR)/misc_test
unit/btree_test:                   $(BINDIR)/$(UNITDIR)/btree_test
unit/btree_stress_test:            $(BINDIR)/$(UNITDIR)/btree_stress_test
unit/splinter_test:                $(BINDIR)/$(UNITDIR)/splinter_test
unit/splinterdb_quick_test:        $(BINDIR)/$(UNITDIR)/splinterdb_quick_test
unit/splinterdb_stress_test:       $(BINDIR)/$(UNITDIR)/splinterdb_stress_test
unit_test:                         $(BINDIR)/unit_test

#*************************************************************#

# Report build machine details and compiler version for troubleshooting, so
# we see this output for clean builds, especially in CI-jobs.
.PHONY : clean tags
clean :
	rm -rf $(OBJDIR) $(LIBDIR) $(BINDIR) $(CONFIG_FILE_PREFIX)*
	uname -a
	$(CC) --version
tags:
	ctags -R $(SRCDIR)


#*************************************************************#
# Testing
#

.PHONY: install

run-tests: $(BINDIR)/driver_test $(BINDIR)/unit_test
	./test.sh

test-results: $(BINDIR)/driver_test $(BINDIR)/unit_test
	(./test.sh > ./test-results.out 2>&1 &) && echo "tail -f ./test-results.out "

INSTALL_PATH ?= /usr/local

install: $(LIBDIR)/libsplinterdb.so
	mkdir -p $(INSTALL_PATH)/include/splinterdb $(INSTALL_PATH)/lib

	# -p retains the timestamp of the file being copied over
	cp -p $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a $(INSTALL_PATH)/lib
	cp -p $(INCDIR)/splinterdb/*.h $(INSTALL_PATH)/include/splinterdb/
