# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := all

PLATFORM = linux
PLATFORM_DIR = platform_$(PLATFORM)

#*************************************************************#
# SOURCE DIRECTORIES AND FILES
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
TESTSRC := $(COMMON_TESTSRC) $(FUNCTIONAL_TESTSRC) $(UNIT_TESTSRC)

# Some unit-tests which are slow will be skipped from this list, as we want the
# resulting unit_test to run as fast as it can. For now, we are just skipping one
# test, which will have to be run stand-alone.
FAST_UNIT_TESTSRC := $(shell find $(UNIT_TESTSDIR) -name "*.c" | egrep -v -e"splinter_test")

#*************************************************************#
# CFLAGS, LDFLAGS, ETC
#

INCLUDE = -I $(INCDIR) -I $(SRCDIR) -I $(SRCDIR)/platform_$(PLATFORM) -I $(TESTS_DIR)

# use += here, so that extra flags can be provided via the environment

CFLAGS += -D_GNU_SOURCE -ggdb3 -Wall -pthread -Wfatal-errors -Werror -Wvla
CFLAGS += -DXXH_STATIC_LINKING_ONLY -fPIC
CFLAGS += -DSPLINTERDB_PLATFORM_DIR=$(PLATFORM_DIR)

# track git ref in the built library
GIT_VERSION := "$(shell git describe --abbrev=8 --dirty --always --tags)"
CFLAGS += -DGIT_VERSION=\"$(GIT_VERSION)\"

cpu_arch := $(shell uname -p)
ifeq ($(cpu_arch),x86_64)
  # not supported on ARM64
  CFLAGS += -march=native
endif

LDFLAGS += -ggdb3 -pthread

LIBS      = -lm -lpthread -laio -lxxhash
DEPFLAGS  = -MMD -MP

#*************************************************************#
# Flags to select release vs debug builds, verbosity, etc.
#

help::
	$(SUMMARY) Environment variables controlling the build
	$(SUMMARY) '  BUILD_SUFFIX: Base suffix for build output.'
	$(SUMMARY) '    Objects go into obj-$$(BUILD_SUFFIX)'
	$(SUMMARY) '    Libraries go into lib-$$(BUILD_SUFFIX)'
	$(SUMMARY) '    Executables go into bin-$$(BUILD_SUFFIX)'
	$(SUMMARY) '    Note: setting DEBUGMODE and other flags may further modify the suffix'

#
# Debug mode
#
ifndef DEBUGMODE
DEBUGMODE=release
endif

ifeq "$(DEBUGMODE)" "debug"
CFLAGS  += -DSPLINTER_DEBUG
#LDFLAGS +=
BUILD_SUFFIX:=$(BUILD_SUFFIX)-debug
else ifeq "$(DEBUGMODE)" "release"
CFLAGS   += -Ofast -flto
LDFLAGS  += -Ofast -flto
else
$(error Unknown DEBUGMODE "$(DEBUGMODE)".  Valid options are "debug" and "release".  Default is "release")
endif

help::
	$(SUMMARY) '  DEBUGMODE: "release" or "debug" (Default: "release")'

#
# address sanitizer
#
ifndef ASAN
ASAN=0
endif

ifeq "$(ASAN)" "1"
CFLAGS  += -fsanitize=address
LDFLAGS += -fsanitize=address
BUILD_SUFFIX:=$(BUILD_SUFFIX)-asan
else ifeq "$(ASAN)" "0"
else
$(error Unknown ASAN mode "$(ASAN)".  Valid values are "0" or "1". Default is "0")
endif

help::
	$(SUMMARY) '  ASAN={0,1}: Disable/enable address-sanitizer (Default: disabled)'

#
# memory sanitizer
#
ifndef MSAN
MSAN=0
endif

ifeq "$(MSAN)" "1"
CFLAGS  += -fsanitize=memory
LDFLAGS += -fsanitize=memory
BUILD_SUFFIX:=$(BUILD_SUFFIX)-msan
else ifeq "$(MSAN)" "0"
else
$(error Unknown MSAN mode "$(MSAN)".  Valid values are "0" or "1". Default is "0")
endif

help::
	$(SUMMARY) '  MSAN={0,1}: Disable/enable memory-sanitizer (Default: disabled)'

#
# Verbosity
#
ifndef VERBOSE
VERBOSE=0
endif

ifeq "$(VERBOSE)" "1"
COMMAND=
PROLIX=@echo
BRIEF=@ >/dev/null echo
BRIEF_FORMATTED=@ >/dev/null echo
BRIEF_PARTIAL=@ >/dev/null echo
else ifeq "$(VERBOSE)" "0"
COMMAND=@
PROLIX=@ >/dev/null echo
BRIEF=@echo
BRIEF_FORMATTED=@printf
BRIEF_PARTIAL=@echo -n
else
$(error Unknown VERBOSE mode "$(VERBOSE)".  Valid values are "0" or "1". Default is "0")
endif

help::
	$(SUMMARY) '  VERBOSE={0,1}: Disable/enable verbose output (Default: disabled)'


###################################################################
# BUILD DIRECTORIES AND FILES
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

#######################################################################
# CONFIGURATION CHECKING
#
# Save a hash of the config we used to perform the build and check for
# any mismatched config from a prior build, so we can ensure we never
# accidentially build using a mixture of configs

CONFIG_HASH=$(shell echo $(CC) $(DEPFLAGS) $(CFLAGS) $(INCLUDE) $(TARGET_ARCH) $(LD) $(LDFLAGS) $(LIBS) $(AR) | md5sum | cut -f1 -d" ")
CONFIG_FILE_PREFIX=.Makefile.config$(BUILD_SUFFIX).
CONFIG_FILE=$(CONFIG_FILE_PREFIX)$(CONFIG_HASH)

.PHONY: mismatched_config_file_check
mismatched_config_file_check:
	$(BRIEF_PARTIAL) Checking for mismatched config...
	$(COMMAND) ls $(CONFIG_FILE_PREFIX)* 2>/dev/null | grep -v $(CONFIG_FILE) | xargs -ri sh -c 'echo "Mismatched config file \"{}\" detected.  You need to \"make clean\"."; false'
	$(BRIEF) No mismatched config found


$(CONFIG_FILE): | mismatched_config_file_check
	$(BRIEF) Saving config to $@
	$(COMMAND) echo CC          = $(CC)          >> $@
	$(COMMAND) echo DEPFLAGS    = $(DEPFLAGS)    >> $@
	$(COMMAND) echo CFLAGS      = $(CFLAGS)      >> $@
	$(COMMAND) echo INCLUDE     = $(INCLUDE)     >> $@
	$(COMMAND) echo TARGET_ARCH = $(TARGET_ARCH) >> $@
	$(COMMAND) echo LD          = $(LD)          >> $@
	$(COMMAND) echo LDFLAGS     = $(LDFLAGS)     >> $@
	$(COMMAND) echo LIBS        = $(LIBS)        >> $@
	$(COMMAND) echo AR          = $(AR)          >> $@


#************************************************************#
# Automatically create directories, based on
# http://ismail.badawi.io/blog/2017/03/28/automatic-directory-creation-in-make/
.SECONDEXPANSION:

.SECONDARY:

%/.: $(CONFIG_FILE)
	$(COMMAND) mkdir -p $@

# These targets prevent circular dependencies arising from the
# recipe for building binaries
$(BINDIR)/.: $(CONFIG_FILE)
	$(COMMAND) mkdir -p $@

$(BINDIR)/%/.: $(CONFIG_FILE)
	$(COMMAND) mkdir -p $@

#*************************************************************#
# RECIPES
#

COMPILE.c = $(CC) $(DEPFLAGS) -MT $@ -MF $(OBJDIR)/$*.d $(CFLAGS) $(INCLUDE) $(TARGET_ARCH) -c

$(OBJDIR)/%.o: %.c | $$(@D)/.
	$(BRIEF_FORMATTED) "%-20s %-40s [%s]\n" COMPILING $< $@
	$(COMMAND) $(COMPILE.c) $< -o $@
	$(PROLIX) # blank line

$(BINDIR)/%: | $$(@D)/.
	$(BRIEF_FORMATTED) "%-20s %s\n" LINKING $@
	$(COMMAND) $(LD) $(LDFLAGS) -o $@ $^ $(LIBS)
	$(PROLIX) # blank line

$(LIBDIR)/libsplinterdb.so : $(OBJ) | $$(@D)/.
	$(BRIEF_FORMATTED) "%-20s %s\n" LINKING $@
	$(COMMAND) $(LD) $(LDFLAGS) -shared -o $@ $^ $(LIBS)
	$(PROLIX) # blank line

# -c: Create an archive if it does not exist. -r, replacing objects
# -s: Create/update an index to the archive
$(LIBDIR)/libsplinterdb.a : $(OBJ) | $$(@D)/.
	$(BRIEF_FORMATTED) "%-20s %s\n" "BUILDING ARCHIVE" $@
	$(COMMAND) $(AR) -crs $@ $^
	$(PROLIX) # blank line

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

$(BINDIR)/$(UNITDIR)/writable_buffer_test: $(UTIL_SYS)

########################################
# Convenience targets
unit/util_test:                    $(BINDIR)/$(UNITDIR)/util_test
unit/misc_test:                    $(BINDIR)/$(UNITDIR)/misc_test
unit/btree_test:                   $(BINDIR)/$(UNITDIR)/btree_test
unit/btree_stress_test:            $(BINDIR)/$(UNITDIR)/btree_stress_test
unit/splinter_test:                $(BINDIR)/$(UNITDIR)/splinter_test
unit/splinterdb_quick_test:        $(BINDIR)/$(UNITDIR)/splinterdb_quick_test
unit/splinterdb_stress_test:       $(BINDIR)/$(UNITDIR)/splinterdb_stress_test
unit/writable_buffer_test:         $(BINDIR)/$(UNITDIR)/writable_buffer_test
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
	BINDIR=$(BINDIR) ./test.sh

test-results: $(BINDIR)/driver_test $(BINDIR)/unit_test
	(INCLUDE_SLOW_TESTS=true BINDIR=$(BINDIR) ./test.sh > ./test-results.out 2>&1 &) && echo "tail -f ./test-results.out "

INSTALL_PATH ?= /usr/local

install: $(LIBDIR)/libsplinterdb.so
	mkdir -p $(INSTALL_PATH)/include/splinterdb $(INSTALL_PATH)/lib

	# -p retains the timestamp of the file being copied over
	cp -p $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a $(INSTALL_PATH)/lib
	cp -p -r $(INCDIR)/splinterdb/ $(INSTALL_PATH)/include/

# to support clangd: https://clangd.llvm.org/installation.html#compile_flagstxt
.PHONY: compile_flags.txt
compile_flags.txt:
	echo "$(CFLAGS) $(INCLUDE)" | tr ' ' "\n" > compile_flags.txt
