# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := all

PLATFORM = linux
PLATFORM_DIR = platform_$(PLATFORM)

help::
	@echo 'Usage: make [<target>]'
	@echo 'Supported targets: clean all libs all-tests run-tests test-results run-examples install'

#*************************************************************#
# SOURCE DIRECTORIES AND FILES
#
SRCDIR               = src
TESTS_DIR            = tests
INCDIR               = include
FUNCTIONAL_TESTSDIR  = $(TESTS_DIR)/functional
UNITDIR              = unit
UNIT_TESTSDIR        = $(TESTS_DIR)/$(UNITDIR)
EXAMPLES_DIR         = examples

# Define a recursive wildcard function to 'find' all files under a sub-dir
# See https://stackoverflow.com/questions/2483182/recursive-wildcards-in-gnu-make/18258352#18258352
define rwildcard =
	$(foreach d,$(wildcard $(1:=/*)),$(call rwildcard,$d,$2) $(filter $(subst *,%,$2),$d))
endef

SRC := $(call rwildcard, $(SRCDIR), *.c)

# Generate list of common test source files, only from tests/ dir.
# These objects are shared between functional/ and unit/ test binaries.
COMMON_TESTSRC := $(wildcard $(TESTS_DIR)/*.c)
FUNCTIONAL_TESTSRC := $(call rwildcard, $(FUNCTIONAL_TESTSDIR), *.c)

# Symbol for all unit-test sources, from which we will build standalone
# unit-test binaries.
UNIT_TESTSRC := $(call rwildcard, $(UNIT_TESTSDIR), *.c)

# Specify the r.e. so it will only pick-up sources that are common to multiple
# unit-tests (and won't pick-up test-specific common files, e.g.
# btree_test_common.c)
COMMON_UNIT_TESTSRC := $(wildcard $(UNIT_TESTSDIR)/*tests_common.c)

TESTSRC := $(COMMON_TESTSRC) $(FUNCTIONAL_TESTSRC) $(UNIT_TESTSRC)

# Some unit-tests will be excluded from the list of dot-oh's that are linked into
# bin/unit_test, for various reasons:
#  - Slow unit-tests will be skipped, as we want the resulting unit_test binary
#    to run as fast as it can.
#  - Skip tests that are to be invoked with specialized command-line arguments.
#
# These tests which are skipped will have to be run stand-alone.
# Construct a list of fast unit-tests that will be linked into unit_test binary,
# eliminating a sequence of slow-running unit-test programs.
ALL_UNIT_TESTSRC := $(call rwildcard, $(UNIT_TESTSDIR), *.c)
SLOW_UNIT_TESTSRC = splinter_test.c config_parse_test.c large_inserts_stress_test.c splinterdb_forked_child_test.c
SLOW_UNIT_TESTSRC_FILTER := $(foreach slowf,$(SLOW_UNIT_TESTSRC), $(UNIT_TESTSDIR)/$(slowf))
FAST_UNIT_TESTSRC := $(sort $(filter-out $(SLOW_UNIT_TESTSRC_FILTER), $(ALL_UNIT_TESTSRC)))

# To examine constructed variable.
# $(info $$FAST_UNIT_TESTSRC is [${FAST_UNIT_TESTSRC}])

EXAMPLES_SRC := $(call rwildcard, $(EXAMPLES_DIR), *.c)

#*************************************************************#
# CFLAGS, LDFLAGS, ETC
#

INCLUDE = -I $(INCDIR) -I $(SRCDIR) -I $(SRCDIR)/platform_$(PLATFORM) -I $(TESTS_DIR)

# use += here, so that extra flags can be provided via the environment

CFLAGS += -D_GNU_SOURCE -ggdb3 -Wall -pthread -Wfatal-errors -Werror -Wvla
CFLAGS += -DXXH_STATIC_LINKING_ONLY -fPIC
CFLAGS += -DSPLINTERDB_PLATFORM_DIR=$(PLATFORM_DIR)

# track git ref in the built library. We don't put this into CFLAGS
# directly because it causes false-positives in our config tracking.
GIT_VERSION := "$(shell git describe --abbrev=8 --dirty --always --tags)"
GIT_VERSION_CFLAGS += -DGIT_VERSION=\"$(GIT_VERSION)\"

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
	@echo Environment variables controlling the build:
	@echo '  BUILD_ROOT: Base dir name for build outputs (Default: "build").'
	@echo '              Build artifacts are created in '
	@echo '                  $$(BUILD_ROOT)/$$(BUILD_MODE)[-asan][-msan]'
	@echo

ifndef BUILD_ROOT
   BUILD_ROOT := build
endif

#
# Build mode
#
ifndef BUILD_MODE
   BUILD_MODE=release
endif
BUILD_DIR := $(BUILD_MODE)

ifeq "$(BUILD_MODE)" "debug"
   CFLAGS    += -DSPLINTER_DEBUG
else ifeq "$(BUILD_MODE)" "release"
   CFLAGS    += -Ofast -flto
   LDFLAGS   += -Ofast -flto
else ifeq "$(BUILD_MODE)" "optimized-debug"
   CFLAGS    += -DSPLINTER_DEBUG
   CFLAGS    += -Ofast -flto
   LDFLAGS   += -Ofast -flto
else
   $(error Unknown BUILD_MODE "$(BUILD_MODE)".  Valid options are "debug", "optimized-debug", and "release".  Default is "release")
endif

help::
	@echo '  BUILD_MODE: "release", "debug", or "optimized-debug" (Default: "release")'

# ************************************************************************
# Address sanitizer
#   - Ctests will be silently skipped with clang builds. (Known issue.)
#   - Use gcc to build in Asan mode to run unit-tests.
#   - Tests will run slow in address sanitizer builds.
ifndef BUILD_ASAN
   BUILD_ASAN=0
endif

ifeq "$(BUILD_ASAN)" "1"
   CFLAGS  += -fsanitize=address
   LDFLAGS += -fsanitize=address
   BUILD_DIR:=$(BUILD_DIR)-asan
else ifneq "$(BUILD_ASAN)" "0"
   $(error Unknown BUILD_ASAN mode "$(BUILD_ASAN)".  Valid values are "0" or "1". Default is "0")
endif

help::
	@echo '  BUILD_ASAN={0,1}: Disable/enable address-sanitizer (Default: disabled)'
	@echo '                    Use gcc to run unit-tests with ASAN-builds.'

# ************************************************************************
# Memory sanitizer
#   - Builds will fail with gcc due to compiler error. Use clang instead.
#   - Tests will run even slower in memory sanitizer builds.
ifndef BUILD_MSAN
   BUILD_MSAN=0
endif

ifeq "$(BUILD_MSAN)" "1"
   CFLAGS  += -fsanitize=memory
   LDFLAGS += -fsanitize=memory
   BUILD_DIR:=$(BUILD_DIR)-msan
else ifneq "$(BUILD_MSAN)" "0"
   $(error Unknown BUILD_MSAN mode "$(BUILD_MSAN)".  Valid values are "0" or "1". Default is "0")
endif

help::
	@echo '  BUILD_MSAN={0,1}: Disable/enable memory-sanitizer (Default: disabled)'
	@echo '                    Use clang for MSAN-builds.'

#
# Verbosity
#
ifndef BUILD_VERBOSE
   BUILD_VERBOSE=0
endif

ifeq "$(BUILD_VERBOSE)" "1"
   COMMAND=
   PROLIX=@echo
   BRIEF=@ >/dev/null echo
   BRIEF_FORMATTED=@ >/dev/null echo
   BRIEF_PARTIAL=@echo -n >/dev/null
else ifeq "$(BUILD_VERBOSE)" "0"
   COMMAND=@
   PROLIX=@ >/dev/null echo
   BRIEF=@echo
   BRIEF_FORMATTED=@printf
   BRIEF_PARTIAL=@echo -n
else
   $(error Unknown BUILD_VERBOSE mode "$(BUILD_VERBOSE)".  Valid values are "0" or "1". Default is "0")
endif

help::
	@echo '  BUILD_VERBOSE={0,1}: Disable/enable verbose output (Default: disabled)'

ifeq "$(BUILD_VERBOSE)" "1"

	@echo '  '
	@echo 'Examples:'
	@echo '  - Default release build, artifacts created under build/release:'
	@echo '     $$ make'
	@echo '  '
	@echo '  - Default debug build, artifacts created under build/debug:'
	@echo '     $$ BUILD_DEBUG=1 make'
	@echo '  '
	@echo '  - Build release binary and run all tests:'
	@echo '     $$ make all run-tests'
	@echo '     $$ make run-tests'
	@echo '  '
	@echo '  - Debug ASAN build, artifacts created under /tmp/build/debug-asan:'
	@echo '     $$ BUILD_ROOT=/tmp/build BUILD_MODE=debug BUILD_ASAN=1 make'
	@echo '  '
endif


###################################################################
# BUILD DIRECTORIES AND FILES
#

BUILD_PATH=$(BUILD_ROOT)/$(BUILD_DIR)

OBJDIR = $(BUILD_PATH)/obj
BINDIR = $(BUILD_PATH)/bin
LIBDIR = $(BUILD_PATH)/lib

OBJ := $(SRC:%.c=$(OBJDIR)/%.o)

# Objects from test sources in tests/ that are shared by functional/ and unit/ tests
COMMON_TESTOBJ= $(COMMON_TESTSRC:%.c=$(OBJDIR)/%.o)

# Objects from unit-test sources in tests/unit that are shared by unit tests
COMMON_UNIT_TESTOBJ= $(COMMON_UNIT_TESTSRC:%.c=$(OBJDIR)/%.o)

# Objects from test sources in tests/functional/ sub-dir
FUNCTIONAL_TESTOBJ= $(FUNCTIONAL_TESTSRC:%.c=$(OBJDIR)/%.o)

# Objects from unit-test sources in tests/unit/ sub-dir, for fast unit-tests
# Resolves to a list: obj/tests/unit/a.o obj/tests/unit/b.o obj/tests/unit/c.o
FAST_UNIT_TESTOBJS := $(FAST_UNIT_TESTSRC:%.c=$(OBJDIR)/%.o)

# ----
# Binaries from unit-test sources in tests/unit/ sub-dir
# Although the sources are in, say, tests/unit/splinterdb_quick_test.c, and so on
# the binaries are named bin/unit/splinterdb_quick_test.
# Also, there may be other shared .c files that don't yield a standalone
# binary. Hence, only build a list from files named *_test.c
# Resolves to a list: bin/unit/a_test bin/unit/b_test bin/unit/c_test ...
UNIT_TESTBIN_SRC=$(filter %_test.c, $(UNIT_TESTSRC))
UNIT_TESTBINS=$(UNIT_TESTBIN_SRC:$(TESTS_DIR)/%_test.c=$(BINDIR)/%_test)

# ---- Symbols to build example sample programs
EXAMPLES_OBJ= $(EXAMPLES_SRC:%.c=$(OBJDIR)/%.o)
EXAMPLES_BIN_SRC=$(filter %_example.c, $(EXAMPLES_SRC))
EXAMPLES_BINS=$(EXAMPLES_BIN_SRC:$(EXAMPLES_DIR)/%_example.c=$(BINDIR)/$(EXAMPLES_DIR)/%_example)

####################################################################
# The main targets
#
all: libs all-tests all-examples $(EXTRA_TARGETS)
libs: $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a
all-tests: $(BINDIR)/driver_test $(BINDIR)/unit_test $(UNIT_TESTBINS)
all-examples: $(EXAMPLES_BINS)

#######################################################################
# CONFIGURATION CHECKING
#
# Save a hash of the config we used to perform the build and check for
# any mismatched config from a prior build, so we can ensure we never
# accidentially build using a mixture of configs

CONFIG_HASH = $(shell echo $(CC) $(DEPFLAGS) $(CFLAGS) $(INCLUDE) $(TARGET_ARCH) $(LD) $(LDFLAGS) $(LIBS) $(AR) | md5sum | cut -f1 -d" ")
CONFIG_FILE_PREFIX = $(BUILD_PATH)/build-config.
CONFIG_FILE = $(CONFIG_FILE_PREFIX)$(CONFIG_HASH)

.PHONY: mismatched_config_file_check
mismatched_config_file_check: | $(BUILD_PATH)/.
	$(BRIEF_PARTIAL) Checking for mismatched config...
	$(COMMAND) ls $(CONFIG_FILE_PREFIX)* 2>/dev/null | grep -v $(CONFIG_FILE) | xargs -rI{} sh -c 'echo "Mismatched config file \"{}\" detected.  You need to \"make clean\"."; false'
	$(BRIEF) No mismatched config found


$(CONFIG_FILE): | $(BUILD_PATH)/. mismatched_config_file_check
	$(BRIEF) Saving config to $@
	$(COMMAND) env | grep -E "BUILD_|CC"         >  $@
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

%/.:
	$(COMMAND) mkdir -p $@

# These targets prevent circular dependencies arising from the
# recipe for building binaries
$(BINDIR)/.:
	$(COMMAND) mkdir -p $@

$(BINDIR)/%/.:
	$(COMMAND) mkdir -p $@

#*************************************************************#
# RECIPES
#

COMPILE.c = $(CC) $(DEPFLAGS) -MT $@ -MF $(OBJDIR)/$*.d $(CFLAGS) $(GIT_VERSION_CFLAGS) $(INCLUDE) $(TARGET_ARCH) -c

$(OBJDIR)/%.o: %.c | $$(@D)/. $(CONFIG_FILE)
	$(BRIEF_FORMATTED) "%-20s %-50s [%s]\n" Compiling $< $@
	$(COMMAND) $(COMPILE.c) $< -o $@
	$(PROLIX) # blank line

$(BINDIR)/%: | $$(@D)/. $(CONFIG_FILE)
	$(BRIEF_FORMATTED) "%-20s %s\n" Linking $@
	$(COMMAND) $(LD) $(LDFLAGS) -o $@ $^ $(LIBS)
	$(PROLIX) # blank line

$(LIBDIR)/libsplinterdb.so : $(OBJ) | $$(@D)/. $(CONFIG_FILE)
	$(BRIEF_FORMATTED) "%-20s %s\n" Linking $@
	$(COMMAND) $(LD) $(LDFLAGS) -shared -o $@ $^ $(LIBS)
	$(PROLIX) # blank line

# -c: Create an archive if it does not exist. -r, replacing objects
# -s: Create/update an index to the archive
$(LIBDIR)/libsplinterdb.a : $(OBJ) | $$(@D)/. $(CONFIG_FILE)
	$(BRIEF_FORMATTED) "%-20s %s\n" "Building archive" $@
	$(COMMAND) $(AR) -crs $@ $^
	$(PROLIX) # blank line

#################################################################
# Dependencies
#

# Automatically generated .o dependencies on .c and .h files
-include $(SRC:%.c=$(OBJDIR)/%.d) $(TESTSRC:%.c=$(OBJDIR)/%.d)

# Dependencies for the main executables
$(BINDIR)/driver_test: $(FUNCTIONAL_TESTOBJ) $(COMMON_TESTOBJ) $(LIBDIR)/libsplinterdb.so
$(BINDIR)/unit_test: $(FAST_UNIT_TESTOBJS) $(COMMON_TESTOBJ) $(COMMON_UNIT_TESTOBJ) $(LIBDIR)/libsplinterdb.so $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o

#################################################################
# Dependencies for the mini unit tests
# Each mini unit test is a self-contained binary.
# It links only with its needed .o files
#

# all the mini unit tests depend on these files
$(UNIT_TESTBINS): $(OBJDIR)/$(UNIT_TESTSDIR)/main.o

# -----------------------------------------------------------------------------
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
PLATFORM_SYS = $(OBJDIR)/$(SRCDIR)/$(PLATFORM_DIR)/platform.o \
               $(OBJDIR)/$(SRCDIR)/$(PLATFORM_DIR)/shmem.o

PLATFORM_IO_SYS = $(OBJDIR)/$(SRCDIR)/$(PLATFORM_DIR)/laio.o

UTIL_SYS = $(OBJDIR)/$(SRCDIR)/util.o $(PLATFORM_SYS)

CLOCKCACHE_SYS = $(OBJDIR)/$(SRCDIR)/clockcache.o	  \
                 $(OBJDIR)/$(SRCDIR)/allocator.o    \
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
$(BINDIR)/$(UNITDIR)/misc_test: $(UTIL_SYS) $(COMMON_UNIT_TESTOBJ)

$(BINDIR)/$(UNITDIR)/util_test: $(UTIL_SYS)            \
                                $(COMMON_UNIT_TESTOBJ)

$(BINDIR)/$(UNITDIR)/btree_test: $(OBJDIR)/$(UNIT_TESTSDIR)/btree_test_common.o \
                                 $(OBJDIR)/$(TESTS_DIR)/config.o                \
                                 $(OBJDIR)/$(TESTS_DIR)/test_data.o             \
                                 $(COMMON_UNIT_TESTOBJ)                         \
                                 $(BTREE_SYS)

$(BINDIR)/$(UNITDIR)/btree_stress_test: $(OBJDIR)/$(UNIT_TESTSDIR)/btree_test_common.o  \
                                        $(OBJDIR)/$(TESTS_DIR)/config.o                 \
                                        $(OBJDIR)/$(TESTS_DIR)/test_data.o              \
                                        $(COMMON_UNIT_TESTOBJ)                          \
                                        $(BTREE_SYS)

$(BINDIR)/$(UNITDIR)/splinter_test: $(COMMON_TESTOBJ)                             \
                                    $(COMMON_UNIT_TESTOBJ)                        \
                                    $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                    $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/splinterdb_quick_test: $(COMMON_TESTOBJ)                             \
                                            $(COMMON_UNIT_TESTOBJ)                        \
                                            $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                            $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/splinterdb_stress_test: $(COMMON_TESTOBJ)                             \
                                             $(COMMON_UNIT_TESTOBJ)                        \
                                             $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                             $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/writable_buffer_test: $(UTIL_SYS)            \
                                           $(COMMON_UNIT_TESTOBJ)

$(BINDIR)/$(UNITDIR)/limitations_test: $(COMMON_TESTOBJ)                             \
                                       $(COMMON_UNIT_TESTOBJ)                        \
                                       $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                       $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/config_parse_test: $(UTIL_SYS)                                   \
                                        $(COMMON_TESTOBJ)                             \
                                        $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                        $(COMMON_UNIT_TESTOBJ)                        \
                                        $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/task_system_test: $(UTIL_SYS)                                   \
                                       $(COMMON_TESTOBJ)                             \
                                       $(COMMON_UNIT_TESTOBJ)                        \
                                       $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                       $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/platform_apis_test: $(UTIL_SYS)               \
                                         $(COMMON_UNIT_TESTOBJ)    \
                                         $(PLATFORM_SYS)

$(BINDIR)/$(UNITDIR)/splinter_shmem_test: $(UTIL_SYS)            \
                                          $(COMMON_UNIT_TESTOBJ) \
                                          $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/splinter_shmem_oom_test: $(UTIL_SYS)            \
                                              $(COMMON_UNIT_TESTOBJ) \
                                              $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/splinter_ipc_test: $(UTIL_SYS)            \
                                        $(COMMON_UNIT_TESTOBJ)

$(BINDIR)/$(UNITDIR)/splinterdb_forked_child_test: $(OBJDIR)/$(TESTS_DIR)/config.o               \
                                                   $(COMMON_TESTOBJ)                             \
                                                   $(COMMON_UNIT_TESTOBJ)                        \
                                                   $(OBJDIR)/$(FUNCTIONAL_TESTSDIR)/test_async.o \
                                                   $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(UNITDIR)/large_inserts_stress_test: $(UTIL_SYS)                      \
                                                $(OBJDIR)/$(TESTS_DIR)/config.o  \
                                                $(COMMON_UNIT_TESTOBJ)           \
                                                $(LIBDIR)/libsplinterdb.so

########################################
# Convenience mini unit-test targets
unit/util_test:                    $(BINDIR)/$(UNITDIR)/util_test
unit/misc_test:                    $(BINDIR)/$(UNITDIR)/misc_test
unit/btree_test:                   $(BINDIR)/$(UNITDIR)/btree_test
unit/btree_stress_test:            $(BINDIR)/$(UNITDIR)/btree_stress_test
unit/splinter_test:                $(BINDIR)/$(UNITDIR)/splinter_test
unit/splinterdb_quick_test:        $(BINDIR)/$(UNITDIR)/splinterdb_quick_test
unit/splinterdb_stress_test:       $(BINDIR)/$(UNITDIR)/splinterdb_stress_test
unit/writable_buffer_test:         $(BINDIR)/$(UNITDIR)/writable_buffer_test
unit/config_parse_test:            $(BINDIR)/$(UNITDIR)/config_parse_test
unit/limitations_test:             $(BINDIR)/$(UNITDIR)/limitations_test
unit/task_system_test:             $(BINDIR)/$(UNITDIR)/task_system_test
unit/splinter_shmem_test:          $(BINDIR)/$(UNITDIR)/splinter_shmem_test
unit/splinter_ipc_test:            $(BINDIR)/$(UNITDIR)/splinter_ipc_test
unit_test:                         $(BINDIR)/unit_test

# -----------------------------------------------------------------------------
# Every example program of the form bin/examples/<eg-prog> depends on
# obj/examples/<eg-prog>.o

#################################################################
# The dependencies of each example program

$(BINDIR)/$(EXAMPLES_DIR)/splinterdb_intro_example: $(OBJDIR)/$(EXAMPLES_DIR)/splinterdb_intro_example.o \
                                                    $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(EXAMPLES_DIR)/splinterdb_wide_values_example: $(OBJDIR)/$(EXAMPLES_DIR)/splinterdb_wide_values_example.o \
                                                   $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(EXAMPLES_DIR)/splinterdb_iterators_example: $(OBJDIR)/$(EXAMPLES_DIR)/splinterdb_iterators_example.o \
                                                        $(LIBDIR)/libsplinterdb.so

$(BINDIR)/$(EXAMPLES_DIR)/splinterdb_custom_ipv4_addr_sortcmp_example: $(OBJDIR)/$(EXAMPLES_DIR)/splinterdb_custom_ipv4_addr_sortcmp_example.o \
                                                                       $(LIBDIR)/libsplinterdb.so

#*************************************************************#

# Report build machine details and compiler version for troubleshooting, so
# we see this output for clean builds, especially in CI-jobs.
.PHONY : clean tags
clean :
	rm -rf $(BUILD_ROOT)
	uname -a
	$(CC) --version
tags:
	ctags -R $(SRCDIR)


#*************************************************************#
# Testing
#

.PHONY: install

run-tests: all-tests
	BINDIR=$(BINDIR) ./test.sh

test-results: all-tests
	INCLUDE_SLOW_TESTS=true BINDIR=$(BINDIR) ./test.sh 2>&1 | tee ./test-results

run-examples: all-examples
		for i in $(EXAMPLES_BINS); do $$i || exit; done

INSTALL_PATH ?= /usr/local

install: $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a
	mkdir -p $(INSTALL_PATH)/include/splinterdb $(INSTALL_PATH)/lib
	# -p retains the timestamp of the file being copied over
	cp -p $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a $(INSTALL_PATH)/lib
	cp -p -r $(INCDIR)/splinterdb/ $(INSTALL_PATH)/include/

# to support clangd: https://clangd.llvm.org/installation.html#compile_flagstxt
.PHONY: compile_flags.txt
compile_flags.txt:
	echo "$(CFLAGS) $(GIT_VERSION_CFLAGS) $(INCLUDE)" | tr ' ' "\n" > compile_flags.txt
