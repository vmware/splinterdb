# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := release

PLATFORM = linux

#*************************************************************#
# DIRECTORIES, SRC, OBJ, ETC
#

SRCDIR   = src
TESTSDIR = tests
OBJDIR   = obj
BINDIR   = bin
LIBDIR   = lib
INCDIR   = include

SRC := $(shell find $(SRCDIR) -name "*.c")
TESTSRC := $(shell find $(TESTSDIR) -name "*.c")

OBJ := $(SRC:%.c=$(OBJDIR)/%.o)
TESTOBJ= $(TESTSRC:%.c=$(OBJDIR)/%.o)

# Automatically create directories, based on
# http://ismail.badawi.io/blog/2017/03/28/automatic-directory-creation-in-make/
.SECONDEXPANSION:

.PRECIOUS: $(OBJDIR)/%/.

$(OBJDIR)/. $(BINDIR)/. $(LIBDIR)/.:
	mkdir -p $@

$(OBJDIR)/%/. $(BINDIR)/%/.:
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
LDFLAGS = $(DEFAULT_LDFLAGS) -Ofast -flto
LIBS = -lm -lpthread -laio -lxxhash $(LIBCONFIG_LIBS)


#*********************************************************#
# Targets to track whether we have a release or debug build
#
all: $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a $(BINDIR)/driver_test

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

$(BINDIR)/driver_test : $(TESTOBJ) $(LIBDIR)/libsplinterdb.so | $$(@D)/.
	$(LD) $(LDFLAGS) -o $@ $^ $(LIBS)

$(LIBDIR)/libsplinterdb.so : $(OBJ) | $$(@D)/.
	$(LD) $(LDFLAGS) -shared -o $@ $^ $(LIBS)

# -c: Create an archive if it does not exist. -r, replacing objects
# -s: Create/update an index to the archive
$(LIBDIR)/libsplinterdb.a : $(OBJ) | $$(@D)/.
	$(AR) -crs $@ $^

DEPFLAGS = -MMD -MT $@ -MP -MF $(OBJDIR)/$*.d

COMPILE.c = $(CC) $(DEPFLAGS) $(CFLAGS) $(INCLUDE) $(TARGET_ARCH) -c

$(OBJDIR)/%.o: %.c | $$(@D)/.
	$(COMPILE.c) $< -o $@

-include $(SRC:%.c=$(OBJDIR)/%.d) $(TESTSRC:%.c=$(OBJDIR)/%.d)

#*************************************************************#

.PHONY : clean tags
clean :
	rm -rf $(OBJDIR)/*
	rm -f  $(BINDIR)/*
	rm -f  $(LIBDIR)/*

tags:
	ctags -R src


#*************************************************************#
# Testing
#

.PHONY: test install

test: $(BINDIR)/driver_test
	./test.sh

INSTALL_PATH ?= /usr/local

install: $(LIBDIR)/libsplinterdb.so
	mkdir -p $(INSTALL_PATH)/include/splinterdb $(INSTALL_PATH)/lib

	# -p retains the timestamp of the file being copied over$
	cp -p $(LIBDIR)/libsplinterdb.so $(LIBDIR)/libsplinterdb.a $(INSTALL_PATH)/lib
	cp -p $(INCDIR)/splinterdb/*.h $(INSTALL_PATH)/include/splinterdb/
