# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := release

PLATFORM = linux

CC = clang-8
LD = clang-8

#*************************************************************#
# DIRECTORIES, SRC, OBJ, ETC
#

SRCDIR   = src
TESTSDIR = tests
OBJDIR   = obj
BINDIR   = bin

SRC := $(shell find $(SRCDIR) -name "*.c")
TESTSRC := $(shell find $(TESTSDIR) -name "*.c")

OBJ := $(SRC:%.c=$(OBJDIR)/%.o)
TESTOBJ= $(TESTSRC:%.c=$(OBJDIR)/%.o)

# Automatically create directories, based on
# http://ismail.badawi.io/blog/2017/03/28/automatic-directory-creation-in-make/
.SECONDEXPANSION:

.PRECIOUS: $(OBJDIR)/%/.

$(OBJDIR)/. $(BINDIR)/.:
	mkdir -p $@

$(OBJDIR)/%/. $(BINDIR)/%/.:
	mkdir -p $@

#*************************************************************#
# CFLAGS, ETC
#

INCLUDE = -I $(SRCDIR) -I $(SRCDIR)/platform_$(PLATFORM)

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
DEFAULT_CFLAGS += -msse4.2 -mpopcnt -DXXH_STATIC_LINKING_ONLY
#DEFAULT_CFLAGS += -fsanitize=memory -fsanitize-memory-track-origins
#DEFAULT_CFLAGS += -fsanitize=address
#DEFAULT_CFLAGS += -fsanitize=integer
DEFAULT_CFLAGS += $(LIBCONFIG_CFLAGS)


CFLAGS += $(DEFAULT_CFLAGS) -Ofast -flto -march=native -fPIC
#CFLAGS += -DDEBUG
DEFAULT_LDFLAGS = -ggdb3 -pthread
LDFLAGS = $(DEFAULT_LDFLAGS) -Ofast -flto
LIBS = -lm -lpthread -laio -lxxhash $(LIBCONFIG_LIBS)


#*********************************************************#
# Targets to track whether we have a release or debug build
#

all: $(BINDIR)/splinterdb.so $(BINDIR)/driver_test

release: .release all
	rm -f .debug
	rm -f .debug-log

debug: CFLAGS = -g -DDEBUG $(DEFAULT_CFLAGS)
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

$(BINDIR)/driver_test : $(TESTOBJ) $(BINDIR)/splinterdb.so | $$(@D)/.
	$(LD) $(LDFLAGS) $(LIBS) -o $@ $^

$(BINDIR)/splinterdb.so : $(OBJ) | $$(@D)/.
	$(LD) $(LDFLAGS) -shared $(LIBS) -o $@ $^

DEPFLAGS = -MMD -MT $@ -MP -MF $(OBJDIR)/$*.d

COMPILE.c = $(CC) $(DEPFLAGS) $(CFLAGS) $(INCLUDE) $(TARGET_ARCH) -c

$(OBJDIR)/%.o: %.c | $$(@D)/.
	$(COMPILE.c) $< -o $@

-include $(SRC:%.c=$(OBJDIR)/%.d) $(TESTSRC:%.c=$(OBJDIR)/%.d)

#*************************************************************#

.PHONY : clean tags
clean :
	rm -rf $(OBJDIR)/*
	rm -f $(BINDIR)/*

tags:
	ctags -R src
