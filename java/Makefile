# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0

.suffixes:
.PHONY: clean

JAVA_HOME ?= $(shell readlink -e "$$(dirname "$$(readlink -e "$$(which javac)")")"/..)
JAVAC = javac

CFLAGS  += -Iinclude -I"$(JAVA_HOME)/include" -I"$(JAVA_HOME)/include/linux" \
           -fPIC
LDFLAGS += -shared -lsplinterdb -DSPLINTERDB_PLATFORM_DIR=platform_linux

OBJS = $(patsubst src/%.c,build/%.o,$(wildcard src/*.c))

all: build SplinterDBJNI.h libsplinterdbjni.so splinterdb.jar run-test

build:
	mkdir -p include
	mkdir -p build

splinterdb.jar:
	$(JAVAC) -d build src/SplinterDBJNI.java
	jar cf build/splinterdbjni.jar -C build/ org/splinterdb/SplinterDBJNI.class

SplinterDBJNI.h:
	$(JAVAC) -h include/ src/SplinterDBJNI.java

libsplinterdbjni.so:
	gcc $(CFLAGS) -o build/libsplinterdbjni.so src/SplinterDBJNI.c $(LDFLAGS) 

run-test:
	mvn test

clean:
	-rm -rf include
	-rm -rf build
	-mvn clean
