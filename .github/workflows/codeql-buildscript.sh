#!/usr/bin/env bash

export COMPILER=gcc
sudo apt update -y
sudo apt-get install -y libaio-dev libconfig-dev libxxhash-dev $COMPILER
export CC=$COMPILER
export LD=$COMPILER
make
