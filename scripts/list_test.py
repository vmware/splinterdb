#!/usr/bin/env python3

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0



# List tests by the options passed in

import argparse
import sys
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List, Tuple, Dict
from random import randint

# HOW TO MAKE CHANGES TO THIS SCRIPT
# - Add more tests by changing the test dict ALL_TESTS
# - Add more cli options by adding a parser argument in main(), and
#   inheriting the base Filter class to implement a custom match function


# ALL_TESTS has the following format:
# OrderedDict([
#   (( TAG, ... ), [
#       [ TEST ],
#       ...
#   ]),
#   ...
# ])
#
# TEST has the following format:
# [ TEST_STRING, [TEST_INFO] ]
# TEST_STRING will be split into TEST_NAME([0]) and TEST_ARGS([1:])
# TEST_INFO is optional for attaching extra info like bug No. to the test

PRECHECKIN = 'precheckin'
CACHESTRESS = 'cachestress'
ASYNC = 'async'
DELETE = 'delete'
PERF = 'perf'
SPLINTERPERF='splinterperf'
SEQPERF = 'seqperf'
SEMISEQPERF = 'semiseqperf'
PARALLELPERF = 'parallelperf'
BTREEPERF = 'btreeperf'
CACHEPERF = 'cacheperf'

precheckin_tests = [
   [ 'splinter_test --stats' ],
   # splinter_test --functionality NUM_INSERTS CORRECTNESS_CHECK_FREQUENCY
   [ 'splinter_test --functionality 1000000 1000 --stats' ],
   [ 'splinter_test --functionality 1000000 10 --num-tables 2 --stats'],
   [ 'splinter_test --functionality 1000000 10 --num-tables 2 \
      --cache-per-table --stats'],
   [ 'btree_test' ],
   [ 'splinter_test --parallel-perf --num-bg-threads 2 --max-async-inflight 4 --num-pthreads 4 --tree-size-mib 512 --key-size 20 --data-size 20' ],
]
# precheckin tests that don't need extra arguments
precheckin_tests_no_args = [
   ['splinter_test --delete --num-insert-threads 2 --tree-size-mib 256 \
    --stats'],
   [ 'cache_test --stats' ],
   [ 'filter_test --stats' ],
   [ 'log_test --stats' ],
]
cachestress_tests = [
   [ 'splinter_test --cache-capacity-mib 128' ],
   [ 'btree_test --cache-capacity-mib 4' ],
   [ 'filter_test --cache-capacity-mib 16' ],
   [ 'btree_test --perf --cache-capacity-mib 32' ],
]
async_tests = [
   [ 'cache_test --async --stats' ],
   [ 'filter_test --perf --cache-capacity-mib 16' ],
   [ 'btree_test --cache-capacity-mib 4' ],
   [ 'splinter_test --perf --num-insert-threads 8 --num-lookup-threads 8 --tree-size-mib 512 --key-size 20 --data-size 20 --cache-capacity-mib 32' ],
   [ 'splinter_test --parallel-perf --num-bg-threads 4 --max-async-inflight 8 --num-pthreads 8 --tree-size-mib 128 --key-size 20 --data-size 20 --cache-capacity-mib 32' ],
]
delete_tests = [
   [ 'splinter_test --delete' ],
]
perf_tests_splinter = [
   [ 'splinter_test --perf --key-size 20 --data-size 20' ],
]
perf_tests_seqperf = [
   [ 'splinter_test --seq-perf' ],
]
perf_tests_semiseqperf = [
   [ 'splinter_test --semiseq-perf' ],
]
perf_tests_parallelperf = [
   [ 'splinter_test --parallel-perf --num-bg-threads 4 --max-async-inflight 8 --num-pthreads 8 --tree-size-mib 512 --key-size 20 --data-size 20 --cache-capacity-mib 64' ],
]
perf_tests_btree = [
   [ 'btree_test --perf' ],
]
perf_tests_cache = [
   [ 'cache_test --perf' ],
]
perf_tests_all = perf_tests_splinter + perf_tests_seqperf + \
                 perf_tests_semiseqperf + perf_tests_parallelperf + \
                 perf_tests_btree + perf_tests_cache

#
# Create a new list by appending each extra arg to each test
#
def add_extra_args(tests: List[List[str]]) -> List[List[str]]:
   extras = [
      ' --key-size 24 --data-size 100',
      ' --key-size 20 --data-size 20',
   ]
   return [[test[0] + extra, *test[1:]] for test in tests for extra in extras]

ALL_TESTS = OrderedDict([
   ((PRECHECKIN), add_extra_args(precheckin_tests) + precheckin_tests_no_args),
   ((CACHESTRESS), add_extra_args(cachestress_tests)),
   ((DELETE), delete_tests),
   ((PERF), perf_tests_all),
   ((SPLINTERPERF), perf_tests_splinter),
   ((SEQPERF), perf_tests_seqperf),
   ((SEMISEQPERF), perf_tests_semiseqperf),
   ((PARALLELPERF), perf_tests_parallelperf),
   ((BTREEPERF), perf_tests_btree),
   ((CACHEPERF), perf_tests_cache),
   ((ASYNC), async_tests),
])

# global counter for generating test info
TEST_CNT = 1

# For anything that isn't sensible test params, use eprint
# e.g., ./list_test.py --list
def eprint(*args, **kwargs) -> None:
   print(*args, file=sys.stderr, **kwargs)

class Test:
   def __init__(self,
                tags: Tuple[str, ...],
                name: str,
                args: str,
                info: str) -> None:
      self.tags = tags
      self.name = name
      self.args = args
      self.info = info

   def __str__(self) -> str:
      return ('tags: %s\tinfo: %s\ttest: %s' %
              (self.tags, self.info, self.raw()))

   # raw test arguments for the command line
   def raw(self) -> str:
      return ('%s %s' % (self.name, self.args))

#
# Base class for filter tests by specific condition
# Inherit this class to implement custom match function
#
class Filter(ABC):
   @abstractmethod
   def match(self, test: Test) -> None:
      pass

class ListAllFilter(Filter):
   def match(self, test: Test) -> bool:
      eprint(test)
      return False

class TagFilter(Filter):
   def __init__(self, tag: str) -> None:
      self.tag = tag

   def match(self, test: Test) -> bool:
      # when the test has only one tag
      if isinstance(test.tags, str):
         return self.tag == test.tags
      return self.tag in test.tags


#
# Parse a single test and return a test object
# Note it modifies the global counter
#
def parse_test(tags: Tuple[str, ...], test: List[str]) -> Test:
   global TEST_CNT
   name, *args_list = test[0].split()
   args = ' '.join(args_list)
   if len(test) == 1:
      # populate the info field for tests without one
      info = name + '_' + str(TEST_CNT)
      TEST_CNT += 1
   elif len(test) == 2:
      info = test[1]
   else:
      raise ValueError('Wrong number of arguments for test')
   return Test(tags=tags, name=name, args=args, info=info)

#
# Create a list of tests from test db
#
def create_test(test_db: Dict[Tuple[str, ...], List[List[str]]]) -> List[Test]:
   return [parse_test(tags, test) for tags, tests in test_db.items()
           for test in tests]

#
# Apply each filter to each of the test in the list,
# and extract the common tests
#
def filter_test(tests: List[Test], filters: List[Filter]) -> List[str]:
   total = len(filters)
   tests_list = [list(filter(x.match, tests)) for x in filters]
   # count the frequency of each test, if cnt == len(filters), means the
   # test appeared in each filtered result, thus it's the common one
   db = OrderedDict()
   for tests in tests_list:
      for test in tests:
         raw = test.raw()
         db[raw] = db.get(raw, 0) + 1

   return [raw for raw, cnt in db.items() if cnt == total]

def main():
   parser = argparse.ArgumentParser(prog='', add_help=False,
                                    description='SplinterDB test runner.')
   parser.add_argument('-t', '--tags', nargs='+',
                       help='Run the tests that match ALL of the TAGS')
   parser.add_argument('-l', '--list', action='store_true',
                       help='List all the tests')
   parser.add_argument('-h', '--help', action='store_true',
                       help='Show help message')
   parser.add_argument('-e', '--extra', help='Extra test arguments')
   parser.add_argument('-i', '--times', type=int,
                       help='Run the tests repeatedly')
   args = parser.parse_args()

   rc = -1
   filters = []
   extra = ' ' + args.extra if args.extra else ''
   times = args.times if args.times else 1

   # set the filters for each option
   if args.list:
      filters.append(ListAllFilter())
   elif args.tags:
      filters = [TagFilter(tag) for tag in args.tags]
   else:
      # no arguments supplied
      parser.print_help(sys.stderr)
      return -1

   tests = filter_test(create_test(ALL_TESTS), filters)
   # only when we find something return success
   if tests:
      rc = 0

   [print(test + ' --seed %s' % randint(0, 65535) + extra)
    for i in range(times)
    for test in tests]

   return rc

if __name__ == '__main__':
   sys.exit(main())
