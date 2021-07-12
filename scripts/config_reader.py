#!/usr/bin/env python3

# Copyright 2018-2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0



# Config reader for parsing cfg file and cli params

# Example usage:
# ./config_reader.py splinter_test.cfg splinter_test --perf

import sys
import collections
import shlex

def panic(msg):
   print(msg)
   sys.stdout.flush()
   sys.exit(1)

class ConfigDB():
   def __init__(self):
      # config dict for storing each config option, e.g., ['key-size'] = 20
      # use ordered dict to preserve the order they passed in
      self.cfg = collections.OrderedDict()
      # indicate whether a test type is passed in
      self.use_test_type = False

   # config file only contains kv pairs, or keys with no values,
   # e.g., key-size=20, or stats
   def parse_cfg_file(self, file):
      try:
         with open(file, 'r') as cfg_file:
            splitter = shlex.shlex(cfg_file)
            # remove comments and whitespace
            splitter.commenters = '#'
            splitter.whitespace = '\n'
            splitter.whitespace_split = True
            for kv_pair in splitter:
               kv_tuple = [x.strip() for x in kv_pair.split('=')]
               if len(kv_tuple) == 1:
                  # keys with no values
                  self.cfg[kv_tuple[0]] = ''
               else:
                  self.cfg[kv_tuple[0]] = kv_tuple[1]
      except IOError as e:
         panic(str(e))

   # cli options can have keys with multiple values,
   # e.g., --functionality 10 100 1000
   def parse_cli_opts(self, opts):
      override_cfg = collections.OrderedDict()

      splitter = [x.strip() for x in ' '.join(opts).split('--')]
      for kv_pair in splitter:
         kv_tuple = kv_pair.split()
         if kv_tuple:
            if len(kv_tuple) == 1:
               override_cfg[kv_tuple[0]] = ''
            else:
               # keys with multiple values
               override_cfg[kv_tuple[0]] = kv_tuple[1:]

      # override options from config file
      for k, v in self.cfg.items():
         if k not in override_cfg:
            override_cfg[k] = v
      self.cfg = override_cfg
      self.use_test_type = True

   def dump_to_stdout(self):
      out = ''
      for k, v in self.cfg.items():
         if type(v) is list:
            out += '--%s %s ' % (k, ' '.join(v))
         else:
            out += '--%s %s ' % (k, v)
      if self.use_test_type:
         # test type don't have -- prefx, e.g., splinter_test
         print(out.lstrip('--'))
      else:
         print(out)

def main():
   if len(sys.argv) < 2:
      panic('Need at least 1 argument')

   db = ConfigDB()
   if len(sys.argv) == 2:
      db.parse_cfg_file(sys.argv[1])
   else:
      db.parse_cfg_file(sys.argv[1])
      db.parse_cli_opts(sys.argv[2:])

   db.dump_to_stdout()

if __name__ == '__main__':
   main()
