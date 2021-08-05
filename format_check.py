#!/usr/bin/env python

/*
 * Based on https://dx13.co.uk/articles/2015/04/03/setting-up-git-clang-format/
 */

import subprocess

output = subprocess.check_output(["git", "clang-format", "--diff"])

if output not in ['no modified files to format\n', 'clang-format did not modify any files\n']:
   print "Format check failed!\n"
   print output
   exit(1)
else:
   exit(0)
