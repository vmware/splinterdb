# Diagnostics and Troubleshooting
This document gives brief references to diagnostics and troubleshooting
interfaces available in the library.

A small collection of print routines is available which can be used to examine
key data strucures, such as the SplinterDB trunk pages, BTree pages, space-allocation
maps and so on.

- [trunk_print_super_block](https://github.com/vmware/splinterdb/tree/main/src/trunk.c#:~:text=trunk%5Fprint%5Fsuper%5Fblock%28%29)
  Print contents of SplinterDB super block

- [trunk_print](https://github.com/vmware/splinterdb/tree/main/src/trunk.c#:~:text=trunk%5Fprint%5Fnode%28platform%5Flog%5Fhandle)
  Print contents of a Trunk node (page)

- [trunk_print_subtree](https://github.com/vmware/splinterdb/tree/main/src/trunk.c#:~:text=trunk%5Fprint%5Fsubtree%28platform%5Flog%5Fhandle)
  Print contents of all sub-trees under a Trunk node (page)

- [trunk_print_space_use](https://github.com/vmware/splinterdb/tree/main/src/trunk.c#:~:text=trunk%5Fprint%5Fspace%5Fuse%28platform%5Flog%5Fhandle)
  Print space usage by level across all trunk nodes

- [trunk_print_memtable](https://github.com/vmware/splinterdb/tree/main/src/trunk.c#:~:text=trunk%5Fprint%5Fmemtable%28platform%5Flog%5Fhandle)
  Print contents of all Memtables in Splinter. Drills-down to printing BTrees under Memtables

- [btree_print_tree](https://github.com/vmware/splinterdb/tree/main/src/btree.c#:~:text=btree%5Fprint%5Ftree%28platform%5Flog%5Fhandle),
  [btree_print_node](https://github.com/vmware/splinterdb/tree/main/src/btree.c#:~:text=btree%5Fprint%5Fnode%28platform%5Flog%5Fhandle),
  [btree_print_locked_node](https://github.com/vmware/splinterdb/tree/main/src/btree.c#:~:text=btree%5Fprint%5Flocked%5Fnode%28platform%5Flog%5Fhandle)
  Print BTree nodes, and recurse down to sub-tree nodes.

- [rc_allocator_print_allocated](https://github.com/vmware/splinterdb/tree/main/src/rc_allocator.c#:~:text=rc_allocator%5Fprint%5Fallocated%28%29)
  Print the space allocation maps and lists of extents / pages allocated.

- [rc_allocator_print_stats](https://github.com/vmware/splinterdb/tree/main/src/rc_allocator.c#:~:text=rc_allocator%5Fprint%5Fstats%28%29)
  Prints basic statistics about the allocator state.

----

A small unit test to exercise these print diagnostics can be found here,
[splinter_test.c:test_splinter_print_diags](https://github.com/vmware/splinterdb/tree/main/tests/unit/splinter_test.c#:~:text=test%5Fsplinter%5Fprint%5Fdiags).


A useful way to learn about SplinterDB internal structures is to run this test case: `$ bin/unit/splinter_test test_splinter_print_diags`

This test case creates a small SplinterDB instance, and loads a large set
of rows, enough to create several pages in the trunk.

Examine the outputs generated in /tmp/unit_test.stdout by this test case to get
a quick view of the internal structures.
