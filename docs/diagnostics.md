# Diagnostics and Troubleshooting
This document gives brief references to diagnostics and troubleshooting
interfaces available in the library.

A small collection of print routines is available which can be used to examine key data strucures, such as the Splinter trunk pages, BTree pages, space-allocation maps and so on.

- [trunk_print_super_block](../src/trunk.c#:~:text=trunk\_print\_super\_block\(trunk\_handle)
  Print contents of Splinter super block

- [trunk_print](../src/trunk.c#:~:text=trunk\_print\_node\(trunk\_handle)
  Print contents of a Trunk node (page)

- [trunk_sprint_subtree](../src/trunk.c#:~:text=trunk\_sprint\_subtree\(platform\_stream\_handle)
  Print contents of all sub-trees under a Trunk node (page)

- [trunk_print_space_use](../src/trunk.c#:~:text=trunk\_print\_space\_use\(trunk\_handle)
  Print space usage by level across all trunk nodes

- [trunk_sprint_memtable](../src/trunk.c#:~:text=trunk\_sprint\_memtable\(platform\_stream\_handle)
  Print contents of all Memtables in Splinter. Drills-down to printing BTrees under Memtables

- [btree_print_node](../src/btree.c#:~:text=btree\_print\_node\(cache),
  [btree_print_locked_node](../src/btree.c#:~:text=btree\_print\_locked\_node\(btree\_config)
  Print BTree nodes, and recurse down to sub-tree nodes.

- [rc_allocator_print_debug](../src/rc_allocator.c#:~:text=rc_allocator\_print\_debug\(rc_allocator),
  [rc_allocator_sprint_debug](../src/rc_allocator.c#:~:text=rc_allocator\_sprint\_debug\(platform\_stream\_handle)
  Print the space allocation maps and lists of extents / pages allocated.

- [srq_print](../src/srq.h#::~:text=srq_print\(srq)
  Prints contents of the space reclamation queue

----

A small unit test to exercise these print diagnostics can be found here,
[splinter_test.c:test_splinter_print_diags](../tests/unit/splinter_test.c#:~:text=test\_splinter\_print\_diags).


A useful way to learn about SplinterDB internal structures is to run this test case: `$ bin/unit/splinter_test test_splinter_print_diags`

This test case creates a small SplinterDB instance, and loads a large set
of rows, enough to create several pages in the trunk.

Examine the outputs generated in /tmp/unit_test.stdout by this test case to get a quick view of the internal structures.