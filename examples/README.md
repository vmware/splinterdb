# SplinterDB Example Programs

The following example C-programs are useful to understand SplinterDB interfaces
and the key-value APIs. The programs are laid out with slightly increasing
complexity of the usages and program construction.

> Try to work through these example programs by running them, in this order,
  and review the output messages.

[SplinterDB, Hello World! ](./splinterdb_intro_example.c)
An introductory program that show how to configure and start a SplinterDB instance.
And how to insert key-value pairs, do a lookup given a key, and to iterate through
all key-value pairs in the database. Many of the commonly used interfaces can be
seen in this program.

[Handling wide values](./splinterdb_wide_values_example.c)
Program shows how to work with wide-values and how to work with client-provided
output buffers when retrieving data. Lookups of wide-values may trigger memory
allocation if caller-provided output buffer is not wide enough.

[Using Lookup Iterators](./splinterdb_iterators_example.c)
Program demonstrates use of the iterator interfaces, using real-life data
involving URLs mapped to inet-addresses, and other hard-coded 'ping' metrics.
This program demonstrates the default storage order of keys, which is in
lexicographic sort order.

[Using Custom Key-Comparison](./splinterdb_custom_ipv4_addr_sortcmp_example.c)
This program demonstrates the use of user-specified key-comparison routines. Here,
the key is a 4-part IP4 ip-address (stored as a string; e.g. "33.24.128.22"). The
key is mapped to a payload consisting of the www-URL, and some hard-coded 'ping'
metrics. A custom key-comparison function is provided while configuring
SplinterDB to generate numerically sorted ordering for the constituent parts of
the IP-address.
