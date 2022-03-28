# SplinterDB Example Programs

The following example C-programs are useful to understand SplinterDB interfaces and
the key-value APIs:

[SplinterDB, Hello World! ](./splinterdb_intro_example.c)
An introductory program that show how to configure and start a SplinterDB instance.
And how to insert key-value pairs, do a lookup given a key, and to iterate through
all key-value pairs in the database.

[Basic SplinterDB Configuration](./splinterdb_admin_config_example.c)
Program highlights few variations of SplinterDB configuration, including
how to restart a running instance.

[Core Insert, Lookup APIs](./splinterdb_apis_example.c)
Program shows how to work with wide-values and how to work with client-provided 
output buffers when retrieving data. Lookups of wide-values may trigger memory
allocation if caller-provided output buffer is not wide enough.

[Using Lookup Iterators](./splinterdb_iterators_example.c)
Program demonstrates use of the iterator interfaces, using more real-life data
involving URLs mapped to inet-addresses, and other 'ping' metrics. This program
demonstrates the default storage order of keys, which is in lexicographic sort order.

[Using Custom Key-Comparison](./splinterdb_custom_ipv4_addr_sortcmp_example.c)
This program demonstrates the use of user-specified key-comparison routines. Here,
the key is an IP4 ip-address, mapped to a payload consisting of the URL, and
some 'ping' metrics. Custom key-comparison function is provided while configuring
SplinterDB to generate numerically sorted ordering for the constituent parts of
the IP-address.

