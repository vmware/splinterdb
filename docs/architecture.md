# SplinterDB Architecture

This document describes the overall Architecture of SplinterDB.

SplinterDB is designed to leverage the low I/O latency and high bandwith of
fast NVMe SSDs.

When deployed on fast NVMe SSDs, SplinterDB leverages the low I/O latency and
high bandwidth of such devices. Consequently, the performance bottlenecks shift
to scalability concerns in multi-core CPU processing. SplinterDB is architected
to exploit this I/O and CPU concurrency to deliver high performance.

----

## High-Level Architecture

At the heart of SplinterDB is the Size-Tiered Bε-tree (STBε-tree), a novel data
structure that combines designs from Log-Structured Merge (LSM) trees and
Bε-trees. The STBε-tree is a tree-of-trees, referred to as the Trunk Tree (or,
simply as the trunk). The trunk tree along with an in-memory Memtable, and
a collection of B-trees constitutes the core data structures of SplinterDB.

All the actual data is stored in the branches, and the trunk indexes and
organizes the data.

- **Memtable**: At the top of the data hierarchy is an in-memory B-tree referred
  to as the Memtable, used to buffer insertions. There is always one active
  Memtable, and a small number of Memtables in various transitionary stages
  to manage the data life-cycle arising from finite, but configurable, Memtable
  capacities.

- **Trunk node**: Each node in the trunk tree has pointers to one or more trunk
  nodes below it, and pointers to the root of one or more B-trees (_aka_ Branch
  trees; see below) hanging off a trunk node.
  The root of the STBε-tree is referred to as the trunk root (node).

- **Branch trees**: Each trunk node has pointers to a collection of B-trees,
  referred to as branch trees. Branch trees store the actual key-value pairs
  in the data set, while the trunk nodes provide the navigation path to
  locate the keys. Branch trees are indexed off the trunk nodes with branch
  numbers such that the most recent inserts are stored in branches with higher
  branch numbers.

- **Routing Filters**: A trunk node also stores a routing filter data structure
  (also referred to as a Quotient filter) associated with each branch tree to
  filter out traversals down branch (sub-)trees during lookups.

- **Values v/s Messages**: All data modifications are performed as inserts of
  new messages to SplinterDB.
  Every insert is associated with a message-type, e.g. INSERT,
  UPDATE, DELETE. Conventional database updates which perform a
  read-modify-write sequence are implemented by a new INSERT message type,
  which over-stores the new image of the value on the latest key-value data.

  If an application frequently makes small modifications to existing values,
  a raw message may encode a "blind update" to a value avoiding this
  read-modify-write sequence. For example an "increment counter" or
  "decrement counter" operation or an "append a new member to a list" operation
  may be persisted by an UPDATE message without doing a read of an existing row
  (key/value pair).

  For a given key-value pair, over its lifetime, a series of mutations may
  be stored simply as a collection of INSERT messages and UPDATE messages
  affecting some parts of the value component. Using application-provided
  merge-callbacks, a series of such messages are aggregated and applied to the
  key-value pair as part of a subsequent lookup, or in the background when
  storage for key-value pairs is consolidated.

  Using this message-based interface, and leveraging the high-throughput insert
  performance offered by SplinterDB, the performance of update-heavy
  applications can be greatly enhanced.

-----

## Data Life Cycle

Fill this up. What do we write here? Insert... incorporate .. flush, merge ...
split etc ..

-----

## Components

A SplinterDB instance is a single file or a block device, which is the database
device used to store the data. A single process is allowed to open the database
and multiple threads can concurrently access the data.

SplinterDB is comprised of the following sub-systems:

### Storage and Space Management

Space is allocated and managed in terms of pages, where the page-size can be
configured when creating a new SplinterDB. (Default is 4 KiB.) Physically
contiguous pages, referred to as an extent, are allocated to an object. (Default
extent size is 32 pages per extent.)

Log pages, used for persisting changes, are also allocated from the same
physical device. (A future direction being explored is to have a dedicated
log-device, to improve IO concurrency.) Individual pages from allocated log
extents are allocated to each active thread which allows each thread to
perform write-ahead logical-logging needed for recovery, without contending
on a single insertion-point of a shared log.

### BTrees

BTrees form the bulk of the data structures used to store and navigate through
the key-value pairs. BTrees come in two flavours in this architecture:

* Dynamic BTrees - Newly inserted / modified data is stored in in-memory BTrees,
  aka Memtables. Data (key-value pairs) can be modified (updated / deleted) in
  these trees. Updating the value of a key residing in the Memtable results in
  immediately consolidating the old / new values resulting in just one instance
  of the key-value pair in-memory. (RESOLVE: How does this pan with update-
  messages, and delta-updates?)

* Static BTrees - Data is immutable in these trees. This form of the BTree is
  used mainly to access key-value pairs that are unchanging.

A conventional multi-level BTree design is used, consisting of a root node
(root page) and one or more child pages. The root and intermediate nodes (pages)
store the keys, whereas the leaf nodes (pages) is where the actual key-value
pair is stored. The fan-out of the BTree pages depends on the size of the
keys stored on each page.

Compaction:
    Nodes in leafs are half-full. Split pages in Memtables may not be contiguous on-disk.
    takes all data, copied over to new sets of pages.


### Memtables

Memtables are the in-memory storage structures which receive new inserts and
updates to the key-value 

A Memtable is implemented as highly-concurrent dynamic BTree designed to avoid
cache misses.
For practical purposes, a Memtable is of a finite capacity as specified when
SplinterDB is configured. (Default: 128 MB).
When the active Memtable is full, it is replaced by a new Memtable. The full
Memtable is then stitched to the underlying layer of the storage structure, the
STBε-tree, as a branch node, with associated metadata updates. This process
of attaching full Memtables to the root trunk node is referred to as
**Incorporation**.

(RESOLVE: Older content ; can be deleted ...)
The size of the active Memtable is finite, as specified when
SplinterDB is configured. When the active Memtable becomes full, a new
Memtable is created. All new inserts go to this new Memtable, while lookups
continue to access the older Memtable. This older Memtable is eventually
attached to the root trunk node, with associated metadata updates. This process
of inserting the older-and-now-full Memtable BTree to the root trunk node
is referred to as Incorporation.

Full memtable BTree is compacted ... Serialization. Generates finger-print list
that is used to build the filter.

### Memory Management

### Locking and Concurrency Control

### Threads and Parallelism

### Transaction Support

### Durability and Crash Recovery

-----

* Single-process with multi-threading support, using Unix pthreads
* Configuring threads for performance

Aspect of this which is front-end user-visible issue.
Appln gets to decide how these threads are allocated / managed.
Appln has an option to volunteer some threads to only do background work.


## Durability

* Per-thread log
* Single log-file, with multiple inserters
* ? Durability / recoverability details in paper are skimpy
 * Logged - sync is 4KB data / thread. Done asynchronously. Can have data loss.

 ? Need an API to do a manual sync that client can call.

 ? Logical logging - Log the puts


### Disk-Resident Artifacts

* Single file for the Key-Value Store data (data device and log)

    * ? What do we want to say here about file-system devices? RAM-devices? Carving up NVMe into multiple devices and so on ...?

* Single file for transaction logging (WAL; log device)

* ? Default page size configured for all pages is 4KB

* ? Any other config block / metadata / super-block ?


## Size-Tiered Bε-Tree

The STBε-tree is really a tree-of-trees. It has two main sub-components:

* The main backbone which organizes the key-value pairs is the Trunk tree, or simply trunks. The Trunk tree
   has a collection of trunk nodes, which help with navigating through the key-value pairs.

* A collection of B-trees, referred to as Branch trees, or simply branches, hanging off each trunk node

### Trunk Nodes

Each page in the Trunk tree is referred to as a Trunk node. It holds ...

### Branches, Branch Nodes (BTree pages)

A Branch is essentially what was previously a Memtable BTree, which got full, and then was
added to the trunk tree as a single unit.

## Routing Filters: Quotient Filters

## Memory Management

* Single user-level cache for all pages read from disk

* Small amount of memory allocated from the system (malloc()) to track metadata for caches and file systems objects

* Clock-based cache manager designed to improve concurrency

* ? Initializing cache mgr at bootstrap? Hash-table?
* 

## Checkpoint - not supported right now - Should be included for OSS-rollout

## Space Management: Extent, Page Allocation

* Extent Allocation, page allocation schemes
* 4KB pages
* 32 pages per extent (128 KB) - Unit of pre-allocation and bulk I/O

* ? Free / allocated space maps? Effectively a refcount table of extents allocated

* Space usage tracking - metrics: total / used / free space?

* ? How does space allocation work when device is getting full?

* ? How does page-deallocation work? Compaction?

* ? Do we reclaim space on-disk? Do we "punch holes" for an extent with all free pages?

* ? How does page allocation work for log-devices? Allocating pages at commit-time will cause
  commit latencies. Interleaving data and log-page allocation can be a problem

Space usage metrics:
    Allocation metrics basic ones
    Stats from cache can be used to compute these metrics.

## Locking and Concurrency Control

* Distributed Read-Write locks (Per-thread reader counter and a shared write-bit)
* Locking granularity is a page (? BTree page, memtable page? Trunk page?)

    * Read locks
    * "Claims" (intent) locks
    * Write locks

    Every fn in Sp has or will need a lock on it. Feature of the system.

### Threads and Parallelism

* Single-process with multi-threading support, using Unix pthreads
* Configuring threads for performance

Aspect of this which is front-end user-visible issue.
Appln gets to decide how these threads are allocated / managed.
Appln has an option to volunteer some threads to only do background work.


## Durability

* Per-thread log
* Single log-file, with multiple inserters
* ? Durability / recoverability details in paper are skimpy
 * Logged - sync is 4KB data / thread. Done asynchronously. Can have data loss.

 ? Need an API to do a manual sync that client can call.

 ? Logical logging - Log the puts


-----

  Is the main data structure off which all key-value pairs
are stored.

 * Offer single-row inserts / updates / deletes
 * Supports safe, concurrent operations writes ... gets/ updates ...

 * If you are using the system, what are the promises it is making. And what external behaviour
   users can se -- should be defined in some user-guide, in user-visible doc.

 * Explain in a page ... where properties that SplinterDB offers are explained. perf, consistency and
   semantics are described.

 * ? We are not ACID Compliant updates? ... as people think
 * ? Persistence - one line?

 * Visibility: Scan dynamics.

* Propopsed structure:
    * Basic APIs: put, scan : Describe guarantees of behaviour
    * In dev-docs: Describe how the guarantees are implemented.
    * Doc on concurrency
    * Doc on persistence
    * Users: Behaviour of concurrent writes
    * Devs: Complexity of concurrency implementation / designs

