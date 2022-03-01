# SplinterDB Known Limitations 

SplinterDB is expected to evolve its API and add significant features, and it is not recommended for production use until version 1.0.
Thus, SplinterDB is provided *as-is* given the following limitations and missing features:

* Key and data size need to be some fraction of page size. 
* The application must specify the minimum and maximum of the key range.
* SplinterDB size is fixed at compile time.
* Recovery mechanism (see [Issue Discussion](https://github.com/vmware/splinterdb/issues/236)).
* SplinterDB on-disk format is not versioned (data may not survive upgrades.)
* SplinterDB does not expose an API to force the latest write to be durable (e.g., fsync/commit.)
* SplinterDB disk size cannot be changed once specified.
* SplinterDB does not have a public API for async features.
* SplinterDB does not retain initialization parameters metadata (which cannot be discovered from the database.)
* Internal metrics and stats are not exposed to applications.
* Checkpointing. 
* Range delete.
* Empty database (e.g. db->clear().)
* Transactions (SplinterDB does not support a set of writes appear atomic to other threads.)
